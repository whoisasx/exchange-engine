# Perpex Engine

C++ matching and risk engine for Perpex. The engine consumes ordered inputs from
Redpanda, applies matching/risk logic, publishes replies and durable events, and
saves checkpoints for recovery.

The exchange server is a separate repo and process. It owns API, wallet checks,
accounting, read models, and websocket fanout. The engine owns `engine.input`
consumption, matching, risk state, checkpoints, and `engine.replies` /
`engine.events` production.

## Architecture

```mermaid
flowchart LR
    exchange[exchange-server<br/>external process] --> input[(engine.input)]
    input --> app[engine_app]

    app --> recovery[RecoveryCoordinator]
    recovery --> store[(Checkpoint Store<br/>S3/MinIO or file)]
    recovery --> input

    app --> parser[EngineInputParser]
    parser --> runtime[EngineRuntime]
    runtime --> core[Core Engine<br/>orderbook + matching + risk]
    core --> outbox[EngineOutbox]

    outbox --> replies[(engine.replies)]
    outbox --> events[(engine.events)]
    app --> store
    app --> commits[Input Offset Commit]

    replies --> exchange
    events --> exchange
```

## Processing Order

```mermaid
sequenceDiagram
    participant RP as Redpanda engine.input
    participant A as engine_app
    participant R as EngineRuntime
    participant O as EngineOutbox
    participant S as Checkpoint Store
    participant C as Offset Committer

    A->>RP: Poll one input
    RP-->>A: Consumed record
    A->>R: Parse and process
    R-->>A: Replies/events plus state changes
    A->>O: Publish output records
    O-->>RP: engine.replies / engine.events
    A->>S: Save checkpoint with next_offset
    A->>C: Commit consumed offset
```

The checkpoint is saved before the input offset is committed. If checkpoint
save fails, the engine fails loudly and does not commit the offset.

## Repo Layout

```text
include/core       Core orderbook, matching, fixed point, and risk types
src/core           Core engine implementation
include/runtime    Input parsing, output translation, runtime orchestration
src/runtime        Runtime implementation
include/broker     Broker interfaces and Redpanda app boundary
src/broker         Redpanda app processing wrapper
include/checkpoint Checkpoint interfaces and data model
src/checkpoint     File and S3 checkpoint stores
include/recovery   Checkpoint recovery and replay
src/recovery       Recovery implementation
src/app            engine_app config and executable entrypoint
docs               Stream contract and JSON fixtures
bench              Benchmark binaries and workload generators
bench-harness      Benchmark build/run scripts
tests              Unit, fixture, recovery, broker, and smoke tests
test-harness       Manual smoke and exchange e2e scripts
```

## Prerequisites

- CMake 3.20+
- C++20 compiler
- libcurl
- librdkafka++ for `engine_app`
- Docker/Redpanda only when running stream-backed tests

On macOS with Homebrew:

```sh
brew install cmake curl librdkafka
```

## Build

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_STANDARD=20
cmake --build build --parallel
```

Build only `engine_app`:

```sh
cmake --build build --target engine_app --parallel
```

## Run Tests

Offline engine smoke:

```sh
test-harness/smoke.sh --skip-redpanda
```

All CTest tests after a build:

```sh
ctest --test-dir build --output-on-failure
```

## Run Benchmarks

Run a focused core matching benchmark:

```sh
bench-harness/run-core.sh --scenario mixed --commands 100000 --warmup 5000
```

Run the full local benchmark matrix:

```sh
bench-harness/run-all.sh
```

Measure JSON output serialization cost for the same runtime workload:

```sh
bench-harness/run-runtime.sh --scenario mixed --commands 100000 --warmup 5000
bench-harness/run-runtime.sh --scenario mixed --commands 100000 --warmup 5000 \
  --include-output-serialization
```

Benchmark reports are JSON with throughput, output byte counts, and latency
percentiles including `p50`, `p90`, `p95`, `p99`, `p99.9`, and max.

Redpanda-backed smoke requires an already-running `engine_app` and reachable
Redpanda:

```sh
test-harness/smoke.sh --require-redpanda
```

## Storage Containers

The engine e2e flow uses the storage containers owned by the exchange repo
harness:

```sh
cd ~/perpex/exchange
test-harness/infra.sh up
```

This starts and prepares:

| Container | Purpose | Local endpoint |
|---|---|---|
| Postgres | Main exchange DB: users, balances, orders, projector rows, ledger rows, wallet outbox | `postgres://postgres:postgres@127.0.0.1:55432/exchange` |
| Redpanda | Streams and queues: `wallet.commands`, `wallet.events`, `engine.input`, `engine.replies`, `engine.events` | `127.0.0.1:19092` |
| TimescaleDB | Time-series DB for trades and candles | `postgres://postgres:postgres@127.0.0.1:55433/exchange_timeseries` |
| MinIO | S3-compatible object storage for engine checkpoints | `http://127.0.0.1:59000` |

`infra.sh up` also creates Redpanda topics, creates the Timescale extension,
creates the MinIO bucket `exchange-checkpoints`, and clears old checkpoint
objects. Stop and remove local infra with:

```sh
test-harness/infra.sh down
```

## Run With Exchange E2E

Use sibling repo checkouts:

```sh
mkdir -p ~/perpex
cd ~/perpex
git clone git@github.com:whoisasx/exchange-engine.git engine
git clone git@github.com:whoisasx/exchange-server.git exchange
```

Start exchange infra:

```sh
cd exchange
test-harness/infra.sh up
```

Start the engine:

```sh
cd ../engine
test-harness/run-exchange-e2e-engine.sh
```

Run the exchange smoke in another terminal:

```sh
cd ../exchange
test-harness/smoke.sh
```

Expected exchange output:

```text
e2e smoke passed
e2e smoke complete
```

Stop `engine_app` with `Ctrl-C`, then stop exchange infra:

```sh
cd ../exchange
test-harness/infra.sh down
```

## Configuration

`engine_app` accepts CLI flags and matching `CEX_ENGINE_*` environment
variables. The most common settings are:

- `CEX_ENGINE_BOOTSTRAP_SERVERS`: Redpanda/Kafka bootstrap servers
- `CEX_ENGINE_GROUP_ID`: consumer group id
- `CEX_ENGINE_CHECKPOINT_STORE`: `s3` or `file`
- `CEX_ENGINE_CHECKPOINT_S3_ENDPOINT`: S3/MinIO endpoint
- `CEX_ENGINE_CHECKPOINT_S3_BUCKET`: checkpoint bucket
- `CEX_ENGINE_MARKETS_CONFIG`: market config file

For the exchange e2e harness, use:

```sh
test-harness/run-exchange-e2e-engine.sh
```

It builds `engine_app` and runs it against exchange harness Redpanda and MinIO
with `test-harness/exchange-e2e-markets.conf`.

## More Detail

- [Test harness](test-harness/README.md)
- [Benchmark harness](bench-harness/README.md)
- [Engine stream contract](docs/engine-contract.md)
- [Protocol fixtures](docs/examples/README.md)
