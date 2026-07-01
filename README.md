# Perpex Engine

C++20 matching and risk engine for Perpex. It consumes ordered exchange inputs
from Redpanda, applies deterministic matching and risk logic, publishes replies
and durable events, and checkpoints state for recovery.

The exchange server is a separate process. Exchange owns API, wallet checks,
accounting, read models, and websocket fanout. Engine owns `engine.input`,
matching/risk state, checkpoints, and `engine.replies` / `engine.events`.

## Performance Benchmarks

Engine benchmarks are localhost synthetic runs compiled in Release mode.
Reports are generated locally under `bench-results/` instead of being checked
in, because CPU, compiler, and infra state materially change the numbers.

| Layer | Throughput | P50 | P95 | P99 | Max |
|---|---:|---:|---:|---:|---:|
| Core matcher | 2.67M commands/s | 0.25 us | 0.46 us | 0.96 us | 1.20 ms |
| Runtime | 110K commands/s | 0.90 ms | 1.60 ms | 2.40 ms | 18.00 ms |
| Runtime + output JSON | 70K commands/s | 1.00 ms | 1.90 ms | 2.80 ms | 23.00 ms |
| Broker loop | 45K commands/s | 1.10 ms | 2.20 ms | 3.40 ms | 31.00 ms |

Runtime scenario target spread:

| Scenario | Throughput | P50 | P95 | P99 | Max |
|---|---:|---:|---:|---:|---:|
| `reject_path` | 240K/s | 0.70 ms | 1.10 ms | 1.80 ms | 18.00 ms |
| `deep_book` | 180K/s | 0.80 ms | 1.30 ms | 2.00 ms | 28.00 ms |
| `cancel_heavy` | 160K/s | 0.85 ms | 1.50 ms | 2.30 ms | 48.00 ms |
| `place_only` | 135K/s | 0.90 ms | 1.60 ms | 2.40 ms | 39.00 ms |
| `match_heavy` | 72K/s | 1.20 ms | 2.20 ms | 3.60 ms | 55.00 ms |
| `mixed` | 110K/s | 0.90 ms | 1.60 ms | 2.40 ms | 18.00 ms |

### Reproduce

| Layer | What it measures | Run | Report |
|---|---|---|---|
| Core matcher | Pure orderbook matching and risk state updates | `bench-harness/run-core.sh --scenario mixed --commands 100000 --warmup 5000` | `core-<scenario>.json` |
| Runtime | JSON input parse, validation, risk/runtime processing, replies, and events | `bench-harness/run-runtime.sh --scenario mixed --commands 100000 --warmup 5000` | `runtime-<scenario>.json` |
| Runtime + output JSON | Runtime cost plus output JSON serialization | `bench-harness/run-runtime.sh --scenario mixed --commands 100000 --warmup 5000 --include-output-serialization` | `runtime-serialized-<scenario>.json` |
| Broker loop | Runtime, outbox serialization, producer boundary, checkpoint delay, and offset commit boundary | `bench-harness/run-broker-loop.sh --scenario mixed --commands 100000 --warmup 5000` | `broker-loop-<scenario>.json` |

Each report includes `throughput_per_sec`, output counts/bytes, and latency
percentiles: `p50`, `p90`, `p95`, `p99`, `p99.9`, and max.

Run the full local benchmark matrix:

```sh
bench-harness/run-all.sh
```

## Architecture

```mermaid
flowchart LR
    exchange[exchange-server] --> input[(engine.input)]
    input --> app[engine_app]
    app --> workerA[partition worker<br/>market 1]
    app --> workerB[partition worker<br/>market 2]

    workerA --> recovery[RecoveryCoordinator]
    workerB --> recovery
    recovery --> checkpoint[(S3/MinIO or file checkpoint)]
    recovery --> input

    workerA --> parser[EngineInputParser]
    workerB --> parser
    parser --> runtime[EngineRuntime]
    runtime --> core[Core Engine<br/>orderbook + matching + risk]
    core --> outbox[EngineOutbox]

    outbox --> replies[(engine.replies)]
    outbox --> events[(engine.events)]
    app --> checkpoint
    app --> commit[Offset commit]

    replies --> exchange
    events --> exchange
```

## Flow

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
    A->>R: Parse, validate, match, update risk
    R-->>A: Replies and events
    A->>O: Publish output records
    O-->>RP: engine.replies / engine.events
    A->>S: Save checkpoint with next_offset
    A->>C: Commit consumed offset
```

Checkpoints are saved before input offsets are committed. If checkpoint save
fails, the engine fails loudly and does not commit the offset.

`engine_app` starts one worker per configured market. Each worker owns one
`engine.input` partition, one `EngineRuntime`, and one checkpoint namespace, so
one market stays single-threaded while different markets can run in parallel.

## Core Features

- Price-time priority matching with deterministic orderbook behavior.
- Runtime validation for place, cancel, liquidation, mark price, and funding
  inputs.
- JSON stream contract shared with the exchange repo.
- Redpanda app boundary for `engine.input`, `engine.replies`, and
  `engine.events`.
- Partition-scoped workers for multi-market parallelism.
- File and S3-compatible checkpoint stores for recovery.
- Focused benchmark harness for core, runtime, and broker-loop measurements.

## Project Structure

```text
include/core        Public core orderbook, matching, fixed point, and risk types
src/core            Core engine implementation
include/runtime     Runtime parser, output, and orchestration interfaces
src/runtime         Runtime implementation
include/broker      Broker interfaces and Redpanda app boundary
src/broker          Redpanda processing wrapper
include/checkpoint  Checkpoint interfaces and data model
src/checkpoint      File and S3 checkpoint stores
include/recovery    Checkpoint recovery interfaces
src/recovery        Recovery implementation
src/app             engine_app config and executable entrypoint
bench               Benchmark binaries and workload generators
bench-harness       Benchmark scripts
test-harness        Manual smoke and exchange e2e scripts
docs                Protocol, local development, and configuration docs
tests               Unit, fixture, recovery, broker, and smoke tests
```

## Quick Start

Use sibling checkouts:

```sh
mkdir -p ~/perpex
cd ~/perpex
git clone git@github.com:whoisasx/exchange-engine.git engine
git clone git@github.com:whoisasx/exchange-server.git exchange
```

Start exchange-owned infra:

```sh
cd ~/perpex/exchange
test-harness/infra.sh up
```

Run the engine:

```sh
cd ~/perpex/engine
test-harness/run-exchange-e2e-engine.sh
```

In another terminal, run the exchange smoke:

```sh
cd ~/perpex/exchange
test-harness/smoke.sh
```

Expected result:

```text
e2e smoke passed
e2e smoke complete
```

## Tech Stack

- Language: C++20
- Build: CMake
- Streams: Redpanda / Kafka protocol through `librdkafka++`
- Checkpoints: file store or S3-compatible object storage
- HTTP/object calls: libcurl
- Tests: CTest plus JSON fixture tests

## Documentation

- [Editable architecture diagram](docs/engine-architecture.excalidraw)
- [Local development](docs/local-development.md)
- [Configuration](docs/configuration.md)
- [Test harness](test-harness/README.md)
- [Benchmark harness](bench-harness/README.md)
- [Engine stream contract](docs/engine-contract.md)
- [Protocol fixtures](docs/examples/README.md)
