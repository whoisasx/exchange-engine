# Engine Local Development

This page holds the operational setup that is intentionally kept out of the
top-level README.

## Prerequisites

- CMake 3.20+
- C++20 compiler
- libcurl
- `librdkafka++` for `engine_app`
- Docker when running Redpanda, MinIO, and the full exchange e2e flow

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

## Test

Offline smoke:

```sh
test-harness/smoke.sh --skip-redpanda
```

All CTest tests after a build:

```sh
ctest --test-dir build --output-on-failure
```

Redpanda-backed smoke requires an already-running `engine_app` and reachable
Redpanda:

```sh
test-harness/smoke.sh --require-redpanda
```

## Storage Containers

The engine e2e flow uses storage containers owned by the exchange repo harness:

```sh
cd ~/perpex/exchange
test-harness/infra.sh up
```

```mermaid
flowchart LR
    infra[exchange test-harness/infra.sh up] --> pg[(Postgres)]
    infra --> rp[(Redpanda)]
    infra --> ts[(TimescaleDB)]
    infra --> minio[(MinIO)]
    rp --> engine[engine_app]
    minio --> engine
```

| Container | Purpose | Local endpoint |
|---|---|---|
| Postgres | Main exchange DB | `postgres://postgres:postgres@127.0.0.1:55432/exchange` |
| Redpanda | `wallet.commands`, `wallet.events`, `engine.input`, `engine.replies`, `engine.events` | `127.0.0.1:19092` |
| TimescaleDB | Trade and candle time-series data | `postgres://postgres:postgres@127.0.0.1:55433/exchange_timeseries` |
| MinIO | S3-compatible engine checkpoints | `http://127.0.0.1:59000` |

`infra.sh up` creates Redpanda topics, creates the Timescale extension, creates
the MinIO bucket `exchange-checkpoints`, and clears old checkpoint objects.

Stop local infra:

```sh
cd ~/perpex/exchange
test-harness/infra.sh down
```

## Full Exchange E2E Run

Use sibling checkouts:

```sh
mkdir -p ~/perpex
cd ~/perpex
git clone git@github.com:whoisasx/exchange-engine.git engine
git clone git@github.com:whoisasx/exchange-server.git exchange
```

Start infra:

```sh
cd ~/perpex/exchange
test-harness/infra.sh up
```

Start the engine:

```sh
cd ~/perpex/engine
test-harness/run-exchange-e2e-engine.sh
```

Run exchange smoke in another terminal:

```sh
cd ~/perpex/exchange
test-harness/smoke.sh
```

Expected exchange output:

```text
e2e smoke passed
e2e smoke complete
```

Stop `engine_app` with `Ctrl-C`, then stop exchange infra:

```sh
cd ~/perpex/exchange
test-harness/infra.sh down
```

## Benchmarks

Focused core matching benchmark:

```sh
bench-harness/run-core.sh --scenario mixed --commands 100000 --warmup 5000
```

Runtime parsing and processing:

```sh
bench-harness/run-runtime.sh --scenario mixed --commands 100000 --warmup 5000
```

Runtime plus JSON output serialization:

```sh
bench-harness/run-runtime.sh --scenario mixed --commands 100000 --warmup 5000 \
  --include-output-serialization
```

Full matrix:

```sh
bench-harness/run-all.sh
```

Reports are JSON with throughput, output byte counts, and latency percentiles
including `p50`, `p90`, `p95`, `p99`, `p99.9`, and max.
