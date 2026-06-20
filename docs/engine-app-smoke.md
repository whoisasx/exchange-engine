# Engine App Smoke Harness

`scripts/engine-smoke.sh` is the local C++ engine smoke entrypoint. It always
builds the repo and runs the offline `engine_smoke` CTest target. When local
Redpanda prerequisites are present, it also runs a broker-backed `engine_app`
smoke against `engine.input`, `engine.replies`, and `engine.events`.

## Prerequisites

Offline smoke only:

- CMake and a C++20 compiler.

Optional Redpanda-backed smoke:

- `librdkafka++` development headers and library available at CMake configure
  time. Without this, CMake intentionally skips `engine_app`, and the script
  prints a skip message.
- `rpk` installed.
- A local Redpanda cluster reachable by `rpk cluster info`.
- `engine_app` bootstrap servers matching the same local cluster. The default
  is `127.0.0.1:9092`; override with `CEX_ENGINE_BOOTSTRAP_SERVERS` if needed
  and make sure the active `rpk` profile points to the same cluster.

The script does not delete, truncate, or recreate existing topics. It uses a
temporary consumer group named `cex-engine-smoke-<id>`, seeks that group to the
current end of `engine.input`, then produces the fixture. This keeps the smoke
from processing older local records already in the topic.

## Script Usage

Run the standard local smoke:

```sh
scripts/engine-smoke.sh
```

Run only the offline CTest smoke:

```sh
scripts/engine-smoke.sh --skip-redpanda
```

Require the Redpanda-backed path to run and fail otherwise:

```sh
scripts/engine-smoke.sh --require-redpanda
```

Useful environment overrides:

```sh
ENGINE_SMOKE_ID=manual-001 scripts/engine-smoke.sh
CEX_ENGINE_BOOTSTRAP_SERVERS=127.0.0.1:19092 scripts/engine-smoke.sh
ENGINE_SMOKE_REDPANDA_POLL_LIMIT=8 ENGINE_SMOKE_RESTART_POLL_LIMIT=2 scripts/engine-smoke.sh
```

## What The Redpanda Path Does

1. Ensures `engine.input`, `engine.replies`, and `engine.events` exist.
2. Ensures `engine.input` has one partition and `retention.ms=1800000` when
   topic creation/config alteration is allowed by the local cluster.
3. Seeks the temporary smoke consumer group to the current end of
   `engine.input`.
4. Produces `docs/examples/engine-place-order.command.json` to `engine.input`
   with key `1`.
5. Runs `engine_app` with a bounded poll limit.
6. Checks that a checkpoint file was written.
7. Runs `engine_app` again with the same group and checkpoint directory to
   exercise startup recovery from the checkpoint.
8. Attempts bounded consumes from recent `engine.replies` and `engine.events`
   records.

## Manual Redpanda Commands

These are the equivalent manual commands after a CMake build has produced
`build/engine_app`:

```sh
rpk topic create --if-not-exists --partitions 1 -c retention.ms=1800000 engine.input
rpk topic alter-config engine.input --set retention.ms=1800000
rpk topic create --if-not-exists --partitions 1 engine.replies engine.events

smoke_id=manual-$(date +%Y%m%d%H%M%S)
group_id="cex-engine-smoke-$smoke_id"
checkpoint_dir=".data/engine/smoke-checkpoints/$smoke_id"

rpk group seek "$group_id" --to end --topics engine.input --allow-new-topics
rpk topic produce engine.input --key 1 --format '%v{json}' < docs/examples/engine-place-order.command.json

build/engine_app --group-id "$group_id" --checkpoint-dir "$checkpoint_dir" --poll-limit 6
build/engine_app --group-id "$group_id" --checkpoint-dir "$checkpoint_dir" --poll-limit 2

rpk topic consume engine.replies --offset "-5:end" --num 5 --format json
rpk topic consume engine.events --offset "-5:end" --num 5 --format json
```

For the baseline repository verification that does not require a Redpanda
daemon, run:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_STANDARD=20 -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
bash -n scripts/engine-smoke.sh
git diff --check
```
