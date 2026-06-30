#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

COMMANDS="${ENGINE_BENCH_COMMANDS:-100000}"
WARMUP="${ENGINE_BENCH_WARMUP:-5000}"
BOOK_DEPTH="${ENGINE_BENCH_BOOK_DEPTH:-10000}"
CHECKPOINT_DELAY_US="${ENGINE_BENCH_CHECKPOINT_DELAY_US:-0}"
RESULT_ROOT="${ENGINE_BENCH_RESULT_DIR:-$ENGINE_ROOT/bench-results}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RESULT_DIR="$RESULT_ROOT/$RUN_ID"

SCENARIOS=(
  place_only
  match_heavy
  cancel_heavy
  mixed
  reject_path
  deep_book
)

mkdir -p "$RESULT_DIR"

build_bench_target engine_core_benchmark
build_bench_target engine_runtime_benchmark
build_bench_target engine_broker_loop_benchmark

for scenario in "${SCENARIOS[@]}"; do
  echo "running core $scenario"
  "$(bench_executable engine_core_benchmark)" \
    --scenario "$scenario" \
    --commands "$COMMANDS" \
    --warmup "$WARMUP" \
    --book-depth "$BOOK_DEPTH" \
    > "$RESULT_DIR/core-$scenario.json"

  echo "running runtime $scenario"
  "$(bench_executable engine_runtime_benchmark)" \
    --scenario "$scenario" \
    --commands "$COMMANDS" \
    --warmup "$WARMUP" \
    --book-depth "$BOOK_DEPTH" \
    > "$RESULT_DIR/runtime-$scenario.json"

  echo "running runtime serialized $scenario"
  "$(bench_executable engine_runtime_benchmark)" \
    --scenario "$scenario" \
    --commands "$COMMANDS" \
    --warmup "$WARMUP" \
    --book-depth "$BOOK_DEPTH" \
    --include-output-serialization \
    > "$RESULT_DIR/runtime-serialized-$scenario.json"

  echo "running broker loop $scenario"
  "$(bench_executable engine_broker_loop_benchmark)" \
    --scenario "$scenario" \
    --commands "$COMMANDS" \
    --warmup "$WARMUP" \
    --book-depth "$BOOK_DEPTH" \
    --checkpoint-delay-us "$CHECKPOINT_DELAY_US" \
    > "$RESULT_DIR/broker-loop-$scenario.json"
done

cat > "$RESULT_DIR/manifest.json" <<EOF_JSON
{
  "run_id": "$RUN_ID",
  "commands": $COMMANDS,
  "warmup": $WARMUP,
  "book_depth": $BOOK_DEPTH,
  "checkpoint_delay_us": $CHECKPOINT_DELAY_US,
  "result_dir": "$RESULT_DIR"
}
EOF_JSON

echo "benchmark results: $RESULT_DIR"
