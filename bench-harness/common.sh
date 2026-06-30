#!/usr/bin/env bash
set -euo pipefail

BENCH_HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_ROOT="$(cd "$BENCH_HARNESS_DIR/.." && pwd)"
BENCH_BUILD_DIR="${ENGINE_BENCH_BUILD_DIR:-$ENGINE_ROOT/build-bench}"
BENCH_BUILD_TYPE="${ENGINE_BENCH_BUILD_TYPE:-Release}"

configure_bench_build() {
  cmake \
    -S "$ENGINE_ROOT" \
    -B "$BENCH_BUILD_DIR" \
    -DCMAKE_BUILD_TYPE="$BENCH_BUILD_TYPE" \
    -DENGINE_BUILD_BENCHMARKS=ON \
    -DBUILD_TESTING=OFF
}

build_bench_target() {
  local target="$1"

  configure_bench_build
  cmake --build "$BENCH_BUILD_DIR" --target "$target" --parallel
}

bench_executable() {
  local target="$1"
  printf '%s/%s' "$BENCH_BUILD_DIR" "$target"
}
