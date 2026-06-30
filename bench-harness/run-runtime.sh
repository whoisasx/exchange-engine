#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

build_bench_target engine_runtime_benchmark
"$(bench_executable engine_runtime_benchmark)" "$@"
