#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/engine-smoke.sh [--provision-redpanda] [--no-redpanda-hint]

Builds the C++ engine with CMake and runs the local in-process engine_smoke
CTest target. Redpanda is optional and is not required for the smoke.

Options:
  --provision-redpanda  If rpk and a local Redpanda are available, create or
                        update engine.input with one partition and
                        retention.ms=1800000.
  --no-redpanda-hint    Do not print optional rpk provisioning guidance.
USAGE
}

provision_redpanda=false
redpanda_hint=true

while (($#)); do
  case "$1" in
    --provision-redpanda)
      provision_redpanda=true
      ;;
    --no-redpanda-hint)
      redpanda_hint=false
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
build_dir="${ENGINE_SMOKE_BUILD_DIR:-$repo_root/build}"
build_type="${CMAKE_BUILD_TYPE:-Debug}"
cxx_standard="${CMAKE_CXX_STANDARD:-20}"

cmake -S "$repo_root" -B "$build_dir" \
  -DCMAKE_BUILD_TYPE="$build_type" \
  -DCMAKE_CXX_STANDARD="$cxx_standard" \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

build_args=(--build "$build_dir" --parallel)
if [[ -n "${ENGINE_SMOKE_BUILD_JOBS:-}" ]]; then
  build_args+=("$ENGINE_SMOKE_BUILD_JOBS")
fi
cmake "${build_args[@]}"

ctest --test-dir "$build_dir" --output-on-failure -R '^engine_smoke$'

if [[ "$redpanda_hint" == true ]]; then
  if command -v rpk >/dev/null 2>&1; then
    if [[ "$provision_redpanda" == true ]]; then
      if rpk cluster info >/dev/null 2>&1; then
        rpk topic create --if-not-exists --partitions 1 \
          -c retention.ms=1800000 engine.input
        rpk topic alter-config engine.input --set retention.ms=1800000
      else
        echo "rpk is installed, but no local Redpanda cluster is reachable; skipping topic provisioning."
      fi
    else
      cat <<'HINT'
rpk is installed. Optional local Redpanda provisioning was not run.
To provision the engine input topic:
  rpk topic create --if-not-exists --partitions 1 -c retention.ms=1800000 engine.input
  rpk topic alter-config engine.input --set retention.ms=1800000
Or rerun:
  scripts/engine-smoke.sh --provision-redpanda
HINT
    fi
  else
    echo "rpk not found; skipping optional Redpanda topic provisioning."
  fi
fi
