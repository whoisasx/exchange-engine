#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/engine-smoke.sh [options]

Builds the C++ engine with CMake, runs the offline engine_smoke CTest target,
then attempts a local Redpanda-backed smoke against an engine_app process that
is already running.

Options:
  --skip-redpanda       Run only the offline CTest smoke.
  --require-redpanda    Fail if the Redpanda-backed smoke cannot run.
  --provision-redpanda  Compatibility alias; topics are ensured automatically
                        when the Redpanda-backed smoke runs.
  -h, --help            Show this help.

Environment:
  ENGINE_SMOKE_BUILD_DIR          CMake build dir (default ./build)
  ENGINE_SMOKE_BUILD_JOBS         Value passed after cmake --build --parallel
  ENGINE_SMOKE_ID                 Stable id for input/request names

Redpanda smoke:
  When Redpanda is available, the script creates engine.input, engine.replies,
  and engine.events if needed, produces a unique PlaceOrder input, then waits
  for the already-running engine to publish matching engine.replies and
  engine.events records.

Manual Redpanda path:
  rpk topic create --if-not-exists --partitions 1 -c retention.ms=1800000 engine.input
  rpk topic alter-config engine.input --set retention.ms=1800000
  rpk topic create --if-not-exists --partitions 1 engine.replies engine.events
  rpk topic produce engine.input --key 1 --format '%v{json}' < docs/examples/engine-place-order.command.json
  rpk topic consume engine.replies --offset "-5:end" --num 5 --format json
  rpk topic consume engine.events --offset "-5:end" --num 5 --format json
USAGE
}

log() {
  printf '[engine-smoke] %s\n' "$*"
}

warn() {
  printf '[engine-smoke] %s\n' "$*" >&2
}

redpanda_mode="auto"

while (($#)); do
  case "$1" in
    --skip-redpanda)
      redpanda_mode="skip"
      ;;
    --require-redpanda)
      redpanda_mode="require"
      ;;
    --provision-redpanda)
      redpanda_mode="auto"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      warn "unknown argument: $1"
      usage >&2
      exit 2
      ;;
  esac
  shift
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd "$script_dir/.." && pwd -P)"
build_dir="${ENGINE_SMOKE_BUILD_DIR:-$repo_root/build}"
build_type="${CMAKE_BUILD_TYPE:-Debug}"
cxx_standard="${CMAKE_CXX_STANDARD:-20}"

skip_redpanda() {
  local reason="$1"
  warn "skipping Redpanda engine_app smoke: $reason"
  if [[ "$redpanda_mode" == "require" ]]; then
    return 1
  fi
  return 0
}

ensure_redpanda_topics() {
  log "ensuring local Redpanda topics"
  rpk topic create --if-not-exists --partitions 1 \
    -c retention.ms=1800000 engine.input
  rpk topic alter-config engine.input --set retention.ms=1800000
  rpk topic create --if-not-exists --partitions 1 engine.replies engine.events
}

produce_fixture() {
  local fixture="$1"
  log "producing generated PlaceOrder smoke input to engine.input"
  rpk topic produce engine.input --key 1 --format '%v{json}' \
    --output-format 'produced %t[%p] offset %o\n' < "$fixture"
}

render_fixture() {
  local smoke_id="$1"
  local order_id="$2"
  local source="$repo_root/docs/examples/engine-place-order.command.json"

  sed \
    -e "s/input_place_001/input_place_${smoke_id}/g" \
    -e "s/req_place_001/req_place_${smoke_id}/g" \
    -e "s/client-order-001/client-order-${smoke_id}/g" \
    -e "s/res_place_001/res_place_${smoke_id}/g" \
    -e "s/\"order_id\": 9001/\"order_id\": ${order_id}/" \
    "$source"
}

consume_topic_window() {
  local topic="$1"
  local count="$2"
  local seconds="${3:-5}"
  local output_file
  local status_file
  local status
  local pid

  output_file="$(mktemp)"
  status_file="$(mktemp)"
  (
    rpk topic consume "$topic" --offset "@-2m:end" --num "$count" \
      --format json >"$output_file" 2>&1
    printf '%s' "$?" >"$status_file"
  ) &
  pid="$!"

  for _ in $(seq 1 "$seconds"); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      wait "$pid" >/dev/null 2>&1 || true
      cat "$output_file"
      status="$(cat "$status_file" 2>/dev/null || printf '0')"
      rm -f "$output_file" "$status_file"
      return "$status"
    fi
    sleep 1
  done

  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
  cat "$output_file"
  rm -f "$output_file" "$status_file"
  return 124
}

wait_for_topic_value() {
  local topic="$1"
  local needle="$2"
  local label="$3"
  local output

  for _ in $(seq 1 30); do
    output="$(consume_topic_window "$topic" 20 4 || true)"
    if printf '%s\n' "$output" | grep -Fq "$needle"; then
      log "observed $label in $topic"
      printf '%s\n' "$output"
      return 0
    fi
    sleep 1
  done

  warn "did not observe $label containing $needle in $topic"
  return 1
}

cleanup_smoke_file() {
  if [[ -n "${smoke_fixture:-}" && -f "$smoke_fixture" ]]; then
    rm -f "$smoke_fixture"
  fi
}

run_redpanda_smoke() {
  if [[ "$redpanda_mode" == "skip" ]]; then
    log "Redpanda engine_app smoke skipped by request"
    return 0
  fi

  if ! command -v rpk >/dev/null 2>&1; then
    skip_redpanda "rpk is not installed"
    return $?
  fi

  if ! rpk cluster info >/dev/null 2>&1; then
    skip_redpanda "rpk is installed, but no local Redpanda cluster is reachable"
    return $?
  fi

  local smoke_id
  smoke_id="${ENGINE_SMOKE_ID:-$(date +%Y%m%d%H%M%S)-$$}"
  local order_id
  order_id="$((($(date +%s) % 1000000) * 1000 + ($$ % 1000)))"
  local input_id="input_place_${smoke_id}"

  ensure_redpanda_topics

  smoke_fixture="$(mktemp)"
  trap cleanup_smoke_file RETURN
  render_fixture "$smoke_id" "$order_id" >"$smoke_fixture"
  produce_fixture "$smoke_fixture"

  wait_for_topic_value engine.replies "$input_id" "engine reply"
  wait_for_topic_value engine.events "$input_id" "engine event"
}

log "configuring CMake build in $build_dir"
cmake -S "$repo_root" -B "$build_dir" \
  -DCMAKE_BUILD_TYPE="$build_type" \
  -DCMAKE_CXX_STANDARD="$cxx_standard" \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

build_args=(--build "$build_dir" --parallel)
if [[ -n "${ENGINE_SMOKE_BUILD_JOBS:-}" ]]; then
  build_args+=("$ENGINE_SMOKE_BUILD_JOBS")
fi

log "building engine targets"
cmake "${build_args[@]}"

log "running offline engine_smoke CTest target"
ctest --test-dir "$build_dir" --output-on-failure -R '^engine_smoke$'

run_redpanda_smoke
