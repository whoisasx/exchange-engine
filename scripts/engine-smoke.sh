#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/engine-smoke.sh [options]

Builds the C++ engine with CMake, runs the offline engine_smoke CTest target,
then attempts a local Redpanda-backed engine_app smoke when rpk, a reachable
local Redpanda cluster, and the engine_app executable are all available.

Options:
  --skip-redpanda       Run only the offline CTest smoke.
  --require-redpanda    Fail if the Redpanda-backed smoke cannot run.
  --provision-redpanda  Compatibility alias; topics are ensured automatically
                        when the Redpanda-backed smoke runs.
  --no-redpanda-hint    Suppress extra manual Redpanda command hints.
  -h, --help            Show this help.

Environment:
  ENGINE_SMOKE_BUILD_DIR          CMake build dir (default ./build)
  ENGINE_SMOKE_BUILD_JOBS         Value passed after cmake --build --parallel
  ENGINE_SMOKE_ID                 Stable id for group/checkpoint names
  ENGINE_SMOKE_REDPANDA_POLL_LIMIT  First engine_app poll limit (default 6)
  ENGINE_SMOKE_RESTART_POLL_LIMIT   Restart engine_app poll limit (default 2)
  CEX_ENGINE_BOOTSTRAP_SERVERS    engine_app bootstrap servers
                                  (default 127.0.0.1:9092)

Redpanda smoke:
  When Redpanda is available, the script creates engine.input,
  engine.replies, and engine.events if needed, seeks a temporary group to the
  end of engine.input, produces docs/examples/engine-place-order.command.json,
  runs engine_app once, verifies a checkpoint was written, then runs engine_app
  again to exercise checkpoint recovery.

Manual Redpanda path:
  rpk topic create --if-not-exists --partitions 1 -c retention.ms=1800000 engine.input
  rpk topic alter-config engine.input --set retention.ms=1800000
  rpk topic create --if-not-exists --partitions 1 engine.replies engine.events
  rpk group seek cex-engine-smoke-manual --to end --topics engine.input --allow-new-topics
  rpk topic produce engine.input --key 1 --format '%v{json}' < docs/examples/engine-place-order.command.json
  build/engine_app --group-id cex-engine-smoke-manual --checkpoint-dir .data/engine/smoke-checkpoints/manual --poll-limit 6
  build/engine_app --group-id cex-engine-smoke-manual --checkpoint-dir .data/engine/smoke-checkpoints/manual --poll-limit 2
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
redpanda_hint=true

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
    --no-redpanda-hint)
      redpanda_hint=false
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

find_engine_app() {
  local candidate
  for candidate in \
    "$build_dir/engine_app" \
    "$build_dir/Debug/engine_app" \
    "$build_dir/Release/engine_app" \
    "$build_dir/RelWithDebInfo/engine_app" \
    "$build_dir/MinSizeRel/engine_app"; do
    if [[ -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  find "$build_dir" -maxdepth 3 -type f -name engine_app -perm -111 -print \
    2>/dev/null | head -n 1
}

ensure_redpanda_topics() {
  log "ensuring local Redpanda topics"
  rpk topic create --if-not-exists --partitions 1 \
    -c retention.ms=1800000 engine.input
  rpk topic alter-config engine.input --set retention.ms=1800000
  rpk topic create --if-not-exists --partitions 1 engine.replies engine.events
}

seek_smoke_group_to_end() {
  local group_id="$1"
  if rpk group seek "$group_id" --to end --topics engine.input \
    --allow-new-topics >/dev/null; then
    log "seeded consumer group $group_id at current engine.input end"
    return 0
  fi

  skip_redpanda \
    "rpk group seek failed; refusing to consume old engine.input records"
  return 1
}

produce_fixture() {
  local fixture="$repo_root/docs/examples/engine-place-order.command.json"
  log "producing docs/examples/engine-place-order.command.json to engine.input"
  rpk topic produce engine.input --key 1 --format '%v{json}' \
    --output-format 'produced %t[%p] offset %o\n' < "$fixture"
}

checkpoint_count() {
  local checkpoint_dir="$1"
  find "$checkpoint_dir" -type f -name '*.checkpoint' -print 2>/dev/null \
    | wc -l | tr -d '[:space:]'
}

consume_recent_topic() {
  local topic="$1"
  local count="$2"
  log "attempting bounded consume from $topic (last $count records, if any)"
  if ! rpk topic consume "$topic" --offset "@-5m:end" --num "$count" \
    --format json; then
    warn "could not consume recent records from $topic"
  fi
}

run_engine_app_pass() {
  local engine_app="$1"
  local bootstrap_servers="$2"
  local group_id="$3"
  local checkpoint_dir="$4"
  local poll_limit="$5"
  local label="$6"

  log "running engine_app $label pass with poll limit $poll_limit"
  "$engine_app" \
    --bootstrap-servers "$bootstrap_servers" \
    --group-id "$group_id" \
    --checkpoint-dir "$checkpoint_dir" \
    --poll-limit "$poll_limit"
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

  local engine_app
  engine_app="$(find_engine_app)"
  if [[ -z "$engine_app" ]]; then
    skip_redpanda \
      "build did not produce engine_app; librdkafka++ may be unavailable"
    return $?
  fi

  if ! rpk cluster info >/dev/null 2>&1; then
    skip_redpanda "rpk is installed, but no local Redpanda cluster is reachable"
    return $?
  fi

  local smoke_id
  smoke_id="${ENGINE_SMOKE_ID:-$(date +%Y%m%d%H%M%S)-$$}"
  local group_id="cex-engine-smoke-$smoke_id"
  local checkpoint_dir="$repo_root/.data/engine/smoke-checkpoints/$smoke_id"
  local bootstrap_servers="${CEX_ENGINE_BOOTSTRAP_SERVERS:-127.0.0.1:9092}"
  local poll_limit="${ENGINE_SMOKE_REDPANDA_POLL_LIMIT:-6}"
  local restart_poll_limit="${ENGINE_SMOKE_RESTART_POLL_LIMIT:-2}"

  if [[ "$bootstrap_servers" != "127.0.0.1:9092" ]]; then
    log "engine_app will use $bootstrap_servers; ensure rpk points at the same local cluster"
  fi

  mkdir -p "$checkpoint_dir"
  ensure_redpanda_topics
  if ! seek_smoke_group_to_end "$group_id"; then
    if [[ "$redpanda_mode" == "require" ]]; then
      return 1
    fi
    return 0
  fi
  produce_fixture

  run_engine_app_pass "$engine_app" "$bootstrap_servers" "$group_id" \
    "$checkpoint_dir" "$poll_limit" "initial"

  local checkpoints
  checkpoints="$(checkpoint_count "$checkpoint_dir")"
  if [[ "$checkpoints" == "0" ]]; then
    warn "engine_app completed without writing a checkpoint in $checkpoint_dir"
    return 1
  fi
  log "checkpoint files after first pass: $checkpoints"

  run_engine_app_pass "$engine_app" "$bootstrap_servers" "$group_id" \
    "$checkpoint_dir" "$restart_poll_limit" "restart/recovery"

  consume_recent_topic engine.replies 5
  consume_recent_topic engine.events 5
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

if [[ "$redpanda_hint" == true && "$redpanda_mode" != "skip" ]]; then
  cat <<'HINT'
[engine-smoke] Manual Redpanda path:
  rpk topic create --if-not-exists --partitions 1 -c retention.ms=1800000 engine.input
  rpk topic alter-config engine.input --set retention.ms=1800000
  rpk topic create --if-not-exists --partitions 1 engine.replies engine.events
  rpk group seek cex-engine-smoke-manual --to end --topics engine.input --allow-new-topics
  rpk topic produce engine.input --key 1 --format '%v{json}' < docs/examples/engine-place-order.command.json
  build/engine_app --group-id cex-engine-smoke-manual --checkpoint-dir .data/engine/smoke-checkpoints/manual --poll-limit 6
  build/engine_app --group-id cex-engine-smoke-manual --checkpoint-dir .data/engine/smoke-checkpoints/manual --poll-limit 2
  rpk topic consume engine.replies --offset "-5:end" --num 5 --format json
  rpk topic consume engine.events --offset "-5:end" --num 5 --format json
HINT
fi
