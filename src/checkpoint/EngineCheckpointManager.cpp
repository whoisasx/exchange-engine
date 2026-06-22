#include "checkpoint/EngineCheckpointManager.hpp"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace cex::checkpoint {
namespace {

[[nodiscard]] bool blank(std::string_view value) {
  return std::ranges::all_of(value, [](unsigned char ch) {
    return std::isspace(ch) != 0;
  });
}

void require_valid_source_position(
    const CheckpointSourcePosition& source_position) {
  if (auto error = validate_checkpoint_source_position(source_position);
      error.has_value()) {
    throw std::invalid_argument(*error);
  }
}

}  // namespace

std::optional<std::string> validate_checkpoint_source_position(
    const CheckpointSourcePosition& position) {
  if (blank(position.topic)) {
    return "checkpoint source topic must not be empty";
  }
  if (position.partition < 0) {
    return "checkpoint source partition must not be negative";
  }
  if (position.next_offset < 0) {
    return "checkpoint source next_offset must not be negative";
  }
  return std::nullopt;
}

EngineCheckpoint EngineCheckpointManager::create_checkpoint(
    const cex::runtime::EngineRuntime& runtime,
    CheckpointSourcePosition source_position,
    std::string checkpoint_id) const {
  require_valid_source_position(source_position);
  auto runtime_snapshot = runtime.snapshot_state();

  return EngineCheckpoint{
      .schema_version = CurrentEngineCheckpointSchemaVersion,
      .checkpoint_id = std::move(checkpoint_id),
      .core_snapshot = std::move(runtime_snapshot.core_snapshot),
      .source_position = std::move(source_position),
      .public_sequences = std::move(runtime_snapshot.public_sequences),
      .mark_prices = std::move(runtime_snapshot.mark_prices),
      .funding_rates = std::move(runtime_snapshot.funding_rates),
      .metadata_store = std::move(runtime_snapshot.metadata_store),
      .processed_input_ids = std::move(runtime_snapshot.processed_input_ids),
      .processed_idempotency_keys =
          std::move(runtime_snapshot.processed_idempotency_keys),
  };
}

void EngineCheckpointManager::restore_runtime(
    const EngineCheckpoint& checkpoint,
    cex::runtime::EngineRuntime& runtime) const {
  if (checkpoint.schema_version != CurrentEngineCheckpointSchemaVersion) {
    throw std::invalid_argument("unsupported engine checkpoint schema version");
  }
  require_valid_source_position(checkpoint.source_position);

  runtime.restore_state(cex::runtime::EngineRuntimeStateSnapshot{
      .core_snapshot = checkpoint.core_snapshot,
      .metadata_store = checkpoint.metadata_store,
      .public_sequences = checkpoint.public_sequences,
      .mark_prices = checkpoint.mark_prices,
      .funding_rates = checkpoint.funding_rates,
      .processed_input_ids = checkpoint.processed_input_ids,
      .processed_idempotency_keys = checkpoint.processed_idempotency_keys,
  });
}

}  // namespace cex::checkpoint
