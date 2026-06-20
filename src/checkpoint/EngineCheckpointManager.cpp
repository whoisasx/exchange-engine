#include "checkpoint/EngineCheckpointManager.hpp"

#include <stdexcept>
#include <utility>

namespace cex::checkpoint {

EngineCheckpoint EngineCheckpointManager::create_checkpoint(
    const cex::runtime::EngineRuntime& runtime,
    CheckpointSourcePosition source_position,
    std::string checkpoint_id) const {
  auto runtime_snapshot = runtime.snapshot_state();

  return EngineCheckpoint{
      .schema_version = CurrentEngineCheckpointSchemaVersion,
      .checkpoint_id = std::move(checkpoint_id),
      .core_snapshot = std::move(runtime_snapshot.core_snapshot),
      .source_position = std::move(source_position),
      .public_sequences = std::move(runtime_snapshot.public_sequences),
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

  runtime.restore_state(cex::runtime::EngineRuntimeStateSnapshot{
      .core_snapshot = checkpoint.core_snapshot,
      .metadata_store = checkpoint.metadata_store,
      .public_sequences = checkpoint.public_sequences,
      .processed_input_ids = checkpoint.processed_input_ids,
      .processed_idempotency_keys = checkpoint.processed_idempotency_keys,
  });
}

}  // namespace cex::checkpoint
