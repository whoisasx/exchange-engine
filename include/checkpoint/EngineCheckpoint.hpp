#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

#include "adapter/EngineAdapter.hpp"
#include "core/Snapshot.hpp"
#include "runtime/EngineRuntime.hpp"

namespace cex::checkpoint {

inline constexpr std::uint32_t CurrentEngineCheckpointSchemaVersion = 1;

struct CheckpointSourcePosition {
  std::string topic{cex::runtime::EngineInputTopic};
  std::int32_t partition{0};
  std::int64_t next_offset{0};
};

[[nodiscard]] std::optional<std::string> validate_checkpoint_source_position(
    const CheckpointSourcePosition& position);

struct EngineCheckpoint {
  std::uint32_t schema_version{CurrentEngineCheckpointSchemaVersion};
  std::string checkpoint_id;
  EngineSnapshot core_snapshot;
  CheckpointSourcePosition source_position;
  std::unordered_map<cex::adapter::MarketId, EngineSequence> public_sequences;
  cex::adapter::OrderMetadataStore metadata_store;
  std::unordered_map<std::string,
                     cex::runtime::EngineRuntimeProcessedRequestSnapshot>
      processed_input_ids;
  std::unordered_map<std::string,
                     cex::runtime::EngineRuntimeProcessedRequestSnapshot>
      processed_idempotency_keys;
};

}  // namespace cex::checkpoint
