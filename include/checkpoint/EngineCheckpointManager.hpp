#pragma once

#include <string>

#include "checkpoint/EngineCheckpoint.hpp"

namespace cex::checkpoint {

class EngineCheckpointManager {
 public:
  [[nodiscard]] EngineCheckpoint create_checkpoint(
      const cex::runtime::EngineRuntime& runtime,
      CheckpointSourcePosition source_position,
      std::string checkpoint_id) const;

  void restore_runtime(const EngineCheckpoint& checkpoint,
                       cex::runtime::EngineRuntime& runtime) const;
};

}  // namespace cex::checkpoint
