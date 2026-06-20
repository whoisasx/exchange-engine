#pragma once

#include <optional>

#include "checkpoint/EngineCheckpoint.hpp"

namespace cex::checkpoint {

class ICheckpointStore {
 public:
  virtual ~ICheckpointStore() = default;

  virtual void save(EngineCheckpoint checkpoint) = 0;
  [[nodiscard]] virtual std::optional<EngineCheckpoint> load_latest() const = 0;
};

}  // namespace cex::checkpoint
