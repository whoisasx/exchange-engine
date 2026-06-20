#pragma once

#include <cstddef>
#include <vector>

#include "checkpoint/ICheckpointStore.hpp"

namespace cex::checkpoint {

class InMemoryCheckpointStore final : public ICheckpointStore {
 public:
  void save(EngineCheckpoint checkpoint) override;
  [[nodiscard]] std::optional<EngineCheckpoint> load_latest() const override;
  [[nodiscard]] std::size_t size() const noexcept;

 private:
  std::vector<EngineCheckpoint> checkpoints_;
};

}  // namespace cex::checkpoint
