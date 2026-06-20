#include "checkpoint/InMemoryCheckpointStore.hpp"

#include <utility>

namespace cex::checkpoint {

void InMemoryCheckpointStore::save(EngineCheckpoint checkpoint) {
  checkpoints_.push_back(std::move(checkpoint));
}

std::optional<EngineCheckpoint> InMemoryCheckpointStore::load_latest() const {
  if (checkpoints_.empty()) {
    return std::nullopt;
  }
  return checkpoints_.back();
}

std::size_t InMemoryCheckpointStore::size() const noexcept {
  return checkpoints_.size();
}

}  // namespace cex::checkpoint
