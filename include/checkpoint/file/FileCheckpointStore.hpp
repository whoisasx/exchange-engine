#pragma once

#include <filesystem>
#include <optional>

#include "checkpoint/ICheckpointStore.hpp"

namespace cex::checkpoint {

class FileCheckpointStore final : public ICheckpointStore {
 public:
  explicit FileCheckpointStore(std::filesystem::path directory);

  void save(EngineCheckpoint checkpoint) override;

  // Latest selection is deterministic filename ordering over "*.checkpoint"
  // files in the store directory. save() names files from checkpoint_id, so
  // callers should use checkpoint ids that sort in commit order.
  [[nodiscard]] std::optional<EngineCheckpoint> load_latest() const override;

  [[nodiscard]] const std::filesystem::path& directory() const noexcept;

 private:
  std::filesystem::path directory_;
};

}  // namespace cex::checkpoint
