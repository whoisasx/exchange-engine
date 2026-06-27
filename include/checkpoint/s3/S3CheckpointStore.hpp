#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "checkpoint/ICheckpointStore.hpp"

namespace cex::checkpoint {

struct S3CheckpointStoreConfig {
  std::string endpoint{"http://127.0.0.1:59000"};
  std::string bucket{"exchange-checkpoints"};
  std::string access_key{"minioadmin"};
  std::string secret_key{"minioadmin"};
  std::string region{"us-east-1"};
  std::string prefix;
  std::int64_t connect_timeout_ms{3'000};
  std::int64_t request_timeout_ms{30'000};
};

class S3CheckpointStore final : public ICheckpointStore {
 public:
  explicit S3CheckpointStore(S3CheckpointStoreConfig config);

  void save(EngineCheckpoint checkpoint) override;
  [[nodiscard]] std::optional<EngineCheckpoint> load_latest() const override;

  [[nodiscard]] const S3CheckpointStoreConfig& config() const noexcept;
  [[nodiscard]] std::string checkpoint_key_for_id(
      std::string_view checkpoint_id) const;

 private:
  [[nodiscard]] std::string object_url(std::string_view key) const;
  [[nodiscard]] std::string list_url(
      std::optional<std::string_view> continuation_token) const;
  [[nodiscard]] std::vector<std::string> list_checkpoint_keys() const;

  S3CheckpointStoreConfig config_;
  std::string sigv4_scope_;
};

}  // namespace cex::checkpoint
