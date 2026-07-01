#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "broker/RedpandaEngineApp.hpp"
#include "checkpoint/EngineCheckpoint.hpp"
#include "checkpoint/EngineCheckpointManager.hpp"
#include "checkpoint/ICheckpointStore.hpp"
#include "runtime/EngineRuntime.hpp"

namespace cex::recovery {

enum class RecoveryStatus {
  NoCheckpoint,
  Recovered,
  Replayed,
  SourceMismatch,
  CheckpointLoadFailed,
  WatermarkUnavailable,
  InvalidWatermark,
  OffsetBelowLowWatermark,
  OffsetAboveHighWatermark,
  RestoreFailed,
  SeekFailed,
  ReplayFailed,
};

struct RecoveryResult {
  RecoveryStatus status{RecoveryStatus::NoCheckpoint};
  std::string checkpoint_id;
  std::optional<cex::checkpoint::CheckpointSourcePosition> source_position;
  std::optional<cex::broker::BrokerWatermarkOffsets> watermark;
  std::int64_t next_offset{0};
  std::int64_t replayed_records{0};
  std::int64_t replay_output_records{0};
  std::optional<std::int64_t> last_replayed_offset;
  bool caught_up{false};
  std::string error;

  [[nodiscard]] bool ok() const noexcept {
    return status == RecoveryStatus::NoCheckpoint ||
           status == RecoveryStatus::Recovered ||
           status == RecoveryStatus::Replayed;
  }
};

struct RecoverySource {
  std::string topic{cex::broker::EngineInputTopic};
  std::int32_t partition{0};
};

class RecoveryCoordinator {
 public:
  RecoveryCoordinator(cex::checkpoint::ICheckpointStore& checkpoint_store,
                      cex::broker::IEngineInputConsumer& consumer,
                      cex::runtime::EngineRuntime& runtime,
                      cex::checkpoint::EngineCheckpointManager
                          checkpoint_manager = {},
                      std::optional<RecoverySource> expected_source =
                          std::nullopt);

  [[nodiscard]] RecoveryResult recover();
  [[nodiscard]] RecoveryResult recover_and_replay();

 private:
  [[nodiscard]] RecoveryResult recover_impl(bool replay);
  [[nodiscard]] RecoveryResult replay_until_caught_up(
      RecoveryResult result,
      const cex::broker::BrokerWatermarkOffsets& watermark);

  cex::checkpoint::ICheckpointStore& checkpoint_store_;
  cex::broker::IEngineInputConsumer& consumer_;
  cex::runtime::EngineRuntime& runtime_;
  cex::checkpoint::EngineCheckpointManager checkpoint_manager_;
  std::optional<RecoverySource> expected_source_;
};

}  // namespace cex::recovery
