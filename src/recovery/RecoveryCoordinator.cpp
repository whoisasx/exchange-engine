#include "recovery/RecoveryCoordinator.hpp"

#include <exception>
#include <optional>
#include <string>
#include <utility>

namespace cex::recovery {
namespace {

[[nodiscard]] std::string describe_position(
    const cex::checkpoint::CheckpointSourcePosition& position) {
  return position.topic + "[" + std::to_string(position.partition) +
         "] next_offset=" + std::to_string(position.next_offset);
}

[[nodiscard]] std::string describe_source(const RecoverySource& source) {
  return source.topic + "[" + std::to_string(source.partition) + "]";
}

[[nodiscard]] bool same_source(
    const cex::checkpoint::CheckpointSourcePosition& position,
    const RecoverySource& expected) {
  return position.topic == expected.topic &&
         position.partition == expected.partition;
}

[[nodiscard]] RecoveryResult with_checkpoint(
    const cex::checkpoint::EngineCheckpoint& checkpoint) {
  return RecoveryResult{
      .checkpoint_id = checkpoint.checkpoint_id,
      .source_position = checkpoint.source_position,
      .next_offset = checkpoint.source_position.next_offset,
  };
}

[[nodiscard]] RecoveryResult fail(RecoveryResult result,
                                  RecoveryStatus status,
                                  std::string error) {
  result.status = status;
  result.error = std::move(error);
  result.caught_up = false;
  return result;
}

[[nodiscard]] std::optional<RecoveryStatus> validate_recovery_offset(
    const cex::checkpoint::CheckpointSourcePosition& position,
    const cex::broker::BrokerWatermarkOffsets& watermark) {
  if (watermark.low < 0 || watermark.high < 0 ||
      watermark.low > watermark.high) {
    return RecoveryStatus::InvalidWatermark;
  }
  if (position.next_offset < watermark.low) {
    return RecoveryStatus::OffsetBelowLowWatermark;
  }
  if (position.next_offset > watermark.high) {
    return RecoveryStatus::OffsetAboveHighWatermark;
  }
  return std::nullopt;
}

[[nodiscard]] std::string invalid_offset_error(
    const cex::checkpoint::CheckpointSourcePosition& position,
    const cex::broker::BrokerWatermarkOffsets& watermark) {
  if (auto error = cex::broker::validate_seek_offset(
          position.topic, position.partition, position.next_offset, watermark);
      error.has_value()) {
    return *error;
  }
  return "invalid broker watermark for " + position.topic + "[" +
         std::to_string(position.partition) + "]: low=" +
         std::to_string(watermark.low) + ", high=" +
         std::to_string(watermark.high);
}

[[nodiscard]] cex::runtime::InboundEngineRecord to_inbound_record(
    const cex::broker::ConsumedRecord& record) {
  return cex::runtime::InboundEngineRecord{
      .topic = record.topic,
      .partition = record.partition,
      .offset = record.offset,
      .key = record.key,
      .raw_json = record.value,
  };
}

}  // namespace

RecoveryCoordinator::RecoveryCoordinator(
    cex::checkpoint::ICheckpointStore& checkpoint_store,
    cex::broker::IEngineInputConsumer& consumer,
    cex::runtime::EngineRuntime& runtime,
    cex::checkpoint::EngineCheckpointManager checkpoint_manager,
    std::optional<RecoverySource> expected_source)
    : checkpoint_store_(checkpoint_store),
      consumer_(consumer),
      runtime_(runtime),
      checkpoint_manager_(std::move(checkpoint_manager)),
      expected_source_(std::move(expected_source)) {}

RecoveryResult RecoveryCoordinator::recover() {
  return recover_impl(false);
}

RecoveryResult RecoveryCoordinator::recover_and_replay() {
  return recover_impl(true);
}

RecoveryResult RecoveryCoordinator::recover_impl(bool replay) {
  std::optional<cex::checkpoint::EngineCheckpoint> checkpoint;
  try {
    checkpoint = checkpoint_store_.load_latest();
  } catch (const std::exception& error) {
    return fail(RecoveryResult{},
                RecoveryStatus::CheckpointLoadFailed,
                error.what());
  }

  if (!checkpoint.has_value() && !expected_source_.has_value()) {
    return RecoveryResult{
        .status = RecoveryStatus::NoCheckpoint,
        .caught_up = true,
    };
  }

  if (!checkpoint.has_value()) {
    const auto& expected = *expected_source_;
    cex::checkpoint::CheckpointSourcePosition start_position{
        .topic = expected.topic,
        .partition = expected.partition,
        .next_offset = 0,
    };
    RecoveryResult result{
        .status = RecoveryStatus::NoCheckpoint,
        .source_position = start_position,
    };

    const cex::broker::BrokerWatermarkResult watermark_result =
        consumer_.get_watermark(expected.topic, expected.partition);
    if (!watermark_result.ok()) {
      std::string error = "broker watermark unavailable for " +
                          describe_source(expected);
      if (watermark_result.error.has_value()) {
        error = *watermark_result.error;
      }
      return fail(std::move(result),
                  RecoveryStatus::WatermarkUnavailable,
                  std::move(error));
    }

    result.watermark = watermark_result.offsets;
    if (watermark_result.offsets->low < 0 ||
        watermark_result.offsets->high < 0 ||
        watermark_result.offsets->low > watermark_result.offsets->high) {
      start_position.next_offset = watermark_result.offsets->low;
      result.source_position = start_position;
      return fail(std::move(result),
                  RecoveryStatus::InvalidWatermark,
                  "invalid broker watermark for " +
                      describe_source(expected) + ": low=" +
                      std::to_string(watermark_result.offsets->low) +
                      ", high=" +
                      std::to_string(watermark_result.offsets->high));
    }

    start_position.next_offset = watermark_result.offsets->low;
    result.source_position = start_position;
    result.next_offset = start_position.next_offset;
    if (auto error = consumer_.seek(expected.topic,
                                    expected.partition,
                                    start_position.next_offset);
        error.has_value()) {
      return fail(std::move(result),
                  RecoveryStatus::SeekFailed,
                  std::move(*error));
    }

    result.caught_up = result.next_offset >= watermark_result.offsets->high;
    if (replay && !result.caught_up) {
      return replay_until_caught_up(std::move(result),
                                    *watermark_result.offsets);
    }
    return result;
  }

  RecoveryResult result = with_checkpoint(*checkpoint);
  const auto& position = checkpoint->source_position;

  if (expected_source_.has_value() && !same_source(position, *expected_source_)) {
    return fail(std::move(result),
                RecoveryStatus::SourceMismatch,
                "checkpoint source " + describe_position(position) +
                    " does not match expected " +
                    describe_source(*expected_source_));
  }

  const cex::broker::BrokerWatermarkResult watermark_result =
      consumer_.get_watermark(position.topic, position.partition);
  if (!watermark_result.ok()) {
    std::string error = "broker watermark unavailable for " +
                        describe_position(position);
    if (watermark_result.error.has_value()) {
      error = *watermark_result.error;
    }
    return fail(std::move(result),
                RecoveryStatus::WatermarkUnavailable,
                std::move(error));
  }

  result.watermark = watermark_result.offsets;
  if (const auto offset_status =
          validate_recovery_offset(position, *watermark_result.offsets);
      offset_status.has_value()) {
    return fail(std::move(result),
                *offset_status,
                invalid_offset_error(position, *watermark_result.offsets));
  }

  try {
    checkpoint_manager_.restore_runtime(*checkpoint, runtime_);
  } catch (const std::exception& error) {
    return fail(std::move(result), RecoveryStatus::RestoreFailed, error.what());
  }

  if (auto error = consumer_.seek(
          position.topic, position.partition, position.next_offset);
      error.has_value()) {
    return fail(std::move(result),
                RecoveryStatus::SeekFailed,
                std::move(*error));
  }

  if (!replay) {
    result.status = RecoveryStatus::Recovered;
    result.caught_up = position.next_offset >= watermark_result.offsets->high;
    return result;
  }

  return replay_until_caught_up(std::move(result), *watermark_result.offsets);
}

RecoveryResult RecoveryCoordinator::replay_until_caught_up(
    RecoveryResult result,
    const cex::broker::BrokerWatermarkOffsets& watermark) {
  result.status = RecoveryStatus::Replayed;
  result.caught_up = result.next_offset >= watermark.high;

  while (result.next_offset < watermark.high) {
    std::optional<cex::broker::ConsumedRecord> record;
    try {
      record = consumer_.poll();
    } catch (const std::exception& error) {
      return fail(std::move(result),
                  RecoveryStatus::ReplayFailed,
                  error.what());
    }

    if (!record.has_value()) {
      result.caught_up = false;
      return result;
    }

    const auto& position = *result.source_position;
    if (record->topic != position.topic ||
        record->partition != position.partition) {
      return fail(std::move(result),
                  RecoveryStatus::ReplayFailed,
                  "polled " + record->topic + "[" +
                      std::to_string(record->partition) +
                      "] during recovery for " + describe_position(position));
    }

    if (record->offset >= watermark.high) {
      result.caught_up = true;
      return result;
    }

    try {
      cex::runtime::EngineProcessResult process_result =
          runtime_.process_replay(to_inbound_record(*record));
      result.replay_output_records +=
          static_cast<std::int64_t>(process_result.replies.size() +
                                    process_result.events.size());
    } catch (const std::exception& error) {
      return fail(std::move(result),
                  RecoveryStatus::ReplayFailed,
                  error.what());
    }

    ++result.replayed_records;
    result.last_replayed_offset = record->offset;
    result.next_offset = record->offset + 1;
    result.caught_up = result.next_offset >= watermark.high;
  }

  return result;
}

}  // namespace cex::recovery
