#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "runtime/EngineOutbox.hpp"
#include "runtime/EngineRuntime.hpp"
#include "runtime/EngineRuntimeConfig.hpp"

namespace cex::broker {

inline constexpr const char* EngineInputTopic = runtime::EngineInputTopic;
inline constexpr const char* EngineRepliesTopic = runtime::EngineRepliesTopic;
inline constexpr const char* EngineEventsTopic = runtime::EngineEventsTopic;

struct ConsumedRecord {
  std::string topic{EngineInputTopic};
  std::int32_t partition{0};
  std::int64_t offset{0};
  std::optional<std::string> key;
  std::string value;
};

struct ProducedRecord {
  std::string topic;
  std::string key;
  std::optional<std::int32_t> partition;
  std::string value;
};

using ProduceRequest = ProducedRecord;

struct OffsetCommitRequest {
  std::string topic{EngineInputTopic};
  std::int32_t partition{0};
  std::int64_t offset{0};
};

struct BrokerWatermarkOffsets {
  std::int64_t low{0};
  std::int64_t high{0};
};

struct BrokerWatermarkResult {
  std::optional<BrokerWatermarkOffsets> offsets;
  std::optional<std::string> error;

  [[nodiscard]] bool ok() const noexcept {
    return offsets.has_value() && !error.has_value();
  }
};

[[nodiscard]] inline std::optional<std::string> validate_seek_offset(
    const std::string& topic,
    std::int32_t partition,
    std::int64_t offset,
    const BrokerWatermarkOffsets& watermark) {
  if (topic.empty()) {
    return "topic must not be empty";
  }
  if (partition < 0) {
    return "partition must not be negative";
  }
  if (offset < 0) {
    return "offset must not be negative";
  }
  if (watermark.low < 0 || watermark.high < 0 ||
      watermark.low > watermark.high) {
    return "invalid broker watermark for " + topic + "[" +
           std::to_string(partition) + "]: low=" +
           std::to_string(watermark.low) + ", high=" +
           std::to_string(watermark.high);
  }
  if (offset < watermark.low) {
    return "seek offset " + std::to_string(offset) + " for " + topic + "[" +
           std::to_string(partition) + "] is below low watermark " +
           std::to_string(watermark.low) +
           "; checkpoint is no longer recoverable from broker retention";
  }
  if (offset > watermark.high) {
    return "seek offset " + std::to_string(offset) + " for " + topic + "[" +
           std::to_string(partition) + "] is above high watermark " +
           std::to_string(watermark.high) + "; offset is not available yet";
  }
  return std::nullopt;
}

class IEngineInputConsumer {
 public:
  virtual ~IEngineInputConsumer() = default;

  [[nodiscard]] virtual std::optional<ConsumedRecord> poll() = 0;
  [[nodiscard]] virtual std::optional<std::string> seek(
      const std::string& topic,
      std::int32_t partition,
      std::int64_t offset) = 0;
  [[nodiscard]] virtual BrokerWatermarkResult get_watermark(
      const std::string& topic,
      std::int32_t partition) = 0;
};

class IEngineRecordProducer {
 public:
  virtual ~IEngineRecordProducer() = default;

  [[nodiscard]] virtual std::optional<std::string> produce(
      const ProduceRequest& request) = 0;
};

class IEngineOffsetCommitter {
 public:
  virtual ~IEngineOffsetCommitter() = default;

  [[nodiscard]] virtual std::optional<std::string> commit(
      const OffsetCommitRequest& request) = 0;
};

using EnginePreCommitHook = std::function<std::optional<std::string>(
    const ConsumedRecord& source,
    const runtime::EngineRuntime& runtime)>;

enum class EngineBrokerAppStatus {
  NoRecord,
  Processed,
  RejectedInputTopic,
  PublishFailed,
  CheckpointFailed,
  CommitFailed,
  ProcessingFailed,
  UnsafeDuplicate,
};

struct EngineBrokerAppResult {
  EngineBrokerAppStatus status{EngineBrokerAppStatus::NoRecord};
  std::optional<ConsumedRecord> source;
  std::optional<runtime::EngineProcessStatus> process_status;
  std::optional<runtime::EngineTraceSummary> trace;
  runtime::EnginePublishResult publish_result;
  bool committed{false};
  std::string error;

  [[nodiscard]] bool ok() const noexcept {
    return status == EngineBrokerAppStatus::NoRecord ||
           status == EngineBrokerAppStatus::Processed;
  }
};

class RedpandaEngineApp {
 public:
  RedpandaEngineApp(IEngineInputConsumer& consumer,
                    IEngineRecordProducer& producer,
                    IEngineOffsetCommitter& committer,
                    runtime::EngineRuntime& runtime,
                    EnginePreCommitHook pre_commit_hook = {});
  RedpandaEngineApp(IEngineInputConsumer& consumer,
                    IEngineRecordProducer& producer,
                    IEngineOffsetCommitter& committer,
                    runtime::EngineRuntime& runtime,
                    std::string expected_input_topic,
                    EnginePreCommitHook pre_commit_hook = {});

  [[nodiscard]] EngineBrokerAppResult poll_once();
  [[nodiscard]] EngineBrokerAppResult consume(const ConsumedRecord& record);
  [[nodiscard]] runtime::EnginePublishResult publish(
      const runtime::EngineProcessResult& process_result);

 private:
  [[nodiscard]] bool duplicate_source_is_safe(
      const runtime::EngineProcessResult& process_result) const;
  void mark_committed(const OffsetCommitRequest& request);

  IEngineInputConsumer& consumer_;
  IEngineRecordProducer& producer_;
  IEngineOffsetCommitter& committer_;
  runtime::EngineRuntime& runtime_;
  std::string expected_input_topic_;
  EnginePreCommitHook pre_commit_hook_;
  std::vector<OffsetCommitRequest> committed_offsets_;
};

}  // namespace cex::broker
