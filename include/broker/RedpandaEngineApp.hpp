#pragma once

#include <cstdint>
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

class IEngineInputConsumer {
 public:
  virtual ~IEngineInputConsumer() = default;

  [[nodiscard]] virtual std::optional<ConsumedRecord> poll() = 0;
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

enum class EngineBrokerAppStatus {
  NoRecord,
  Processed,
  RejectedInputTopic,
  PublishFailed,
  CommitFailed,
  ProcessingFailed,
  UnsafeDuplicate,
};

struct EngineBrokerAppResult {
  EngineBrokerAppStatus status{EngineBrokerAppStatus::NoRecord};
  std::optional<ConsumedRecord> source;
  std::optional<runtime::EngineProcessStatus> process_status;
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
                    runtime::EngineRuntime& runtime);

  [[nodiscard]] EngineBrokerAppResult poll_once();
  [[nodiscard]] EngineBrokerAppResult consume(const ConsumedRecord& record);

 private:
  [[nodiscard]] bool duplicate_source_is_safe(
      const runtime::EngineProcessResult& process_result) const;
  void mark_committed(const OffsetCommitRequest& request);

  IEngineInputConsumer& consumer_;
  IEngineRecordProducer& producer_;
  IEngineOffsetCommitter& committer_;
  runtime::EngineRuntime& runtime_;
  std::vector<OffsetCommitRequest> committed_offsets_;
};

}  // namespace cex::broker
