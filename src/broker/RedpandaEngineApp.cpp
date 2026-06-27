#include "broker/RedpandaEngineApp.hpp"

#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace cex::broker {
namespace {

class ProducerBackedPublisher final : public runtime::IEnginePublisher {
 public:
  explicit ProducerBackedPublisher(IEngineRecordProducer& producer)
      : producer_(producer) {}

  [[nodiscard]] std::optional<std::string> publish(
      const runtime::EngineOutputRecord& record,
      std::string_view serialized_json) override {
    return producer_.produce(ProduceRequest{
        .topic = record.topic,
        .key = record.key,
        .partition = record.partition,
        .value = std::string(serialized_json),
    });
  }

 private:
  IEngineRecordProducer& producer_;
};

[[nodiscard]] bool same_offset(const OffsetCommitRequest& lhs,
                               const OffsetCommitRequest& rhs) {
  return lhs.topic == rhs.topic && lhs.partition == rhs.partition &&
         lhs.offset == rhs.offset;
}

[[nodiscard]] OffsetCommitRequest source_offset(
    const ConsumedRecord& record) {
  return OffsetCommitRequest{
      .topic = record.topic,
      .partition = record.partition,
      .offset = record.offset,
  };
}

[[nodiscard]] OffsetCommitRequest source_offset(
    const runtime::EngineDuplicateInfo& duplicate) {
  return OffsetCommitRequest{
      .topic = duplicate.original_topic,
      .partition = duplicate.original_partition,
      .offset = duplicate.original_offset,
  };
}

}  // namespace

RedpandaEngineApp::RedpandaEngineApp(IEngineInputConsumer& consumer,
                                     IEngineRecordProducer& producer,
                                     IEngineOffsetCommitter& committer,
                                     runtime::EngineRuntime& runtime,
                                     EnginePreCommitHook pre_commit_hook)
    : RedpandaEngineApp(consumer,
                        producer,
                        committer,
                        runtime,
                        EngineInputTopic,
                        std::move(pre_commit_hook)) {}

RedpandaEngineApp::RedpandaEngineApp(IEngineInputConsumer& consumer,
                                     IEngineRecordProducer& producer,
                                     IEngineOffsetCommitter& committer,
                                     runtime::EngineRuntime& runtime,
                                     std::string expected_input_topic,
                                     EnginePreCommitHook pre_commit_hook)
    : consumer_(consumer),
      producer_(producer),
      committer_(committer),
      runtime_(runtime),
      expected_input_topic_(std::move(expected_input_topic)),
      pre_commit_hook_(std::move(pre_commit_hook)) {
  if (expected_input_topic_.empty()) {
    throw std::invalid_argument("engine input topic must not be empty");
  }
}

EngineBrokerAppResult RedpandaEngineApp::poll_once() {
  auto record = consumer_.poll();
  if (!record.has_value()) {
    return EngineBrokerAppResult{
        .status = EngineBrokerAppStatus::NoRecord,
    };
  }

  return consume(*record);
}

EngineBrokerAppResult RedpandaEngineApp::consume(
    const ConsumedRecord& record) {
  EngineBrokerAppResult result{
      .source = record,
  };

  if (record.topic != expected_input_topic_) {
    result.status = EngineBrokerAppStatus::RejectedInputTopic;
    result.error = "expected input topic " + expected_input_topic_ +
                   ", received " + record.topic;
    return result;
  }

  runtime::EngineProcessResult process_result;
  try {
    process_result = runtime_.process(runtime::InboundEngineRecord{
        .topic = record.topic,
        .partition = record.partition,
        .offset = record.offset,
        .key = record.key,
        .raw_json = record.value,
    });
  } catch (const std::exception& error) {
    result.status = EngineBrokerAppStatus::ProcessingFailed;
    result.error = error.what();
    return result;
  }

  result.process_status = process_result.status;
  result.trace = process_result.trace_summary();

  result.publish_result = publish(process_result);
  if (!result.publish_result.ok()) {
    result.status = EngineBrokerAppStatus::PublishFailed;
    result.error = result.publish_result.failures.front().error;
    return result;
  }

  if (process_result.status == runtime::EngineProcessStatus::Duplicate &&
      !duplicate_source_is_safe(process_result)) {
    result.status = EngineBrokerAppStatus::UnsafeDuplicate;
    result.error = "duplicate source offset has not been committed";
    return result;
  }

  const OffsetCommitRequest commit_request = source_offset(record);
  if (pre_commit_hook_) {
    try {
      if (auto error = pre_commit_hook_(record, runtime_); error.has_value()) {
        result.status = EngineBrokerAppStatus::CheckpointFailed;
        result.error = std::move(*error);
        return result;
      }
    } catch (const std::exception& error) {
      result.status = EngineBrokerAppStatus::CheckpointFailed;
      result.error = error.what();
      return result;
    }
  }

  if (auto error = committer_.commit(commit_request); error.has_value()) {
    result.status = EngineBrokerAppStatus::CommitFailed;
    result.error = std::move(*error);
    return result;
  }

  mark_committed(commit_request);
  result.status = EngineBrokerAppStatus::Processed;
  result.committed = true;
  return result;
}

runtime::EnginePublishResult RedpandaEngineApp::publish(
    const runtime::EngineProcessResult& process_result) {
  ProducerBackedPublisher publisher(producer_);
  runtime::EngineOutbox outbox(publisher);
  return outbox.publish(process_result);
}

bool RedpandaEngineApp::duplicate_source_is_safe(
    const runtime::EngineProcessResult& process_result) const {
  if (!process_result.duplicate.has_value()) {
    return false;
  }

  const OffsetCommitRequest original =
      source_offset(*process_result.duplicate);
  for (const auto& committed : committed_offsets_) {
    if (same_offset(committed, original)) {
      return true;
    }
  }
  return false;
}

void RedpandaEngineApp::mark_committed(
    const OffsetCommitRequest& request) {
  for (const auto& committed : committed_offsets_) {
    if (same_offset(committed, request)) {
      return;
    }
  }
  committed_offsets_.push_back(request);
}

}  // namespace cex::broker
