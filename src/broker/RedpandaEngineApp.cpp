#include "broker/RedpandaEngineApp.hpp"

#include "runtime/EngineInputParser.hpp"

#include <algorithm>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

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

[[nodiscard]] cex::adapter::MarketId market_id_for(
    const runtime::ParsedEngineInput& input) {
  return std::visit(
      [](const auto& command) -> cex::adapter::MarketId {
        return command.market_id;
      },
      input.value);
}

[[nodiscard]] bool contains_market(
    const std::vector<cex::adapter::MarketId>& market_ids,
    cex::adapter::MarketId market_id) {
  return std::find(market_ids.begin(), market_ids.end(), market_id) !=
         market_ids.end();
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
                        EngineInputGuardConfig{.topic = EngineInputTopic},
                        std::move(pre_commit_hook)) {}

RedpandaEngineApp::RedpandaEngineApp(IEngineInputConsumer& consumer,
                                     IEngineRecordProducer& producer,
                                     IEngineOffsetCommitter& committer,
                                     runtime::EngineRuntime& runtime,
                                     std::string expected_input_topic,
                                     EnginePreCommitHook pre_commit_hook)
    : RedpandaEngineApp(
          consumer,
          producer,
          committer,
          runtime,
          EngineInputGuardConfig{.topic = std::move(expected_input_topic)},
          std::move(pre_commit_hook)) {}

RedpandaEngineApp::RedpandaEngineApp(IEngineInputConsumer& consumer,
                                     IEngineRecordProducer& producer,
                                     IEngineOffsetCommitter& committer,
                                     runtime::EngineRuntime& runtime,
                                     EngineInputGuardConfig guard_config,
                                     EnginePreCommitHook pre_commit_hook)
    : consumer_(consumer),
      producer_(producer),
      committer_(committer),
      runtime_(runtime),
      guard_config_(std::move(guard_config)),
      pre_commit_hook_(std::move(pre_commit_hook)) {
  if (guard_config_.topic.empty()) {
    throw std::invalid_argument("engine input topic must not be empty");
  }
  if (guard_config_.partition.has_value() && *guard_config_.partition < 0) {
    throw std::invalid_argument("engine input partition must not be negative");
  }
  for (const auto market_id : guard_config_.market_ids) {
    if (market_id <= 0) {
      throw std::invalid_argument("engine input market ids must be positive");
    }
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

  if (record.topic != guard_config_.topic) {
    result.status = EngineBrokerAppStatus::RejectedInputTopic;
    result.error = "expected input topic " + guard_config_.topic +
                   ", received " + record.topic;
    return result;
  }

  if (guard_config_.partition.has_value() &&
      record.partition != *guard_config_.partition) {
    result.status = EngineBrokerAppStatus::RejectedInputPartition;
    result.error =
        "expected input partition " +
        std::to_string(*guard_config_.partition) + ", received " +
        std::to_string(record.partition);
    return result;
  }

  if (!guard_config_.market_ids.empty()) {
    cex::adapter::MarketId market_id{0};
    try {
      runtime::EngineInputParser parser;
      market_id = market_id_for(parser.parse(record.value));
    } catch (const std::exception& error) {
      result.status = EngineBrokerAppStatus::ProcessingFailed;
      result.error = error.what();
      return result;
    }

    if (!contains_market(guard_config_.market_ids, market_id)) {
      result.status = EngineBrokerAppStatus::RejectedInputMarket;
      result.error = "market_id " + std::to_string(market_id) +
                     " is not owned by this engine worker";
      return result;
    }
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
