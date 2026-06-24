#include "broker/RedpandaEngineApp.hpp"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef PROTOCOL_EXAMPLES_DIR
#error "PROTOCOL_EXAMPLES_DIR must be defined"
#endif

using namespace cex::broker;

namespace {

struct FakeSeekRequest {
  std::string topic;
  std::int32_t partition{0};
  std::int64_t offset{0};
};

class FakeConsumer final : public IEngineInputConsumer {
 public:
  explicit FakeConsumer(std::vector<ConsumedRecord> records)
      : records_(std::move(records)) {}

  std::optional<ConsumedRecord> poll() override {
    if (next_ >= records_.size()) {
      return std::nullopt;
    }
    return records_[next_++];
  }

  std::optional<std::string> seek(const std::string& topic,
                                  std::int32_t partition,
                                  std::int64_t offset) override {
    seeks.push_back(FakeSeekRequest{
        .topic = topic,
        .partition = partition,
        .offset = offset,
    });
    if (seek_error.has_value()) {
      std::optional<std::string> error = std::move(seek_error);
      seek_error.reset();
      return error;
    }

    BrokerWatermarkResult result = get_watermark(topic, partition);
    if (!result.ok()) {
      if (result.error.has_value()) {
        return result.error;
      }
      return "broker watermark unavailable";
    }
    if (auto error =
            validate_seek_offset(topic, partition, offset, *result.offsets);
        error.has_value()) {
      return error;
    }

    const auto next =
        std::find_if(records_.begin(), records_.end(),
                     [&](const ConsumedRecord& record) {
                       return record.topic == topic &&
                              record.partition == partition &&
                              record.offset >= offset;
                     });
    next_ = static_cast<std::size_t>(
        std::distance(records_.begin(), next));
    return std::nullopt;
  }

  BrokerWatermarkResult get_watermark(
      const std::string& topic,
      std::int32_t partition) override {
    if (watermark_error.has_value()) {
      return BrokerWatermarkResult{.offsets = std::nullopt,
                                   .error = watermark_error};
    }
    if (!watermark.has_value()) {
      return BrokerWatermarkResult{
          .offsets = std::nullopt,
          .error = "watermark unavailable for " + topic + "[" +
                   std::to_string(partition) + "]",
      };
    }
    return BrokerWatermarkResult{.offsets = watermark,
                                 .error = std::nullopt};
  }

  std::vector<FakeSeekRequest> seeks;
  std::optional<std::string> seek_error;
  std::optional<BrokerWatermarkOffsets> watermark{
      BrokerWatermarkOffsets{.low = 0,
                             .high = std::numeric_limits<std::int64_t>::max()}};
  std::optional<std::string> watermark_error;

 private:
  std::vector<ConsumedRecord> records_;
  std::size_t next_{0};
};

class FakeProducer final : public IEngineRecordProducer {
 public:
  std::optional<std::string> produce(
      const ProduceRequest& request) override {
    records.push_back(request);
    if (fail_next.has_value()) {
      std::optional<std::string> error = std::move(fail_next);
      fail_next.reset();
      return error;
    }
    return std::nullopt;
  }

  std::vector<ProducedRecord> records;
  std::optional<std::string> fail_next;
};

class FakeCommitter final : public IEngineOffsetCommitter {
 public:
  explicit FakeCommitter(std::vector<std::string>* operations = nullptr)
      : operations_(operations) {}

  std::optional<std::string> commit(
      const OffsetCommitRequest& request) override {
    if (operations_ != nullptr) {
      operations_->push_back("commit");
    }
    commits.push_back(request);
    if (fail_next.has_value()) {
      std::optional<std::string> error = std::move(fail_next);
      fail_next.reset();
      return error;
    }
    return std::nullopt;
  }

  std::vector<OffsetCommitRequest> commits;
  std::optional<std::string> fail_next;

 private:
  std::vector<std::string>* operations_{nullptr};
};

void assert_contains(const std::string& value, const std::string& token) {
  assert(value.find(token) != std::string::npos);
}

std::string read_file(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("failed to open " + path.string());
  }

  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

SymbolConfig make_symbol() {
  return SymbolConfig{
      .symbolId = 1,
      .baseAssetId = 1,
      .quoteAssetId = 2,
      .tickSize = Price::from_ticks(1),
      .lotSize = Quantity::from_lots(1),
      .minQuantity = Quantity::from_lots(1),
      .maxQuantity = Quantity::from_lots(1'000'000),
      .minPrice = Price::from_ticks(1),
      .maxPrice = Price::from_ticks(1'000'000),
      .ringCapacityTicks = 1'000,
      .thresholdPercentage = 10,
      .initialBaseTick = 0,
      .priceScale = 0,
      .quantityScale = 0,
      .makerFeeRate = 0,
      .takerFeeRate = 0,
      .tradingEnabled = true,
  };
}

cex::runtime::EngineRuntime make_runtime() {
  return cex::runtime::EngineRuntime(cex::runtime::EngineRuntimeConfig{
      .symbols = {make_symbol()},
      .first_public_sequence = 1,
      .clock = [] { return 1'710'000'000'000LL; },
  });
}

std::string place_order_fixture() {
  return read_file(std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
                   "engine-place-order.command.json");
}

std::string funding_settlement_tick_fixture() {
  return read_file(std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
                   "engine-funding-settlement-tick.input.json");
}

ConsumedRecord make_input_record(std::int64_t offset, std::string value) {
  return ConsumedRecord{
      .topic = EngineInputTopic,
      .partition = 0,
      .offset = offset,
      .key = std::string{"1"},
      .value = std::move(value),
  };
}

void assert_committed_source_offset(const FakeCommitter& committer,
                                    std::int64_t offset) {
  assert(committer.commits.size() == 1);
  assert(committer.commits[0].topic == EngineInputTopic);
  assert(committer.commits[0].partition == 0);
  assert(committer.commits[0].offset == offset);
}

void test_seek_uses_watermark_and_repositions_consumer() {
  FakeConsumer consumer(
      {make_input_record(10, "first"), make_input_record(11, "second")});
  consumer.watermark = BrokerWatermarkOffsets{.low = 10, .high = 12};

  const BrokerWatermarkResult watermark =
      consumer.get_watermark(EngineInputTopic, 0);
  assert(watermark.ok());
  assert(watermark.offsets->low == 10);
  assert(watermark.offsets->high == 12);

  const auto error = consumer.seek(EngineInputTopic, 0, 11);

  assert(!error.has_value());
  assert(consumer.seeks.size() == 1);
  assert(consumer.seeks[0].topic == EngineInputTopic);
  assert(consumer.seeks[0].partition == 0);
  assert(consumer.seeks[0].offset == 11);
  const auto record = consumer.poll();
  assert(record.has_value());
  assert(record->offset == 11);
  assert(record->value == "second");
}

void test_seek_below_low_watermark_fails_loudly() {
  FakeConsumer consumer({make_input_record(10, "first")});
  consumer.watermark = BrokerWatermarkOffsets{.low = 10, .high = 11};

  const auto error = consumer.seek(EngineInputTopic, 0, 9);

  assert(error.has_value());
  assert_contains(*error, "below low watermark");
  assert_contains(*error, "checkpoint");
}

void test_seek_to_high_watermark_is_valid_end_position() {
  FakeConsumer consumer({make_input_record(10, "first")});
  consumer.watermark = BrokerWatermarkOffsets{.low = 10, .high = 11};

  const auto error = consumer.seek(EngineInputTopic, 0, 11);

  assert(!error.has_value());
  assert(!consumer.poll().has_value());
}

void test_successful_place_order_publishes_then_commits() {
  const auto place_order = place_order_fixture();
  FakeConsumer consumer({make_input_record(101, place_order)});
  FakeProducer producer;
  std::vector<std::string> operations;
  FakeCommitter committer(&operations);
  auto runtime = make_runtime();
  RedpandaEngineApp app(
      consumer,
      producer,
      committer,
      runtime,
      [&](const ConsumedRecord& source,
          const cex::runtime::EngineRuntime& hook_runtime) {
        assert(source.offset == 101);
        assert(hook_runtime.metadata_store().find(9001) != nullptr);
        operations.push_back("checkpoint");
        return std::nullopt;
      });

  const auto result = app.poll_once();

  assert(result.status == EngineBrokerAppStatus::Processed);
  assert(result.committed);
  assert(result.publish_result.ok());
  assert(result.publish_result.attempted == 3);
  assert(result.publish_result.published == 3);
  assert(producer.records.size() == 3);
  assert(producer.records[0].topic == EngineRepliesTopic);
  assert(producer.records[0].key == "req_place_001");
  assert(producer.records[0].partition == 0);
  assert(producer.records[0].value.find("OrderAccepted") !=
         std::string::npos);
  assert(producer.records[1].topic == EngineEventsTopic);
  assert(producer.records[1].key == "1");
  assert(producer.records[1].partition == std::nullopt);
  assert(producer.records[2].topic == EngineEventsTopic);
  assert_committed_source_offset(committer, 101);
  assert((operations == std::vector<std::string>{"checkpoint", "commit"}));
}

void test_publish_failure_does_not_commit() {
  const auto place_order = place_order_fixture();
  FakeConsumer consumer({make_input_record(201, place_order)});
  FakeProducer producer;
  producer.fail_next = "broker unavailable";
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(consumer, producer, committer, runtime);

  const auto result = app.poll_once();

  assert(result.status == EngineBrokerAppStatus::PublishFailed);
  assert(!result.committed);
  assert(!result.publish_result.ok());
  assert(result.publish_result.attempted == 3);
  assert(result.publish_result.published == 2);
  assert(result.error == "broker unavailable");
  assert(committer.commits.empty());
}

void test_checkpoint_failure_does_not_commit() {
  const auto place_order = place_order_fixture();
  FakeConsumer consumer({make_input_record(251, place_order)});
  FakeProducer producer;
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(
      consumer,
      producer,
      committer,
      runtime,
      [](const ConsumedRecord&,
         const cex::runtime::EngineRuntime&) -> std::optional<std::string> {
        return "checkpoint disk full";
      });

  const auto result = app.poll_once();

  assert(result.status == EngineBrokerAppStatus::CheckpointFailed);
  assert(!result.committed);
  assert(result.publish_result.ok());
  assert(result.publish_result.published == 3);
  assert(producer.records.size() == 3);
  assert(committer.commits.empty());
  assert(result.error == "checkpoint disk full");
}

void test_publish_helper_uses_outbox_without_commit() {
  FakeConsumer consumer({});
  FakeProducer producer;
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(consumer, producer, committer, runtime);

  cex::runtime::EngineProcessResult process_result;
  process_result.replies.push_back(cex::runtime::EngineOutputRecord{
      .topic = EngineRepliesTopic,
      .type = "StartupReply",
      .key = "reply-key",
      .partition = 0,
      .payload = {},
  });
  process_result.events.push_back(cex::runtime::EngineOutputRecord{
      .topic = EngineEventsTopic,
      .type = "StartupEvent",
      .key = "event-key",
      .partition = std::nullopt,
      .payload = {},
  });

  const auto publish_result = app.publish(process_result);

  assert(publish_result.ok());
  assert(publish_result.attempted == 2);
  assert(publish_result.published == 2);
  assert(producer.records.size() == 2);
  assert(producer.records[0].topic == EngineRepliesTopic);
  assert(producer.records[0].key == "reply-key");
  assert(producer.records[1].topic == EngineEventsTopic);
  assert(producer.records[1].key == "event-key");
  assert(committer.commits.empty());
}

void test_publish_helper_empty_result_is_clean_noop() {
  FakeConsumer consumer({});
  FakeProducer producer;
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(consumer, producer, committer, runtime);

  const auto publish_result = app.publish(cex::runtime::EngineProcessResult{});

  assert(publish_result.ok());
  assert(publish_result.attempted == 0);
  assert(publish_result.published == 0);
  assert(producer.records.empty());
  assert(committer.commits.empty());
}

void test_duplicate_no_output_result_commits_without_publishes() {
  const auto place_order = place_order_fixture();
  FakeConsumer consumer(
      {make_input_record(301, place_order), make_input_record(302, place_order)});
  FakeProducer producer;
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(consumer, producer, committer, runtime);

  const auto first = app.poll_once();
  assert(first.status == EngineBrokerAppStatus::Processed);
  assert(first.committed);

  producer.records.clear();
  committer.commits.clear();

  const auto duplicate = app.poll_once();

  assert(duplicate.status == EngineBrokerAppStatus::Processed);
  assert(duplicate.process_status == cex::runtime::EngineProcessStatus::Duplicate);
  assert(duplicate.committed);
  assert(duplicate.publish_result.ok());
  assert(duplicate.publish_result.attempted == 0);
  assert(duplicate.publish_result.published == 0);
  assert(producer.records.empty());
  assert_committed_source_offset(committer, 302);
}

void test_rejected_no_output_result_commits_without_publishes() {
  FakeConsumer consumer({make_input_record(351,
                                           funding_settlement_tick_fixture())});
  FakeProducer producer;
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(consumer, producer, committer, runtime);

  const auto result = app.poll_once();

  assert(result.status == EngineBrokerAppStatus::Processed);
  assert(result.process_status == cex::runtime::EngineProcessStatus::Rejected);
  assert(result.committed);
  assert(result.publish_result.ok());
  assert(result.publish_result.attempted == 0);
  assert(result.publish_result.published == 0);
  assert(producer.records.empty());
  assert_committed_source_offset(committer, 351);
}

void test_wrong_input_topic_is_rejected_without_commit() {
  const auto place_order = place_order_fixture();
  ConsumedRecord wrong_topic = make_input_record(401, place_order);
  wrong_topic.topic = EngineEventsTopic;

  FakeConsumer consumer({wrong_topic});
  FakeProducer producer;
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(consumer, producer, committer, runtime);

  const auto result = app.poll_once();

  assert(result.status == EngineBrokerAppStatus::RejectedInputTopic);
  assert(!result.committed);
  assert(result.publish_result.attempted == 0);
  assert(producer.records.empty());
  assert(committer.commits.empty());
  assert(result.error == "expected input topic engine.input, received " +
                             std::string(EngineEventsTopic));
}

void test_configured_input_topic_is_processed_and_committed() {
  const std::string configured_topic = "engine.input.blue";
  const auto place_order = place_order_fixture();
  ConsumedRecord record = make_input_record(451, place_order);
  record.topic = configured_topic;

  FakeConsumer consumer({record});
  FakeProducer producer;
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(
      consumer, producer, committer, runtime, configured_topic);

  const auto result = app.poll_once();

  assert(result.status == EngineBrokerAppStatus::Processed);
  assert(result.committed);
  assert(result.publish_result.ok());
  assert(producer.records.size() == 3);
  assert(committer.commits.size() == 1);
  assert(committer.commits[0].topic == configured_topic);
  assert(committer.commits[0].partition == 0);
  assert(committer.commits[0].offset == 451);
}

void test_configured_input_topic_rejects_default_topic_without_commit() {
  const std::string configured_topic = "engine.input.blue";
  const auto place_order = place_order_fixture();
  ConsumedRecord record = make_input_record(452, place_order);

  FakeConsumer consumer({record});
  FakeProducer producer;
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(
      consumer, producer, committer, runtime, configured_topic);

  const auto result = app.poll_once();

  assert(result.status == EngineBrokerAppStatus::RejectedInputTopic);
  assert(!result.committed);
  assert(result.publish_result.attempted == 0);
  assert(producer.records.empty());
  assert(committer.commits.empty());
  assert(result.error == "expected input topic engine.input.blue, received " +
                             std::string(EngineInputTopic));
}

}  // namespace

int main() {
  try {
    test_seek_uses_watermark_and_repositions_consumer();
    test_seek_below_low_watermark_fails_loudly();
    test_seek_to_high_watermark_is_valid_end_position();
    test_successful_place_order_publishes_then_commits();
    test_publish_failure_does_not_commit();
    test_checkpoint_failure_does_not_commit();
    test_publish_helper_uses_outbox_without_commit();
    test_publish_helper_empty_result_is_clean_noop();
    test_duplicate_no_output_result_commits_without_publishes();
    test_rejected_no_output_result_commits_without_publishes();
    test_wrong_input_topic_is_rejected_without_commit();
    test_configured_input_topic_is_processed_and_committed();
    test_configured_input_topic_rejects_default_topic_without_commit();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
