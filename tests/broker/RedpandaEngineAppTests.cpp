#include "broker/RedpandaEngineApp.hpp"

#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
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
  std::optional<std::string> commit(
      const OffsetCommitRequest& request) override {
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
};

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

void test_successful_place_order_publishes_then_commits() {
  const auto place_order = place_order_fixture();
  FakeConsumer consumer({make_input_record(101, place_order)});
  FakeProducer producer;
  FakeCommitter committer;
  auto runtime = make_runtime();
  RedpandaEngineApp app(consumer, producer, committer, runtime);

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
}

}  // namespace

int main() {
  try {
    test_successful_place_order_publishes_then_commits();
    test_publish_failure_does_not_commit();
    test_duplicate_no_output_result_commits_without_publishes();
    test_wrong_input_topic_is_rejected_without_commit();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
