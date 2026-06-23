#include "checkpoint/EngineCheckpointManager.hpp"
#include "checkpoint/ICheckpointStore.hpp"
#include "recovery/RecoveryCoordinator.hpp"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <iterator>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace cex::broker;
using namespace cex::checkpoint;
using namespace cex::recovery;

namespace {

struct FakeSeekRequest {
  std::string topic;
  std::int32_t partition{0};
  std::int64_t offset{0};
};

class FakeCheckpointStore final : public ICheckpointStore {
 public:
  void save(EngineCheckpoint checkpoint) override {
    latest = std::move(checkpoint);
  }

  std::optional<EngineCheckpoint> load_latest() const override {
    ++load_calls;
    return latest;
  }

  std::optional<EngineCheckpoint> latest;
  mutable int load_calls{0};
};

class FakeConsumer final : public IEngineInputConsumer {
 public:
  explicit FakeConsumer(std::vector<ConsumedRecord> records = {})
      : records_(std::move(records)) {}

  std::optional<ConsumedRecord> poll() override {
    ++poll_calls;
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
      return seek_error;
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
    ++watermark_calls;
    (void)topic;
    (void)partition;
    if (watermark_error.has_value()) {
      return BrokerWatermarkResult{.error = watermark_error};
    }
    return BrokerWatermarkResult{.offsets = watermark};
  }

  BrokerWatermarkOffsets watermark{
      .low = 0,
      .high = std::numeric_limits<std::int64_t>::max(),
  };
  std::optional<std::string> watermark_error;
  std::optional<std::string> seek_error;
  std::vector<FakeSeekRequest> seeks;
  int poll_calls{0};
  int watermark_calls{0};

 private:
  std::vector<ConsumedRecord> records_;
  std::size_t next_{0};
};

void assert_contains(const std::string& value, const std::string& token) {
  assert(value.find(token) != std::string::npos);
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

cex::runtime::InboundEngineRecord make_runtime_record(
    std::string raw_json,
    std::int64_t offset) {
  return cex::runtime::InboundEngineRecord{
      .topic = EngineInputTopic,
      .partition = 0,
      .offset = offset,
      .key = std::string{"1"},
      .raw_json = std::move(raw_json),
  };
}

ConsumedRecord make_consumed_record(std::string value,
                                    std::int64_t offset) {
  return ConsumedRecord{
      .topic = EngineInputTopic,
      .partition = 0,
      .offset = offset,
      .key = std::string{"1"},
      .value = std::move(value),
  };
}

std::string place_order_json(const std::string& input_id = "input_place_001",
                             const std::string& request_id = "req_place_001",
                             const std::string& idempotency_key =
                                 "client-order-001",
                             std::int64_t order_id = 9001,
                             const std::string& reservation_id =
                                 "res_place_001",
                             const std::string& side = "LONG",
                             std::int64_t user_id = 42,
                             std::int64_t quantity = 10,
                             std::int64_t price = 100) {
  return R"json({
  "type": "PlaceOrder",
  "payload": {
    "input_id": ")json" +
         input_id + R"json(",
    "envelope": {
      "request_id": ")json" +
         request_id + R"json(",
      "idempotency_key": ")json" +
         idempotency_key + R"json(",
      "user_id": )json" +
         std::to_string(user_id) + R"json(,
      "reply_partition": 0
    },
    "reservation_id": ")json" +
         reservation_id + R"json(",
    "order_id": )json" +
         std::to_string(order_id) + R"json(,
    "market_id": 1,
    "market_name": "SOL-PERP",
    "side": ")json" +
         side + R"json(",
    "order_type": "LIMIT",
    "quantity": )json" +
         std::to_string(quantity) + R"json(,
    "price": )json" +
         std::to_string(price) + R"json(,
    "reduce_only": false,
    "margin_asset": "USDC",
    "reserved_margin_amount": 100,
    "leverage": 10
  }
})json";
}

std::string cancel_order_json(const std::string& input_id,
                              const std::string& request_id,
                              const std::string& idempotency_key,
                              std::int64_t order_id,
                              std::int64_t user_id) {
  return R"json({
  "type": "CancelOrder",
  "payload": {
    "input_id": ")json" +
         input_id + R"json(",
    "envelope": {
      "request_id": ")json" +
         request_id + R"json(",
      "idempotency_key": ")json" +
         idempotency_key + R"json(",
      "user_id": )json" +
         std::to_string(user_id) + R"json(,
      "reply_partition": 0
    },
    "market_id": 1,
    "order_id": )json" +
         std::to_string(order_id) + R"json(
  }
})json";
}

std::string mark_price_json(const std::string& input_id,
                            std::int64_t mark_price = 100,
                            std::int64_t index_price = 99,
                            std::int64_t source_sequence = 45'001) {
  return R"json({
  "type": "MarkPriceUpdated",
  "payload": {
    "input_id": ")json" +
         input_id + R"json(",
    "market_id": 1,
    "mark_price": )json" +
         std::to_string(mark_price) + R"json(,
    "index_price": )json" +
         std::to_string(index_price) + R"json(,
    "source_timestamp_ms": 1710000000000,
    "published_at_ms": 1710000000100,
    "valid_until_ms": 1710000005100,
    "source_sequence": )json" +
         std::to_string(source_sequence) + R"json(,
    "source_status": "VALID"
  }
})json";
}

std::string funding_rate_json(const std::string& input_id,
                              std::int64_t rate = 25,
                              std::int64_t rate_scale = 1'000'000) {
  return R"json({
  "type": "FundingRateUpdated",
  "payload": {
    "input_id": ")json" +
         input_id + R"json(",
    "market_id": 1,
    "funding_interval_id": "funding_SOL-PERP_1710000000_1710028800",
    "rate": )json" +
         std::to_string(rate) + R"json(,
    "rate_scale": )json" +
         std::to_string(rate_scale) + R"json(,
    "interval_start_ms": 1710000000000,
    "interval_end_ms": 1710028800000,
    "source_timestamp_ms": 1710000001000
  }
})json";
}

std::string funding_settlement_tick_json(const std::string& input_id) {
  return R"json({
  "type": "FundingSettlementTick",
  "payload": {
    "input_id": ")json" +
         input_id + R"json(",
    "market_id": 1,
    "funding_interval_id": "funding_SOL-PERP_1710000000_1710028800",
    "settle_at_ms": 1710028800000
  }
})json";
}

EngineCheckpoint make_checkpoint(std::int64_t next_offset,
                                 std::string checkpoint_id =
                                     "checkpoint-1") {
  auto runtime = make_runtime();
  const auto result =
      runtime.process(make_runtime_record(place_order_json(), next_offset - 1));
  assert(result.status == cex::runtime::EngineProcessStatus::Processed);

  EngineCheckpointManager manager;
  return manager.create_checkpoint(
      runtime,
      CheckpointSourcePosition{
          .topic = EngineInputTopic,
          .partition = 0,
          .next_offset = next_offset,
      },
      std::move(checkpoint_id));
}

EngineCheckpoint make_empty_checkpoint(std::int64_t next_offset) {
  auto runtime = make_runtime();

  EngineCheckpointManager manager;
  return manager.create_checkpoint(
      runtime,
      CheckpointSourcePosition{
          .topic = EngineInputTopic,
          .partition = 0,
          .next_offset = next_offset,
      },
      "empty-checkpoint");
}

void assert_book_has_order(const cex::runtime::EngineRuntime& runtime,
                           OrderId order_id) {
  const auto* book = runtime.core().get_order_book(1);
  assert(book != nullptr);
  assert(book->has_order(order_id));
}

void assert_book_does_not_have_order(const cex::runtime::EngineRuntime& runtime,
                                     OrderId order_id) {
  const auto* book = runtime.core().get_order_book(1);
  assert(book != nullptr);
  assert(!book->has_order(order_id));
}

void assert_duplicate_result(const cex::runtime::EngineProcessResult& result,
                             cex::runtime::EngineDuplicateReason reason,
                             const std::string& key,
                             std::int64_t original_offset,
                             const std::string& original_input_id) {
  assert(result.status == cex::runtime::EngineProcessStatus::Duplicate);
  assert(result.empty());
  assert(result.duplicate.has_value());
  assert(result.duplicate->reason == reason);
  assert(result.duplicate->key == key);
  assert(result.duplicate->original_topic == EngineInputTopic);
  assert(result.duplicate->original_partition == 0);
  assert(result.duplicate->original_offset == original_offset);
  assert(result.duplicate->original_input_id.has_value());
  assert(*result.duplicate->original_input_id == original_input_id);
}

void assert_duplicate_info_equivalent(
    const std::optional<cex::runtime::EngineDuplicateInfo>& left,
    const std::optional<cex::runtime::EngineDuplicateInfo>& right) {
  assert(left.has_value() == right.has_value());
  if (!left.has_value()) {
    return;
  }

  assert(left->reason == right->reason);
  assert(left->key == right->key);
  assert(left->original_topic == right->original_topic);
  assert(left->original_partition == right->original_partition);
  assert(left->original_offset == right->original_offset);
  assert(left->original_input_id == right->original_input_id);
  assert(left->original_idempotency_key == right->original_idempotency_key);
}

void assert_output_record_equivalent(
    const cex::runtime::EngineOutputRecord& left,
    const cex::runtime::EngineOutputRecord& right) {
  assert(left.topic == right.topic);
  assert(left.type == right.type);
  assert(left.key == right.key);
  assert(left.partition == right.partition);
  assert(left.payload == right.payload);
}

void assert_output_records_equivalent(
    const std::vector<cex::runtime::EngineOutputRecord>& left,
    const std::vector<cex::runtime::EngineOutputRecord>& right) {
  assert(left.size() == right.size());
  for (std::size_t i = 0; i < left.size(); ++i) {
    assert_output_record_equivalent(left[i], right[i]);
  }
}

void assert_process_results_equivalent(
    const cex::runtime::EngineProcessResult& left,
    const cex::runtime::EngineProcessResult& right) {
  assert(left.status == right.status);
  assert_duplicate_info_equivalent(left.duplicate, right.duplicate);
  assert_output_records_equivalent(left.replies, right.replies);
  assert_output_records_equivalent(left.events, right.events);
}

void assert_processed_snapshot_entry(
    const cex::runtime::EngineRuntimeStateSnapshot& snapshot,
    const std::string& input_id,
    const std::string& idempotency_key,
    std::int64_t offset) {
  const auto input = snapshot.processed_input_ids.find(input_id);
  assert(input != snapshot.processed_input_ids.end());
  assert(input->second.topic == EngineInputTopic);
  assert(input->second.partition == 0);
  assert(input->second.offset == offset);
  assert(input->second.input_id.has_value());
  assert(*input->second.input_id == input_id);
  assert(input->second.idempotency_key == idempotency_key);

  const auto idempotency =
      snapshot.processed_idempotency_keys.find(idempotency_key);
  assert(idempotency != snapshot.processed_idempotency_keys.end());
  assert(idempotency->second.topic == EngineInputTopic);
  assert(idempotency->second.partition == 0);
  assert(idempotency->second.offset == offset);
  assert(idempotency->second.input_id.has_value());
  assert(*idempotency->second.input_id == input_id);
  assert(idempotency->second.idempotency_key == idempotency_key);
}

void assert_mark_processed_snapshot_entry(
    const cex::runtime::EngineRuntimeStateSnapshot& snapshot,
    const std::string& input_id,
    std::int64_t offset) {
  const auto input = snapshot.processed_input_ids.find(input_id);
  assert(input != snapshot.processed_input_ids.end());
  assert(input->second.command_kind ==
         cex::runtime::RuntimeCommandKind::MarkPriceUpdated);
  assert(input->second.topic == EngineInputTopic);
  assert(input->second.partition == 0);
  assert(input->second.offset == offset);
  assert(input->second.input_id.has_value());
  assert(*input->second.input_id == input_id);
  assert(input->second.idempotency_key.empty());
  assert(!snapshot.processed_idempotency_keys.contains(""));
}

void assert_funding_processed_snapshot_entry(
    const cex::runtime::EngineRuntimeStateSnapshot& snapshot,
    const std::string& input_id,
    std::int64_t offset) {
  const auto input = snapshot.processed_input_ids.find(input_id);
  assert(input != snapshot.processed_input_ids.end());
  assert(input->second.command_kind ==
         cex::runtime::RuntimeCommandKind::FundingRateUpdated);
  assert(input->second.topic == EngineInputTopic);
  assert(input->second.partition == 0);
  assert(input->second.offset == offset);
  assert(input->second.input_id.has_value());
  assert(*input->second.input_id == input_id);
  assert(input->second.idempotency_key.empty());
  assert(!snapshot.processed_idempotency_keys.contains(""));
}

void assert_funding_settlement_processed_snapshot_entry(
    const cex::runtime::EngineRuntimeStateSnapshot& snapshot,
    const std::string& input_id,
    std::int64_t offset) {
  const auto input = snapshot.processed_input_ids.find(input_id);
  assert(input != snapshot.processed_input_ids.end());
  assert(input->second.command_kind ==
         cex::runtime::RuntimeCommandKind::FundingSettlementTick);
  assert(input->second.topic == EngineInputTopic);
  assert(input->second.partition == 0);
  assert(input->second.offset == offset);
  assert(input->second.input_id.has_value());
  assert(*input->second.input_id == input_id);
  assert(input->second.idempotency_key.empty());
  assert(!snapshot.processed_idempotency_keys.contains(""));
}

void assert_mark_price_state(const cex::runtime::MarkPriceState& state,
                             std::int64_t mark_price,
                             std::int64_t index_price,
                             std::int64_t source_sequence) {
  assert(state.market_id == 1);
  assert(state.mark_price == mark_price);
  assert(state.index_price == index_price);
  assert(state.source_timestamp_ms == 1'710'000'000'000LL);
  assert(state.published_at_ms == 1'710'000'000'100LL);
  assert(state.valid_until_ms == 1'710'000'005'100LL);
  assert(state.source_sequence == source_sequence);
  assert(state.source_status == "VALID");
}

void assert_funding_rate_state(
    const cex::runtime::FundingRateState& state,
    std::int64_t rate,
    std::int64_t rate_scale) {
  assert(state.market_id == 1);
  assert(state.funding_interval_id ==
         "funding_SOL-PERP_1710000000_1710028800");
  assert(state.rate == rate);
  assert(state.rate_scale == rate_scale);
  assert(state.interval_start_ms == 1'710'000'000'000LL);
  assert(state.interval_end_ms == 1'710'028'800'000LL);
  assert(state.source_timestamp_ms == 1'710'000'001'000LL);
}

void test_no_checkpoint_reports_no_recovery_needed() {
  FakeCheckpointStore store;
  FakeConsumer consumer;
  auto runtime = make_runtime();
  RecoveryCoordinator coordinator(store, consumer, runtime);

  const auto result = coordinator.recover();

  assert(result.status == RecoveryStatus::NoCheckpoint);
  assert(result.ok());
  assert(result.caught_up);
  assert(result.error.empty());
  assert(store.load_calls == 1);
  assert(consumer.watermark_calls == 0);
  assert(consumer.seeks.empty());
  assert(consumer.poll_calls == 0);
  assert_book_does_not_have_order(runtime, 9001);
}

void test_successful_recover_restores_and_seeks() {
  FakeCheckpointStore store;
  store.save(make_checkpoint(1202));
  FakeConsumer consumer;
  consumer.watermark = BrokerWatermarkOffsets{.low = 0, .high = 1300};
  auto runtime = make_runtime();
  RecoveryCoordinator coordinator(store, consumer, runtime);

  const auto result = coordinator.recover();

  assert(result.status == RecoveryStatus::Recovered);
  assert(result.ok());
  assert(!result.caught_up);
  assert(result.checkpoint_id == "checkpoint-1");
  assert(result.source_position.has_value());
  assert(result.source_position->next_offset == 1202);
  assert(result.watermark.has_value());
  assert(result.watermark->low == 0);
  assert(result.watermark->high == 1300);
  assert(result.next_offset == 1202);
  assert(result.replayed_records == 0);
  assert(consumer.watermark_calls == 1);
  assert(consumer.seeks.size() == 1);
  assert(consumer.seeks[0].topic == EngineInputTopic);
  assert(consumer.seeks[0].partition == 0);
  assert(consumer.seeks[0].offset == 1202);
  assert(consumer.poll_calls == 0);
  assert(runtime.metadata_store().find(9001) != nullptr);
  assert_book_has_order(runtime, 9001);
}

void test_retention_gap_below_low_watermark_fails() {
  FakeCheckpointStore store;
  store.save(make_checkpoint(9));
  FakeConsumer consumer;
  consumer.watermark = BrokerWatermarkOffsets{.low = 10, .high = 20};
  auto runtime = make_runtime();
  RecoveryCoordinator coordinator(store, consumer, runtime);

  const auto result = coordinator.recover();

  assert(result.status == RecoveryStatus::OffsetBelowLowWatermark);
  assert(!result.ok());
  assert_contains(result.error, "below low watermark");
  assert_contains(result.error, "checkpoint");
  assert(consumer.watermark_calls == 1);
  assert(consumer.seeks.empty());
  assert(consumer.poll_calls == 0);
  assert_book_does_not_have_order(runtime, 9001);
}

void test_checkpoint_above_high_watermark_fails() {
  FakeCheckpointStore store;
  store.save(make_checkpoint(21));
  FakeConsumer consumer;
  consumer.watermark = BrokerWatermarkOffsets{.low = 10, .high = 20};
  auto runtime = make_runtime();
  RecoveryCoordinator coordinator(store, consumer, runtime);

  const auto result = coordinator.recover();

  assert(result.status == RecoveryStatus::OffsetAboveHighWatermark);
  assert(!result.ok());
  assert_contains(result.error, "above high watermark");
  assert(consumer.watermark_calls == 1);
  assert(consumer.seeks.empty());
  assert(consumer.poll_calls == 0);
  assert_book_does_not_have_order(runtime, 9001);
}

void test_replay_silent_poll_updates_state_without_outputs() {
  const auto replayed_order =
      place_order_json("input_replay_001",
                       "req_replay_001",
                       "client-replay-001",
                       9101);
  FakeCheckpointStore store;
  store.save(make_empty_checkpoint(1201));
  FakeConsumer consumer({make_consumed_record(replayed_order, 1201)});
  consumer.watermark = BrokerWatermarkOffsets{.low = 1201, .high = 1202};
  auto runtime = make_runtime();
  RecoveryCoordinator coordinator(store, consumer, runtime);

  const auto result = coordinator.recover_and_replay();

  assert(result.status == RecoveryStatus::Replayed);
  assert(result.ok());
  assert(result.caught_up);
  assert(result.replayed_records == 1);
  assert(result.replay_output_records == 0);
  assert(result.last_replayed_offset == 1201);
  assert(result.next_offset == 1202);
  assert(consumer.seeks.size() == 1);
  assert(consumer.seeks[0].offset == 1201);
  assert(consumer.poll_calls == 1);
  assert(runtime.metadata_store().find(9101) != nullptr);
  assert_book_has_order(runtime, 9101);
  assert(runtime.market_sequences().peek(1) == 3);
}

void test_replay_recovery_matches_uninterrupted_runtime() {
  constexpr std::int64_t pre_checkpoint_offset = 1200;
  constexpr std::int64_t checkpoint_next_offset = 1201;
  constexpr std::int64_t post_checkpoint_end_offset = 1208;

  const auto pre_checkpoint_order =
      place_order_json("input_recovery_pre_001",
                       "req_recovery_pre_001",
                       "idem-recovery-pre-001",
                       9001,
                       "res_recovery_pre_001");
  const auto crossing_order =
      place_order_json("input_recovery_cross_001",
                       "req_recovery_cross_001",
                       "idem-recovery-cross-001",
                       9002,
                       "res_recovery_cross_001",
                       "SHORT",
                       43);
  const auto post_resting_order =
      place_order_json("input_recovery_rest_001",
                       "req_recovery_rest_001",
                       "idem-recovery-rest-001",
                       9201,
                       "res_recovery_rest_001",
                       "LONG",
                       44,
                       5,
                       99);
  const auto cancel_order =
      cancel_order_json("input_recovery_cancel_001",
                        "req_recovery_cancel_001",
                        "idem-recovery-cancel-001",
                        9201,
                        44);
  const auto mark_update =
      mark_price_json("input_recovery_mark_001", 101, 100, 45'002);
  const auto funding_update =
      funding_rate_json("input_recovery_funding_001", 10'000, 1'000'000);
  const auto funding_settlement =
      funding_settlement_tick_json("input_recovery_settle_001");

  auto runtime_a = make_runtime();
  const auto pre_checkpoint_result = runtime_a.process(
      make_runtime_record(pre_checkpoint_order, pre_checkpoint_offset));
  assert(pre_checkpoint_result.status ==
         cex::runtime::EngineProcessStatus::Processed);
  assert(pre_checkpoint_result.replies.size() == 1);
  assert(pre_checkpoint_result.events.size() == 2);
  assert_book_has_order(runtime_a, 9001);

  EngineCheckpointManager manager;
  FakeCheckpointStore store;
  store.save(manager.create_checkpoint(
      runtime_a,
      CheckpointSourcePosition{
          .topic = EngineInputTopic,
          .partition = 0,
          .next_offset = checkpoint_next_offset,
      },
      "checkpoint-replay-equivalence"));

  const auto live_crossing =
      runtime_a.process(make_runtime_record(crossing_order, 1201));
  assert(live_crossing.status == cex::runtime::EngineProcessStatus::Processed);
  assert(live_crossing.replies.size() == 1);
  assert(live_crossing.events.size() == 6);
  assert_book_does_not_have_order(runtime_a, 9001);
  assert_book_does_not_have_order(runtime_a, 9002);

  const auto live_mark =
      runtime_a.process(make_runtime_record(mark_update, 1202));
  assert(live_mark.status == cex::runtime::EngineProcessStatus::Processed);
  assert(live_mark.replies.empty());
  assert(live_mark.events.size() == 3);
  assert(live_mark.events[0].type == "MarkPriceUpdated");
  assert(live_mark.events[0].payload.at("engine_sequence") == "9");
  assert(live_mark.events[1].type == "RiskStateUpdated");
  assert(live_mark.events[1]
             .payload.at("engine_sequence")
             .as_number()
             ->text == "10");
  assert(live_mark.events[2].type == "RiskStateUpdated");
  assert(live_mark.events[2]
             .payload.at("engine_sequence")
             .as_number()
             ->text == "11");
  assert_mark_price_state(runtime_a.mark_prices().at(1), 101, 100, 45'002);

  const auto live_funding =
      runtime_a.process(make_runtime_record(funding_update, 1203));
  assert(live_funding.status ==
         cex::runtime::EngineProcessStatus::Processed);
  assert(live_funding.replies.empty());
  assert(live_funding.events.size() == 1);
  assert(live_funding.events[0].type == "FundingRateUpdated");
  assert(live_funding.events[0].payload.at("engine_sequence") == "12");
  assert_funding_rate_state(runtime_a.funding_rates().at(1), 10'000, 1'000'000);

  const auto live_settlement =
      runtime_a.process(make_runtime_record(funding_settlement, 1204));
  assert(live_settlement.status ==
         cex::runtime::EngineProcessStatus::Processed);
  assert(live_settlement.replies.empty());
  assert(live_settlement.events.size() == 3);
  assert(live_settlement.events[0].type == "FundingPaymentApplied");
  assert(live_settlement.events[0]
             .payload.at("engine_sequence")
             .as_number()
             ->text == "13");
  assert(live_settlement.events[1].type == "RiskStateUpdated");
  assert(live_settlement.events[1]
             .payload.at("engine_sequence")
             .as_number()
             ->text == "14");
  assert(live_settlement.events[2].type == "RiskStateUpdated");
  assert(live_settlement.events[2]
             .payload.at("engine_sequence")
             .as_number()
             ->text == "15");

  const auto live_post_resting =
      runtime_a.process(make_runtime_record(post_resting_order, 1205));
  assert(live_post_resting.status ==
         cex::runtime::EngineProcessStatus::Processed);
  assert(live_post_resting.replies.size() == 1);
  assert(live_post_resting.events.size() == 2);
  assert_book_has_order(runtime_a, 9201);

  const auto live_cancel =
      runtime_a.process(make_runtime_record(cancel_order, 1206));
  assert(live_cancel.status == cex::runtime::EngineProcessStatus::Processed);
  assert(live_cancel.replies.size() == 1);
  assert(live_cancel.events.size() == 2);
  assert_book_does_not_have_order(runtime_a, 9201);
  assert(runtime_a.metadata_store().empty());

  const auto live_duplicate =
      runtime_a.process(make_runtime_record(cancel_order, 1207));
  assert_duplicate_result(live_duplicate,
                          cex::runtime::EngineDuplicateReason::InputId,
                          "input_recovery_cancel_001",
                          1206,
                          "input_recovery_cancel_001");

  std::vector<ConsumedRecord> post_checkpoint_records{
      make_consumed_record(crossing_order, 1201),
      make_consumed_record(mark_update, 1202),
      make_consumed_record(funding_update, 1203),
      make_consumed_record(funding_settlement, 1204),
      make_consumed_record(post_resting_order, 1205),
      make_consumed_record(cancel_order, 1206),
      make_consumed_record(cancel_order, 1207),
  };
  FakeConsumer consumer(post_checkpoint_records);
  consumer.watermark = BrokerWatermarkOffsets{
      .low = checkpoint_next_offset,
      .high = post_checkpoint_end_offset,
  };

  auto runtime_b = make_runtime();
  RecoveryCoordinator coordinator(store, consumer, runtime_b);
  const auto recovery = coordinator.recover_and_replay();

  assert(recovery.status == RecoveryStatus::Replayed);
  assert(recovery.ok());
  assert(recovery.caught_up);
  assert(recovery.checkpoint_id == "checkpoint-replay-equivalence");
  assert(recovery.source_position.has_value());
  assert(recovery.source_position->next_offset == checkpoint_next_offset);
  assert(recovery.watermark.has_value());
  assert(recovery.watermark->low == checkpoint_next_offset);
  assert(recovery.watermark->high == post_checkpoint_end_offset);
  assert(recovery.replayed_records ==
         static_cast<std::int64_t>(post_checkpoint_records.size()));
  assert(recovery.replay_output_records == 0);
  assert(recovery.last_replayed_offset == 1207);
  assert(recovery.next_offset == post_checkpoint_end_offset);
  assert(consumer.watermark_calls == 1);
  assert(consumer.seeks.size() == 1);
  assert(consumer.seeks[0].topic == EngineInputTopic);
  assert(consumer.seeks[0].partition == 0);
  assert(consumer.seeks[0].offset == checkpoint_next_offset);
  assert(consumer.poll_calls ==
         static_cast<int>(post_checkpoint_records.size()));

  assert(runtime_a.metadata_store().empty());
  assert(runtime_b.metadata_store().empty());
  assert_book_does_not_have_order(runtime_a, 9001);
  assert_book_does_not_have_order(runtime_b, 9001);
  assert_book_does_not_have_order(runtime_a, 9002);
  assert_book_does_not_have_order(runtime_b, 9002);
  assert_book_does_not_have_order(runtime_a, 9201);
  assert_book_does_not_have_order(runtime_b, 9201);
  assert(runtime_a.market_sequences().peek(1) ==
         runtime_b.market_sequences().peek(1));
  assert(runtime_a.market_sequences().peek(1) == 20);
  assert_mark_price_state(runtime_b.mark_prices().at(1), 101, 100, 45'002);
  assert_funding_rate_state(runtime_b.funding_rates().at(1), 10'000, 1'000'000);

  const auto live_snapshot = runtime_a.snapshot_state();
  const auto replayed_snapshot = runtime_b.snapshot_state();
  assert(live_snapshot.public_sequences.at(1) ==
         replayed_snapshot.public_sequences.at(1));
  assert(live_snapshot.core_snapshot.sequenceState.nextSequence ==
         replayed_snapshot.core_snapshot.sequenceState.nextSequence);
  assert(live_snapshot.core_snapshot.sequenceState.nextTradeId ==
         replayed_snapshot.core_snapshot.sequenceState.nextTradeId);
  assert(live_snapshot.core_snapshot.sequenceState.nextEventId ==
         replayed_snapshot.core_snapshot.sequenceState.nextEventId);
  assert(live_snapshot.processed_input_ids.size() ==
         replayed_snapshot.processed_input_ids.size());
  assert(live_snapshot.processed_idempotency_keys.size() ==
         replayed_snapshot.processed_idempotency_keys.size());
  assert(live_snapshot.mark_prices.size() == replayed_snapshot.mark_prices.size());
  assert(live_snapshot.mark_prices.size() == 1);
  assert_mark_price_state(live_snapshot.mark_prices.at(1), 101, 100, 45'002);
  assert_mark_price_state(replayed_snapshot.mark_prices.at(1), 101, 100, 45'002);
  assert(live_snapshot.funding_rates.size() ==
         replayed_snapshot.funding_rates.size());
  assert(live_snapshot.funding_rates.size() == 1);
  assert_funding_rate_state(live_snapshot.funding_rates.at(1),
                            10'000,
                            1'000'000);
  assert_funding_rate_state(replayed_snapshot.funding_rates.at(1),
                            10'000,
                            1'000'000);
  const cex::runtime::FundingSettlementKey settlement_key{
      .market_id = 1,
      .funding_interval_id = "funding_SOL-PERP_1710000000_1710028800",
  };
  assert(live_snapshot.settled_funding_intervals ==
         replayed_snapshot.settled_funding_intervals);
  assert(live_snapshot.settled_funding_intervals.contains(settlement_key));
  assert(live_snapshot.positions == replayed_snapshot.positions);
  assert(live_snapshot.risk_states == replayed_snapshot.risk_states);
  assert(live_snapshot.positions.size() == 2);
  assert(live_snapshot.risk_states.size() == 2);
  assert(live_snapshot.risk_states.at(cex::runtime::PositionRiskKey{
      .user_id = 42,
      .market_id = 1,
  }).equity == 100);
  assert(live_snapshot.risk_states.at(cex::runtime::PositionRiskKey{
      .user_id = 43,
      .market_id = 1,
  }).equity == 100);
  assert(live_snapshot.processed_input_ids.size() == 7);
  assert(live_snapshot.processed_idempotency_keys.size() == 4);

  assert_processed_snapshot_entry(live_snapshot,
                                  "input_recovery_pre_001",
                                  "idem-recovery-pre-001",
                                  1200);
  assert_processed_snapshot_entry(replayed_snapshot,
                                  "input_recovery_pre_001",
                                  "idem-recovery-pre-001",
                                  1200);
  assert_processed_snapshot_entry(live_snapshot,
                                  "input_recovery_cross_001",
                                  "idem-recovery-cross-001",
                                  1201);
  assert_processed_snapshot_entry(replayed_snapshot,
                                  "input_recovery_cross_001",
                                  "idem-recovery-cross-001",
                                  1201);
  assert_processed_snapshot_entry(live_snapshot,
                                  "input_recovery_rest_001",
                                  "idem-recovery-rest-001",
                                  1205);
  assert_processed_snapshot_entry(replayed_snapshot,
                                  "input_recovery_rest_001",
                                  "idem-recovery-rest-001",
                                  1205);
  assert_processed_snapshot_entry(live_snapshot,
                                  "input_recovery_cancel_001",
                                  "idem-recovery-cancel-001",
                                  1206);
  assert_processed_snapshot_entry(replayed_snapshot,
                                  "input_recovery_cancel_001",
                                  "idem-recovery-cancel-001",
                                  1206);
  assert_mark_processed_snapshot_entry(live_snapshot,
                                       "input_recovery_mark_001",
                                       1202);
  assert_mark_processed_snapshot_entry(replayed_snapshot,
                                       "input_recovery_mark_001",
                                       1202);
  assert_funding_processed_snapshot_entry(live_snapshot,
                                          "input_recovery_funding_001",
                                          1203);
  assert_funding_processed_snapshot_entry(replayed_snapshot,
                                          "input_recovery_funding_001",
                                          1203);
  assert_funding_settlement_processed_snapshot_entry(
      live_snapshot,
      "input_recovery_settle_001",
      1204);
  assert_funding_settlement_processed_snapshot_entry(
      replayed_snapshot,
      "input_recovery_settle_001",
      1204);

  const auto duplicate_a =
      runtime_a.process(make_runtime_record(cancel_order, 1208));
  const auto duplicate_b =
      runtime_b.process(make_runtime_record(cancel_order, 1208));
  assert_process_results_equivalent(duplicate_a, duplicate_b);
  assert_duplicate_result(duplicate_a,
                          cex::runtime::EngineDuplicateReason::InputId,
                          "input_recovery_cancel_001",
                          1206,
                          "input_recovery_cancel_001");

  const auto next_live_order =
      place_order_json("input_recovery_next_001",
                       "req_recovery_next_001",
                       "idem-recovery-next-001",
                       9301,
                       "res_recovery_next_001",
                       "LONG",
                       45,
                       3,
                       101);
  const auto next_live_a =
      runtime_a.process(make_runtime_record(next_live_order, 1209));
  const auto next_live_b =
      runtime_b.process(make_runtime_record(next_live_order, 1209));
  assert_process_results_equivalent(next_live_a, next_live_b);
  assert(next_live_a.status == cex::runtime::EngineProcessStatus::Processed);
  assert(next_live_a.replies.size() == 1);
  assert(next_live_a.events.size() == 2);
  assert(next_live_a.events[0].type == "OrderOpened");
  assert(next_live_a.events[0].payload.at("engine_sequence") == "20");
  assert(next_live_a.events[1].type == "OrderBookDelta");
  assert(next_live_a.events[1].payload.at("engine_sequence") == "21");
  assert_book_has_order(runtime_a, 9301);
  assert_book_has_order(runtime_b, 9301);
}

}  // namespace

int main() {
  try {
    test_no_checkpoint_reports_no_recovery_needed();
    test_successful_recover_restores_and_seeks();
    test_retention_gap_below_low_watermark_fails();
    test_checkpoint_above_high_watermark_fails();
    test_replay_silent_poll_updates_state_without_outputs();
    test_replay_recovery_matches_uninterrupted_runtime();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
