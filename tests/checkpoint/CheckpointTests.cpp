#include "checkpoint/EngineCheckpointManager.hpp"
#include "checkpoint/InMemoryCheckpointStore.hpp"

#include <cassert>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace cex::checkpoint;
using namespace cex::runtime;

namespace {

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

EngineRuntime make_runtime() {
  return EngineRuntime(EngineRuntimeConfig{
      .symbols = {make_symbol()},
      .first_public_sequence = 1,
      .clock = [] { return 1'710'000'000'000LL; },
  });
}

InboundEngineRecord make_record(std::string raw_json, std::int64_t offset) {
  return InboundEngineRecord{
      .topic = EngineInputTopic,
      .partition = 0,
      .offset = offset,
      .key = std::string{"1"},
      .raw_json = std::move(raw_json),
  };
}

const EngineOutputRecord* find_record(
    const std::vector<EngineOutputRecord>& records,
    const std::string& type) {
  for (const auto& record : records) {
    if (record.type == type) {
      return &record;
    }
  }
  return nullptr;
}

void assert_contains(const std::string& value, const std::string& token) {
  assert(value.find(token) != std::string::npos);
}

void assert_mark_price_state(const MarkPriceState& state) {
  assert(state.market_id == 1);
  assert(state.mark_price == 100);
  assert(state.index_price == 99);
  assert(state.source_timestamp_ms == 1'710'000'000'000LL);
  assert(state.published_at_ms == 1'710'000'000'100LL);
  assert(state.valid_until_ms == 1'710'000'005'100LL);
  assert(state.source_sequence == 45'001);
  assert(state.source_status == "VALID");
}

void assert_funding_rate_state(const FundingRateState& state) {
  assert(state.market_id == 1);
  assert(state.funding_interval_id ==
         "funding_SOL-PERP_1710000000_1710028800");
  assert(state.rate == 25);
  assert(state.rate_scale == 1'000'000);
  assert(state.interval_start_ms == 1'710'000'000'000LL);
  assert(state.interval_end_ms == 1'710'028'800'000LL);
  assert(state.source_timestamp_ms == 1'710'000'001'000LL);
}

std::string place_order_json() {
  return R"json({
  "type": "PlaceOrder",
  "payload": {
    "input_id": "input_place_001",
    "envelope": {
      "request_id": "req_place_001",
      "idempotency_key": "client-order-001",
      "user_id": 42,
      "reply_partition": 0
    },
    "reservation_id": "res_place_001",
    "order_id": 9001,
    "market_id": 1,
    "market_name": "SOL-PERP",
    "side": "LONG",
    "order_type": "LIMIT",
    "quantity": 10,
    "price": 100,
    "reduce_only": false,
    "margin_asset": "USDC",
    "reserved_margin_amount": 100,
    "leverage": 10
  }
})json";
}

std::string crossing_order_json() {
  return R"json({
  "type": "PlaceOrder",
  "payload": {
    "input_id": "input_place_002",
    "envelope": {
      "request_id": "req_place_002",
      "idempotency_key": "client-order-002",
      "user_id": 43,
      "reply_partition": 0
    },
    "reservation_id": "res_taker_001",
    "order_id": 9002,
    "market_id": 1,
    "market_name": "SOL-PERP",
    "side": "SHORT",
    "order_type": "LIMIT",
    "quantity": 10,
    "price": 100,
    "reduce_only": false,
    "margin_asset": "USDC",
    "reserved_margin_amount": 100,
    "leverage": 10
  }
})json";
}

std::string cancel_order_json() {
  return R"json({
  "type": "CancelOrder",
  "payload": {
    "input_id": "input_cancel_001",
    "envelope": {
      "request_id": "req_cancel_001",
      "idempotency_key": "client-cancel-001",
      "user_id": 42,
      "reply_partition": 0
    },
    "market_id": 1,
    "order_id": 9001
  }
})json";
}

std::string mark_price_json() {
  return R"json({
  "type": "MarkPriceUpdated",
  "payload": {
    "input_id": "input_mark_001",
    "market_id": 1,
    "mark_price": 100,
    "index_price": 99,
    "source_timestamp_ms": 1710000000000,
    "published_at_ms": 1710000000100,
    "valid_until_ms": 1710000005100,
    "source_sequence": 45001,
    "source_status": "VALID"
  }
})json";
}

std::string funding_rate_json() {
  return R"json({
  "type": "FundingRateUpdated",
  "payload": {
    "input_id": "input_funding_rate_001",
    "market_id": 1,
    "funding_interval_id": "funding_SOL-PERP_1710000000_1710028800",
    "rate": 25,
    "rate_scale": 1000000,
    "interval_start_ms": 1710000000000,
    "interval_end_ms": 1710028800000,
    "source_timestamp_ms": 1710000001000
  }
})json";
}

std::string funding_rate_settlement_json() {
  return R"json({
  "type": "FundingRateUpdated",
  "payload": {
    "input_id": "input_funding_rate_settle_001",
    "market_id": 1,
    "funding_interval_id": "funding_SOL-PERP_1710000000_1710028800",
    "rate": 10000,
    "rate_scale": 1000000,
    "interval_start_ms": 1710000000000,
    "interval_end_ms": 1710028800000,
    "source_timestamp_ms": 1710000001000
  }
})json";
}

std::string funding_settlement_tick_json() {
  return R"json({
  "type": "FundingSettlementTick",
  "payload": {
    "input_id": "input_funding_settle_001",
    "market_id": 1,
    "funding_interval_id": "funding_SOL-PERP_1710000000_1710028800",
    "settle_at_ms": 1710028800000
  }
})json";
}

void assert_position_and_risk_state(const EngineRuntimeStateSnapshot& snapshot) {
  assert(snapshot.positions.size() == 2);
  assert(snapshot.risk_states.size() == 2);

  const auto maker_key = PositionRiskKey{.user_id = 42, .market_id = 1};
  const auto taker_key = PositionRiskKey{.user_id = 43, .market_id = 1};
  assert(snapshot.positions.at(maker_key).signed_quantity == 10);
  assert(snapshot.positions.at(maker_key).average_entry_price == 100);
  assert(snapshot.positions.at(maker_key).isolated_margin == 100);
  assert(snapshot.positions.at(taker_key).signed_quantity == -10);
  assert(snapshot.positions.at(taker_key).average_entry_price == 100);
  assert(snapshot.positions.at(taker_key).isolated_margin == 100);
  assert(snapshot.risk_states.at(maker_key).status == "HEALTHY");
  assert(snapshot.risk_states.at(maker_key).unrealized_pnl == 0);
  assert(snapshot.risk_states.at(maker_key).equity == 100);
  assert(snapshot.risk_states.at(maker_key).maintenance_margin == 50);
  assert(snapshot.risk_states.at(maker_key).margin_ratio == 20'000);
  assert(snapshot.risk_states.at(taker_key).status == "HEALTHY");
  assert(snapshot.risk_states.at(taker_key).unrealized_pnl == 0);
  assert(snapshot.risk_states.at(taker_key).equity == 100);
  assert(snapshot.risk_states.at(taker_key).maintenance_margin == 50);
  assert(snapshot.risk_states.at(taker_key).margin_ratio == 20'000);
}

EngineCheckpoint make_checkpoint(std::string checkpoint_id = "checkpoint-1") {
  auto runtime = make_runtime();
  const auto result = runtime.process(make_record(place_order_json(), 1201));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.events.size() == 2);

  EngineCheckpointManager manager;
  return manager.create_checkpoint(
      runtime,
      CheckpointSourcePosition{
          .topic = EngineInputTopic,
          .partition = 0,
          .next_offset = 1202,
      },
      std::move(checkpoint_id));
}

EngineCheckpoint make_position_checkpoint(
    std::string checkpoint_id = "position-checkpoint-1") {
  auto runtime = make_runtime();
  (void)runtime.process(make_record(place_order_json(), 1401));
  const auto result =
      runtime.process(make_record(crossing_order_json(), 1402));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.events.size() == 6);

  EngineCheckpointManager manager;
  return manager.create_checkpoint(
      runtime,
      CheckpointSourcePosition{
          .topic = EngineInputTopic,
          .partition = 0,
          .next_offset = 1403,
      },
      std::move(checkpoint_id));
}

EngineCheckpoint make_mark_checkpoint(
    std::string checkpoint_id = "mark-checkpoint-1") {
  auto runtime = make_runtime();
  const auto result = runtime.process(make_record(mark_price_json(), 1301));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.empty());
  assert(result.events.size() == 1);

  EngineCheckpointManager manager;
  return manager.create_checkpoint(
      runtime,
      CheckpointSourcePosition{
          .topic = EngineInputTopic,
          .partition = 0,
          .next_offset = 1302,
      },
      std::move(checkpoint_id));
}

EngineCheckpoint make_funding_checkpoint(
    std::string checkpoint_id = "funding-checkpoint-1") {
  auto runtime = make_runtime();
  const auto result = runtime.process(make_record(funding_rate_json(), 1311));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.empty());
  assert(result.events.size() == 1);

  EngineCheckpointManager manager;
  return manager.create_checkpoint(
      runtime,
      CheckpointSourcePosition{
          .topic = EngineInputTopic,
          .partition = 0,
          .next_offset = 1312,
      },
      std::move(checkpoint_id));
}

EngineCheckpoint make_settled_funding_checkpoint(
    std::string checkpoint_id = "settled-funding-checkpoint-1") {
  auto runtime = make_runtime();
  (void)runtime.process(make_record(place_order_json(), 1501));
  const auto fill = runtime.process(make_record(crossing_order_json(), 1502));
  assert(fill.status == EngineProcessStatus::Processed);
  (void)runtime.process(make_record(funding_rate_settlement_json(), 1503));
  const auto settlement =
      runtime.process(make_record(funding_settlement_tick_json(), 1504));
  assert(settlement.status == EngineProcessStatus::Processed);
  assert(settlement.events.size() == 3);

  EngineCheckpointManager manager;
  return manager.create_checkpoint(
      runtime,
      CheckpointSourcePosition{
          .topic = EngineInputTopic,
          .partition = 0,
          .next_offset = 1505,
      },
      std::move(checkpoint_id));
}

void test_create_checkpoint_captures_recovery_state() {
  const auto checkpoint = make_checkpoint();

  assert(checkpoint.schema_version == CurrentEngineCheckpointSchemaVersion);
  assert(checkpoint.checkpoint_id == "checkpoint-1");
  assert(checkpoint.source_position.topic == EngineInputTopic);
  assert(checkpoint.source_position.partition == 0);
  assert(checkpoint.source_position.next_offset == 1202);

  assert(checkpoint.core_snapshot.symbolSnapshots.size() == 1);
  const auto& book_snapshot = checkpoint.core_snapshot.symbolSnapshots[0];
  assert(book_snapshot.symbolId == 1);
  assert(book_snapshot.activeOrders.size() == 1);
  assert(book_snapshot.activeOrders[0].orderId == 9001);
  assert(book_snapshot.activeOrders[0].remainingQuantity.lots() == 10);

  assert(checkpoint.public_sequences.at(1) == 3);
  assert(checkpoint.metadata_store.find(9001) != nullptr);
  assert(checkpoint.processed_input_ids.contains("input_place_001"));
  assert(checkpoint.processed_idempotency_keys.contains("client-order-001"));
}

void test_checkpoint_restore_preserves_mark_price_state() {
  const auto checkpoint = make_mark_checkpoint();

  assert(checkpoint.public_sequences.at(1) == 2);
  assert(checkpoint.mark_prices.contains(1));
  assert_mark_price_state(checkpoint.mark_prices.at(1));
  assert(checkpoint.processed_input_ids.contains("input_mark_001"));
  assert(checkpoint.processed_input_ids.at("input_mark_001").command_kind ==
         RuntimeCommandKind::MarkPriceUpdated);
  assert(checkpoint.processed_input_ids.at("input_mark_001").idempotency_key.empty());
  assert(checkpoint.processed_idempotency_keys.empty());

  auto restored = make_runtime();
  EngineCheckpointManager manager;
  manager.restore_runtime(checkpoint, restored);

  const auto restored_snapshot = restored.snapshot_state();
  assert(restored_snapshot.public_sequences.at(1) == 2);
  assert(restored_snapshot.mark_prices.contains(1));
  assert_mark_price_state(restored_snapshot.mark_prices.at(1));

  const auto duplicate = restored.process_replay(make_record(mark_price_json(), 1301));
  assert(duplicate.status == EngineProcessStatus::Duplicate);
  assert(duplicate.empty());
  assert(duplicate.duplicate.has_value());
  assert(duplicate.duplicate->reason == EngineDuplicateReason::InputId);
  assert(duplicate.duplicate->original_offset == 1301);
  assert(restored.market_sequences().peek(1) == 2);
}

void test_checkpoint_restore_preserves_funding_rate_state() {
  const auto checkpoint = make_funding_checkpoint();

  assert(checkpoint.public_sequences.at(1) == 2);
  assert(checkpoint.funding_rates.contains(1));
  assert_funding_rate_state(checkpoint.funding_rates.at(1));
  assert(checkpoint.processed_input_ids.contains("input_funding_rate_001"));
  assert(checkpoint.processed_input_ids.at("input_funding_rate_001")
             .command_kind == RuntimeCommandKind::FundingRateUpdated);
  assert(checkpoint.processed_input_ids.at("input_funding_rate_001")
             .idempotency_key.empty());
  assert(checkpoint.processed_idempotency_keys.empty());

  auto restored = make_runtime();
  EngineCheckpointManager manager;
  manager.restore_runtime(checkpoint, restored);

  const auto restored_snapshot = restored.snapshot_state();
  assert(restored_snapshot.public_sequences.at(1) == 2);
  assert(restored_snapshot.funding_rates.contains(1));
  assert_funding_rate_state(restored_snapshot.funding_rates.at(1));

  const auto duplicate =
      restored.process_replay(make_record(funding_rate_json(), 1311));
  assert(duplicate.status == EngineProcessStatus::Duplicate);
  assert(duplicate.empty());
  assert(duplicate.duplicate.has_value());
  assert(duplicate.duplicate->reason == EngineDuplicateReason::InputId);
  assert(duplicate.duplicate->original_offset == 1311);
  assert(restored.market_sequences().peek(1) == 2);
}

void test_checkpoint_restore_preserves_position_and_risk_state() {
  const auto checkpoint = make_position_checkpoint();

  assert(checkpoint.public_sequences.at(1) == 9);
  assert(checkpoint.positions.size() == 2);
  assert(checkpoint.risk_states.size() == 2);

  auto restored = make_runtime();
  EngineCheckpointManager manager;
  manager.restore_runtime(checkpoint, restored);

  const auto restored_snapshot = restored.snapshot_state();
  assert(restored_snapshot.public_sequences.at(1) == 9);
  assert_position_and_risk_state(restored_snapshot);
  assert(restored.metadata_store().empty());
}

void test_checkpoint_restore_preserves_settled_funding_and_adjusted_risk() {
  const auto checkpoint = make_settled_funding_checkpoint();
  const FundingSettlementKey settlement_key{
      .market_id = 1,
      .funding_interval_id = "funding_SOL-PERP_1710000000_1710028800",
  };
  const auto maker_key = PositionRiskKey{.user_id = 42, .market_id = 1};
  const auto taker_key = PositionRiskKey{.user_id = 43, .market_id = 1};

  assert(checkpoint.public_sequences.at(1) == 13);
  assert(checkpoint.settled_funding_intervals.contains(settlement_key));
  assert(checkpoint.risk_states.at(maker_key).equity == 90);
  assert(checkpoint.risk_states.at(taker_key).equity == 110);
  assert(checkpoint.processed_input_ids.contains("input_funding_settle_001"));
  assert(checkpoint.processed_input_ids.at("input_funding_settle_001")
             .command_kind == RuntimeCommandKind::FundingSettlementTick);

  auto restored = make_runtime();
  EngineCheckpointManager manager;
  manager.restore_runtime(checkpoint, restored);

  const auto snapshot = restored.snapshot_state();
  assert(snapshot.public_sequences.at(1) == 13);
  assert(snapshot.settled_funding_intervals.contains(settlement_key));
  assert(snapshot.risk_states.at(maker_key).equity == 90);
  assert(snapshot.risk_states.at(taker_key).equity == 110);

  const auto duplicate = restored.process_replay(
      make_record(funding_settlement_tick_json(), 1504));
  assert(duplicate.status == EngineProcessStatus::Duplicate);
  assert(duplicate.empty());
  assert(restored.risk_states().at(maker_key).equity == 90);
  assert(restored.risk_states().at(taker_key).equity == 110);
}

void test_checkpoint_source_position_validation_is_explicit() {
  assert(!validate_checkpoint_source_position(CheckpointSourcePosition{
                                                    .topic = EngineInputTopic,
                                                    .partition = 0,
                                                    .next_offset = 1202,
                                                })
              .has_value());

  const auto empty_topic = validate_checkpoint_source_position(
      CheckpointSourcePosition{.topic = "", .partition = 0, .next_offset = 0});
  assert(empty_topic.has_value());
  assert_contains(*empty_topic, "topic");

  const auto negative_partition = validate_checkpoint_source_position(
      CheckpointSourcePosition{
          .topic = EngineInputTopic, .partition = -1, .next_offset = 0});
  assert(negative_partition.has_value());
  assert_contains(*negative_partition, "partition");

  const auto negative_offset = validate_checkpoint_source_position(
      CheckpointSourcePosition{
          .topic = EngineInputTopic, .partition = 0, .next_offset = -1});
  assert(negative_offset.has_value());
  assert_contains(*negative_offset, "next_offset");
}

void test_create_checkpoint_rejects_invalid_source_position() {
  auto runtime = make_runtime();
  EngineCheckpointManager manager;

  try {
    (void)manager.create_checkpoint(
        runtime,
        CheckpointSourcePosition{
            .topic = EngineInputTopic, .partition = 0, .next_offset = -1},
        "invalid-source-position");
    assert(false);
  } catch (const std::invalid_argument& error) {
    assert_contains(error.what(), "next_offset");
  }
}

void test_in_memory_store_saves_and_loads_latest() {
  InMemoryCheckpointStore store;
  assert(!store.load_latest().has_value());

  store.save(make_checkpoint("checkpoint-1"));
  store.save(make_checkpoint("checkpoint-2"));

  const auto latest = store.load_latest();
  assert(latest.has_value());
  assert(latest->checkpoint_id == "checkpoint-2");
  assert(latest->source_position.next_offset == 1202);
  assert(store.size() == 2);
}

void test_checkpoint_next_offset_is_exclusive_replay_boundary() {
  const auto checkpoint = make_checkpoint();

  auto restored = make_runtime();
  EngineCheckpointManager manager;
  manager.restore_runtime(checkpoint, restored);

  const auto included = restored.process_replay(
      make_record(place_order_json(),
                  checkpoint.source_position.next_offset - 1));
  assert(included.status == EngineProcessStatus::Duplicate);
  assert(included.empty());
  assert(included.duplicate.has_value());
  assert(included.duplicate->original_offset == 1201);
  assert(restored.metadata_store().find(9001) != nullptr);

  const auto replayed = restored.process_replay(
      make_record(cancel_order_json(), checkpoint.source_position.next_offset));
  assert(replayed.status == EngineProcessStatus::Processed);
  assert(replayed.empty());
  assert(restored.metadata_store().empty());
}

void test_restore_runtime_can_continue_and_cancel_resting_order() {
  const auto checkpoint = make_checkpoint();

  auto restored = make_runtime();
  EngineCheckpointManager manager;
  manager.restore_runtime(checkpoint, restored);

  const auto duplicate = restored.process(make_record(place_order_json(), 1201));
  assert(duplicate.status == EngineProcessStatus::Duplicate);
  assert(duplicate.duplicate.has_value());
  assert(duplicate.duplicate->reason == EngineDuplicateReason::InputId);
  assert(duplicate.duplicate->original_offset == 1201);

  const auto cancel = restored.process(make_record(cancel_order_json(), 1202));
  assert(cancel.status == EngineProcessStatus::Processed);

  const auto* reply = find_record(cancel.replies, "CancelAccepted");
  assert(reply != nullptr);
  assert(reply->payload.at("request_id") == "req_cancel_001");
  assert(reply->payload.at("order_id") == "9001");
  assert(reply->payload.at("source_input_offset") == "1202");

  const auto* cancelled = find_record(cancel.events, "OrderCancelled");
  assert(cancelled != nullptr);
  assert(cancelled->payload.at("order_id") == "9001");
  assert(cancelled->payload.at("reservation_id") == "res_place_001");
  assert(cancelled->payload.at("user_id") == "42");
  assert(cancelled->payload.at("engine_sequence") == "3");

  const auto* delta = find_record(cancel.events, "OrderBookDelta");
  assert(delta != nullptr);
  assert(delta->payload.at("quantity") == "0");
  assert(delta->payload.at("engine_sequence") == "4");
  assert(restored.metadata_store().empty());
}

void test_restore_runtime_rejects_invalid_source_position() {
  auto checkpoint = make_checkpoint();
  checkpoint.source_position.next_offset = -1;

  auto restored = make_runtime();
  EngineCheckpointManager manager;

  try {
    manager.restore_runtime(checkpoint, restored);
    assert(false);
  } catch (const std::invalid_argument& error) {
    assert_contains(error.what(), "next_offset");
  }
}

}  // namespace

int main() {
  try {
    test_create_checkpoint_captures_recovery_state();
    test_checkpoint_restore_preserves_mark_price_state();
    test_checkpoint_restore_preserves_funding_rate_state();
    test_checkpoint_restore_preserves_position_and_risk_state();
    test_checkpoint_restore_preserves_settled_funding_and_adjusted_risk();
    test_checkpoint_source_position_validation_is_explicit();
    test_create_checkpoint_rejects_invalid_source_position();
    test_in_memory_store_saves_and_loads_latest();
    test_checkpoint_next_offset_is_exclusive_replay_boundary();
    test_restore_runtime_can_continue_and_cancel_resting_order();
    test_restore_runtime_rejects_invalid_source_position();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
