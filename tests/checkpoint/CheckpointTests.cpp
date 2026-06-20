#include "checkpoint/EngineCheckpointManager.hpp"
#include "checkpoint/InMemoryCheckpointStore.hpp"

#include <cassert>
#include <cstdlib>
#include <iostream>
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

}  // namespace

int main() {
  try {
    test_create_checkpoint_captures_recovery_state();
    test_in_memory_store_saves_and_loads_latest();
    test_restore_runtime_can_continue_and_cancel_resting_order();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
