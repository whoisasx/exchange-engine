#include "checkpoint/EngineCheckpointManager.hpp"
#include "checkpoint/file/FileCheckpointStore.hpp"

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

using namespace cex::checkpoint;
using namespace cex::runtime;

namespace {

struct TempDirectory {
  TempDirectory() {
    const auto base = std::filesystem::temp_directory_path();
    const auto nonce =
        std::chrono::steady_clock::now().time_since_epoch().count();

    for (int attempt = 0; attempt < 100; ++attempt) {
      path = base / ("cex-checkpoint-file-tests-" + std::to_string(nonce) +
                     "-" + std::to_string(attempt));
      std::error_code error;
      if (std::filesystem::create_directory(path, error)) {
        return;
      }
    }

    throw std::runtime_error("failed to create temporary checkpoint directory");
  }

  ~TempDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path, error);
  }

  std::filesystem::path path;
};

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

EngineCheckpoint make_checkpoint(std::string checkpoint_id) {
  auto runtime = make_runtime();
  const auto result = runtime.process(make_record(place_order_json(), 1201));
  assert(result.status == EngineProcessStatus::Processed);

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

EngineCheckpoint make_mark_checkpoint(std::string checkpoint_id) {
  auto runtime = make_runtime();
  const auto result = runtime.process(make_record(mark_price_json(), 1301));
  assert(result.status == EngineProcessStatus::Processed);

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

EngineCheckpoint make_funding_checkpoint(std::string checkpoint_id) {
  auto runtime = make_runtime();
  const auto result = runtime.process(make_record(funding_rate_json(), 1311));
  assert(result.status == EngineProcessStatus::Processed);

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

void test_file_store_returns_nullopt_when_empty() {
  TempDirectory temp;
  FileCheckpointStore store(temp.path);

  assert(!store.load_latest().has_value());
}

void test_file_store_returns_nullopt_for_corrupted_latest_file() {
  TempDirectory temp;
  {
    std::ofstream out(temp.path / "broken.checkpoint");
    out << "cex.engine.checkpoint.file.v1\n";
    out << "schema_version\t1\n";
  }

  FileCheckpointStore store(temp.path);
  assert(!store.load_latest().has_value());
}

void test_file_store_persists_and_restores_resting_order_cancel() {
  TempDirectory temp;
  FileCheckpointStore store(temp.path);

  store.save(make_checkpoint("checkpoint-0001"));

  const auto loaded = store.load_latest();
  assert(loaded.has_value());
  assert(loaded->checkpoint_id == "checkpoint-0001");
  assert(loaded->source_position.topic == EngineInputTopic);
  assert(loaded->source_position.partition == 0);
  assert(loaded->source_position.next_offset == 1202);
  assert(loaded->public_sequences.at(1) == 3);
  assert(loaded->metadata_store.find(9001) != nullptr);
  assert(loaded->processed_input_ids.contains("input_place_001"));
  assert(loaded->processed_idempotency_keys.contains("client-order-001"));

  auto restored = make_runtime();
  EngineCheckpointManager manager;
  manager.restore_runtime(*loaded, restored);

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
  assert(cancelled->payload.at("engine_sequence") == "3");

  const auto* delta = find_record(cancel.events, "OrderBookDelta");
  assert(delta != nullptr);
  assert(delta->payload.at("quantity") == "0");
  assert(delta->payload.at("engine_sequence") == "4");
  assert(restored.metadata_store().empty());
}

void test_file_store_persists_and_restores_mark_state() {
  TempDirectory temp;
  FileCheckpointStore store(temp.path);

  store.save(make_mark_checkpoint("checkpoint-0002"));

  const auto loaded = store.load_latest();
  assert(loaded.has_value());
  assert(loaded->checkpoint_id == "checkpoint-0002");
  assert(loaded->source_position.next_offset == 1302);
  assert(loaded->public_sequences.at(1) == 2);
  assert(loaded->mark_prices.contains(1));
  assert_mark_price_state(loaded->mark_prices.at(1));
  assert(loaded->processed_input_ids.contains("input_mark_001"));
  assert(loaded->processed_input_ids.at("input_mark_001").command_kind ==
         RuntimeCommandKind::MarkPriceUpdated);
  assert(loaded->processed_idempotency_keys.empty());

  auto restored = make_runtime();
  EngineCheckpointManager manager;
  manager.restore_runtime(*loaded, restored);

  const auto snapshot = restored.snapshot_state();
  assert(snapshot.public_sequences.at(1) == 2);
  assert(snapshot.mark_prices.contains(1));
  assert_mark_price_state(snapshot.mark_prices.at(1));

  const auto duplicate = restored.process_replay(make_record(mark_price_json(), 1301));
  assert(duplicate.status == EngineProcessStatus::Duplicate);
  assert(duplicate.empty());
  assert(restored.market_sequences().peek(1) == 2);
}

void test_file_store_persists_and_restores_funding_rate_state() {
  TempDirectory temp;
  FileCheckpointStore store(temp.path);

  store.save(make_funding_checkpoint("checkpoint-0003"));

  const auto loaded = store.load_latest();
  assert(loaded.has_value());
  assert(loaded->checkpoint_id == "checkpoint-0003");
  assert(loaded->source_position.next_offset == 1312);
  assert(loaded->public_sequences.at(1) == 2);
  assert(loaded->funding_rates.contains(1));
  assert_funding_rate_state(loaded->funding_rates.at(1));
  assert(loaded->processed_input_ids.contains("input_funding_rate_001"));
  assert(loaded->processed_input_ids.at("input_funding_rate_001")
             .command_kind == RuntimeCommandKind::FundingRateUpdated);
  assert(loaded->processed_idempotency_keys.empty());

  auto restored = make_runtime();
  EngineCheckpointManager manager;
  manager.restore_runtime(*loaded, restored);

  const auto snapshot = restored.snapshot_state();
  assert(snapshot.public_sequences.at(1) == 2);
  assert(snapshot.funding_rates.contains(1));
  assert_funding_rate_state(snapshot.funding_rates.at(1));

  const auto duplicate =
      restored.process_replay(make_record(funding_rate_json(), 1311));
  assert(duplicate.status == EngineProcessStatus::Duplicate);
  assert(duplicate.empty());
  assert(restored.market_sequences().peek(1) == 2);
}

}  // namespace

int main() {
  try {
    test_file_store_returns_nullopt_when_empty();
    test_file_store_returns_nullopt_for_corrupted_latest_file();
    test_file_store_persists_and_restores_resting_order_cancel();
    test_file_store_persists_and_restores_mark_state();
    test_file_store_persists_and_restores_funding_rate_state();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
