#include "runtime/EngineRuntime.hpp"

#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#ifndef PROTOCOL_EXAMPLES_DIR
#error "PROTOCOL_EXAMPLES_DIR must be defined"
#endif

using namespace cex::runtime;

namespace {

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

void test_parses_place_order_fixture() {
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");

  EngineInputParser parser;
  const auto parsed = parser.parse(raw);
  assert(parsed.kind == ParsedEngineInputKind::PlaceOrder);

  const auto& input = std::get<cex::adapter::PlaceOrderInput>(parsed.value);
  assert(input.input_id == "input_place_001");
  assert(input.envelope.request_id == "req_place_001");
  assert(input.envelope.idempotency_key == "client-order-001");
  assert(input.envelope.user_id == 42);
  assert(input.order_id == 9001);
  assert(input.reservation_id == "res_place_001");
  assert(input.market_id == 1);
  assert(input.market_name == "SOL-PERP");
  assert(input.side == cex::adapter::AdapterSide::Long);
  assert(input.order_type == cex::adapter::AdapterOrderType::Limit);
  assert(input.price == 100);
  assert(input.quantity == 10);
  assert(!input.reduce_only);
  assert(input.margin_asset == "USDC");
  assert(input.reserved_margin_amount == 100);
  assert(input.leverage == 10);
}

void test_runtime_resting_limit_order() {
  auto runtime = make_runtime();
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");

  const auto result = runtime.process(make_record(raw, 1201));
  assert(result.replies.size() == 1);
  assert(result.events.size() == 2);

  const auto& reply = result.replies[0];
  assert(reply.topic == EngineRepliesTopic);
  assert(reply.type == "OrderAccepted");
  assert(reply.partition == 0);
  assert(reply.payload.at("request_id") == "req_place_001");
  assert(reply.payload.at("source_input_id") == "input_place_001");
  assert(reply.payload.at("source_input_offset") == "1201");
  assert(reply.payload.at("order_id") == "9001");
  assert(reply.payload.at("reservation_id") == "res_place_001");

  const auto* opened = find_record(result.events, "OrderOpened");
  assert(opened != nullptr);
  assert(opened->payload.at("engine_sequence") == "1");
  assert(opened->payload.at("engine_timestamp_ms") == "1710000000000");
  assert(opened->payload.at("source_input_offset") == "1201");
  assert(opened->payload.at("order_id") == "9001");
  assert(opened->payload.at("market_id") == "1");

  const auto* delta = find_record(result.events, "OrderBookDelta");
  assert(delta != nullptr);
  assert(delta->payload.at("engine_sequence") == "2");
  assert(delta->payload.at("market_id") == "1");
  assert(delta->payload.at("side") == "LONG");
  assert(delta->payload.at("price") == "100");
  assert(delta->payload.at("quantity") == "10");
  assert(runtime.metadata_store().find(9001) != nullptr);
}

void test_runtime_crossing_order_emits_trade() {
  auto runtime = make_runtime();
  const auto resting = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  (void)runtime.process(make_record(resting, 1201));

  const auto result = runtime.process(make_record(crossing_order_json(), 1202));
  assert(result.replies.size() == 1);
  assert(result.replies[0].type == "OrderAccepted");
  assert(result.replies[0].payload.at("request_id") == "req_place_002");
  assert(find_record(result.events, "OrderOpened") == nullptr);

  const auto* trade = find_record(result.events, "TradeExecuted");
  assert(trade != nullptr);
  assert(trade->payload.at("engine_sequence") == "3");
  assert(trade->payload.at("market_id") == "1");
  assert(trade->payload.at("price") == "100");
  assert(trade->payload.at("quantity") == "10");
  assert(trade->payload.at("maker_order_id") == "9001");
  assert(trade->payload.at("taker_order_id") == "9002");
  assert(trade->payload.at("maker_reservation_id") == "res_place_001");
  assert(trade->payload.at("taker_reservation_id") == "res_taker_001");

  const auto* delta = find_record(result.events, "OrderBookDelta");
  assert(delta != nullptr);
  assert(delta->payload.at("engine_sequence") == "4");
  assert(delta->payload.at("quantity") == "0");
  assert(runtime.metadata_store().empty());
}

void test_runtime_cancel_order() {
  auto runtime = make_runtime();
  const auto place = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  const auto cancel = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-cancel-order.command.json");

  (void)runtime.process(make_record(place, 1201));
  const auto result = runtime.process(make_record(cancel, 1210));

  const auto* reply = find_record(result.replies, "CancelAccepted");
  assert(reply != nullptr);
  assert(reply->payload.at("request_id") == "req_cancel_001");
  assert(reply->payload.at("source_input_id") == "input_cancel_001");
  assert(reply->payload.at("source_input_offset") == "1210");
  assert(reply->payload.at("order_id") == "9001");

  const auto* cancelled = find_record(result.events, "OrderCancelled");
  assert(cancelled != nullptr);
  assert(cancelled->payload.at("order_id") == "9001");
  assert(cancelled->payload.at("reservation_id") == "res_place_001");
  assert(cancelled->payload.at("market_id") == "1");
  assert(runtime.metadata_store().empty());
}

}  // namespace

int main() {
  try {
    test_parses_place_order_fixture();
    test_runtime_resting_limit_order();
    test_runtime_crossing_order_emits_trade();
    test_runtime_cancel_order();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
