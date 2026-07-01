#include "runtime/EngineRuntime.hpp"
#include "runtime/EngineOutbox.hpp"
#include "runtime/EngineOutputSerializer.hpp"
#include "protocol/Message.hpp"

#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifndef PROTOCOL_EXAMPLES_DIR
#error "PROTOCOL_EXAMPLES_DIR must be defined"
#endif

using namespace cex::runtime;

namespace {

class CapturingPublisher final : public IEnginePublisher {
 public:
  struct PublishedRecord {
    EngineOutputRecord record;
    std::string serialized_json;
  };

  std::optional<std::string> publish(
      const EngineOutputRecord& record,
      std::string_view serialized_json) override {
    records.push_back(PublishedRecord{
        .record = record,
        .serialized_json = std::string(serialized_json),
    });

    if (next_error.has_value()) {
      std::optional<std::string> error = std::move(next_error);
      next_error.reset();
      return error;
    }
    return std::nullopt;
  }

  std::vector<PublishedRecord> records;
  std::optional<std::string> next_error;
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

std::size_t count_records(const std::vector<EngineOutputRecord>& records,
                          const std::string& type) {
  std::size_t count = 0;
  for (const auto& record : records) {
    if (record.type == type) {
      ++count;
    }
  }
  return count;
}

std::string payload_number_text(const EngineOutputRecord& record,
                                const std::string& field) {
  const auto* value = record.payload.at(field).as_number();
  if (value != nullptr) {
    return value->text;
  }
  const auto* text = record.payload.at(field).as_string();
  assert(text != nullptr);
  return *text;
}

void assert_book_has_order(const EngineRuntime& runtime, OrderId order_id) {
  const auto* book = runtime.core().get_order_book(1);
  assert(book != nullptr);
  assert(book->has_order(order_id));
}

void assert_book_does_not_have_order(const EngineRuntime& runtime,
                                     OrderId order_id) {
  const auto* book = runtime.core().get_order_book(1);
  assert(book != nullptr);
  assert(!book->has_order(order_id));
}

protocol::ProtocolMessage parse_serialized_output_json(
    const EngineOutputRecord& record,
    const std::string& serialized) {
  const protocol::JsonValue root = protocol::parse_json(serialized);
  const auto* root_object = root.as_object();
  assert(root_object != nullptr);
  assert(root_object->size() == 2);

  const auto* type = root.find("type");
  assert(type != nullptr);
  assert(type->as_string() != nullptr);
  assert(*type->as_string() == record.type);

  const auto* payload = root.find("payload");
  assert(payload != nullptr);
  assert(payload->as_object() != nullptr);

  protocol::ProtocolMessage message =
      protocol::parse_protocol_message(serialized);
  assert(message.type == record.type);
  assert(message.payload.is_object());
  return message;
}

protocol::ProtocolMessage parse_serialized_output(
    const EngineOutputRecord& record) {
  return parse_serialized_output_json(record,
                                      serialize_engine_output_record(record));
}

const protocol::JsonValue& payload_field(
    const protocol::ProtocolMessage& message,
    const std::string& field) {
  const auto* value = message.payload.find(field);
  assert(value != nullptr);
  return *value;
}

void assert_payload_string(const protocol::ProtocolMessage& message,
                           const std::string& field,
                           const std::string& expected) {
  const auto* value = payload_field(message, field).as_string();
  assert(value != nullptr);
  assert(*value == expected);
}

void assert_payload_number(const protocol::ProtocolMessage& message,
                           const std::string& field,
                           const std::string& expected) {
  const auto* value = payload_field(message, field).as_number();
  assert(value != nullptr);
  assert(value->text == expected);
}

const protocol::JsonValue::Array& payload_array(
    const protocol::ProtocolMessage& message,
    const std::string& field) {
  const auto* value = payload_field(message, field).as_array();
  assert(value != nullptr);
  return *value;
}

void assert_json_number_field(const protocol::JsonValue& object,
                              const std::string& field,
                              const std::string& expected) {
  const auto* value = object.find(field);
  assert(value != nullptr);
  const auto* number = value->as_number();
  assert(number != nullptr);
  assert(number->text == expected);
}

void assert_json_string_field(const protocol::JsonValue& object,
                              const std::string& field,
                              const std::string& expected) {
  const auto* value = object.find(field);
  assert(value != nullptr);
  const auto* text = value->as_string();
  assert(text != nullptr);
  assert(*text == expected);
}

void assert_trade_settlement(const protocol::JsonValue& value,
                             const std::string& expected_reservation_id,
                             const std::string& expected_asset,
                             const std::string& expected_amount) {
  assert(value.as_object() != nullptr);
  assert_json_string_field(value, "reservation_id", expected_reservation_id);
  assert_json_string_field(value, "debit_asset", expected_asset);
  assert_json_number_field(value, "debit_amount", expected_amount);
  assert_json_string_field(value, "credit_asset", expected_asset);
  assert_json_number_field(value, "credit_amount", expected_amount);
}

void assert_price_level_delta(const protocol::JsonValue& value,
                              const std::string& expected_price,
                              const std::string& expected_quantity) {
  assert(value.as_object() != nullptr);
  assert_json_number_field(value, "price", expected_price);
  assert_json_number_field(value, "quantity", expected_quantity);
}

void assert_duplicate_result(const EngineProcessResult& result,
                             EngineDuplicateReason reason,
                             const std::string& key,
                             std::int64_t original_offset,
                             const std::string& original_input_id) {
  assert(result.status == EngineProcessStatus::Duplicate);
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

void assert_mark_price_fixture_state(const MarkPriceState& state) {
  assert(state.market_id == 1);
  assert(state.mark_price == 100);
  assert(state.index_price == 99);
  assert(state.source_timestamp_ms == 1'710'000'000'000LL);
  assert(state.published_at_ms == 1'710'000'000'100LL);
  assert(state.valid_until_ms == 1'710'000'005'100LL);
  assert(state.source_sequence == 45'001);
  assert(state.source_status == "VALID");
}

void assert_funding_rate_fixture_state(const FundingRateState& state) {
  assert(state.market_id == 1);
  assert(state.funding_interval_id ==
         "funding_SOL-PERP_1710000000_1710028800");
  assert(state.rate == 25);
  assert(state.rate_scale == 1'000'000);
  assert(state.interval_start_ms == 1'710'000'000'000LL);
  assert(state.interval_end_ms == 1'710'028'800'000LL);
  assert(state.source_timestamp_ms == 1'710'000'001'000LL);
}

void assert_position_state(const EngineRuntime& runtime,
                           std::int64_t user_id,
                           std::int64_t signed_quantity,
                           std::int64_t average_entry_price,
                           std::int64_t isolated_margin,
                           std::int32_t expected_leverage = 10) {
  const auto it = runtime.positions().find(PositionRiskKey{
      .user_id = user_id,
      .market_id = 1,
  });
  assert(it != runtime.positions().end());
  assert(it->second.user_id == user_id);
  assert(it->second.market_id == 1);
  assert(it->second.signed_quantity == signed_quantity);
  assert(it->second.average_entry_price == average_entry_price);
  assert(it->second.margin_asset == "USDC");
  assert(it->second.isolated_margin == isolated_margin);
  assert(it->second.leverage == expected_leverage);
}

void assert_risk_state(const EngineRuntime& runtime,
                       std::int64_t user_id,
                       std::int64_t signed_quantity,
                       std::int64_t mark_price,
                       std::int64_t unrealized_pnl) {
  const auto it = runtime.risk_states().find(PositionRiskKey{
      .user_id = user_id,
      .market_id = 1,
  });
  assert(it != runtime.risk_states().end());
  assert(it->second.user_id == user_id);
  assert(it->second.market_id == 1);
  assert(it->second.signed_quantity == signed_quantity);
  assert(it->second.average_entry_price == 100);
  assert(it->second.mark_price == mark_price);
  assert(it->second.unrealized_pnl == unrealized_pnl);
  const auto abs_position =
      signed_quantity < 0 ? -signed_quantity : signed_quantity;
  const auto maintenance_margin = (abs_position * mark_price) / 20;
  const auto equity = 100 + unrealized_pnl;
  assert(it->second.equity == equity);
  assert(it->second.maintenance_margin == maintenance_margin);
  assert(it->second.margin_ratio ==
         (maintenance_margin == 0 ? 0 : (equity * 10'000) / maintenance_margin));
  const std::string expected_status =
      signed_quantity == 0
          ? "FLAT"
          : (equity <= maintenance_margin ? "LIQUIDATABLE" : "HEALTHY");
  assert(it->second.status == expected_status);
  assert(it->second.margin_asset == "USDC");
  assert(it->second.isolated_margin == 100);
  assert(it->second.leverage == 10);
}

std::int64_t abs_test_quantity(std::int64_t quantity) {
  return quantity < 0 ? -quantity : quantity;
}

void set_snapshot_position(EngineRuntimeStateSnapshot& snapshot,
                           std::int64_t user_id,
                           std::int64_t signed_quantity,
                           std::int64_t average_entry_price,
                           std::int64_t isolated_margin,
                           std::int64_t mark_price = 110,
                           std::int32_t leverage = 10) {
  const PositionRiskKey key{
      .user_id = user_id,
      .market_id = 1,
  };
  snapshot.positions[key] = IsolatedPositionState{
      .user_id = user_id,
      .market_id = 1,
      .signed_quantity = signed_quantity,
      .average_entry_price = average_entry_price,
      .margin_asset = "USDC",
      .isolated_margin = isolated_margin,
      .leverage = leverage,
      .updated_at_ms = 1'710'000'000'000LL,
  };

  const auto unrealized_pnl =
      (mark_price - average_entry_price) * signed_quantity;
  const auto maintenance_margin =
      (abs_test_quantity(signed_quantity) * mark_price) / 20;
  const auto equity = isolated_margin + unrealized_pnl;
  const auto margin_ratio =
      maintenance_margin == 0 ? 0 : (equity * 10'000) / maintenance_margin;
  const std::string status =
      signed_quantity == 0
          ? "FLAT"
          : (equity <= maintenance_margin ? "LIQUIDATABLE" : "HEALTHY");
  snapshot.risk_states[key] = IsolatedRiskState{
      .user_id = user_id,
      .market_id = 1,
      .status = status,
      .margin_asset = "USDC",
      .signed_quantity = signed_quantity,
      .average_entry_price = average_entry_price,
      .mark_price = mark_price,
      .isolated_margin = isolated_margin,
      .unrealized_pnl = unrealized_pnl,
      .equity = equity,
      .maintenance_margin = maintenance_margin,
      .margin_ratio = margin_ratio,
      .leverage = leverage,
      .updated_at_ms = 1'710'000'000'000LL,
  };
}

std::vector<std::int64_t> adl_counterparty_order(
    const EngineProcessResult& result) {
  std::vector<std::int64_t> users;
  for (const auto& event : result.events) {
    if (event.type == "AdlExecuted") {
      users.push_back(
          std::stoll(payload_number_text(event, "deleveraged_user_id")));
    }
  }
  return users;
}

std::string place_order_json(const std::string& input_id,
                             const std::string& request_id,
                             const std::string& idempotency_key,
                             std::int64_t order_id,
                             const std::string& reservation_id) {
  std::ostringstream json;
  json << R"json({
  "type": "PlaceOrder",
  "payload": {
    "input_id": ")json"
       << input_id << R"json(",
    "envelope": {
      "request_id": ")json"
       << request_id << R"json(",
      "idempotency_key": ")json"
       << idempotency_key << R"json(",
      "user_id": 42,
      "reply_partition": 0
    },
    "reservation_id": ")json"
       << reservation_id << R"json(",
    "order_id": )json"
       << order_id << R"json(,
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
  return json.str();
}

std::string custom_place_order_json(const std::string& input_id,
                                    const std::string& request_id,
                                    const std::string& idempotency_key,
                                    std::int64_t order_id,
                                    const std::string& reservation_id,
                                    std::int64_t user_id,
                                    const std::string& side,
                                    std::int64_t quantity,
                                    std::int64_t price,
                                    bool reduce_only,
                                    std::int64_t reserved_margin_amount = 100) {
  std::ostringstream json;
  json << R"json({
  "type": "PlaceOrder",
  "payload": {
    "input_id": ")json"
       << input_id << R"json(",
    "envelope": {
      "request_id": ")json"
       << request_id << R"json(",
      "idempotency_key": ")json"
       << idempotency_key << R"json(",
      "user_id": )json"
       << user_id << R"json(,
      "reply_partition": 0
    },
    "reservation_id": ")json"
       << reservation_id << R"json(",
    "order_id": )json"
       << order_id << R"json(,
    "market_id": 1,
    "market_name": "SOL-PERP",
    "side": ")json"
       << side << R"json(",
    "order_type": "LIMIT",
    "quantity": )json"
       << quantity << R"json(,
    "price": )json"
       << price << R"json(,
    "reduce_only": )json"
       << (reduce_only ? "true" : "false") << R"json(,
    "margin_asset": "USDC",
    "reserved_margin_amount": )json"
       << reserved_margin_amount << R"json(,
    "leverage": 10
  }
})json";
  return json.str();
}

std::string cancel_order_json(const std::string& input_id,
                              const std::string& request_id,
                              const std::string& idempotency_key,
                              std::int64_t order_id) {
  std::ostringstream json;
  json << R"json({
  "type": "CancelOrder",
  "payload": {
    "input_id": ")json"
       << input_id << R"json(",
    "envelope": {
      "request_id": ")json"
       << request_id << R"json(",
      "idempotency_key": ")json"
       << idempotency_key << R"json(",
      "user_id": 42,
      "reply_partition": 0
    },
    "market_id": 1,
    "order_id": )json"
       << order_id << R"json(
  }
})json";
  return json.str();
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

std::string liquidation_ask_liquidity_json(
    const std::string& input_id,
    const std::string& request_id,
    const std::string& idempotency_key,
    std::int64_t order_id,
    const std::string& reservation_id,
    std::int64_t quantity = 10,
    std::int64_t price = 95) {
  std::ostringstream json;
  json << R"json({
  "type": "PlaceOrder",
  "payload": {
    "input_id": ")json"
       << input_id << R"json(",
    "envelope": {
      "request_id": ")json"
       << request_id << R"json(",
      "idempotency_key": ")json"
       << idempotency_key << R"json(",
      "user_id": 44,
      "reply_partition": 0
    },
    "reservation_id": ")json"
       << reservation_id << R"json(",
    "order_id": )json"
       << order_id << R"json(,
    "market_id": 1,
    "market_name": "SOL-PERP",
    "side": "SHORT",
    "order_type": "LIMIT",
    "quantity": )json"
       << quantity << R"json(,
    "price": )json"
       << price << R"json(,
    "reduce_only": false,
    "margin_asset": "USDC",
    "reserved_margin_amount": 100,
    "leverage": 10
  }
})json";
  return json.str();
}

std::string funding_rate_settlement_json(
    const std::string& input_id = "input_funding_rate_settle_001",
    const std::string& interval_id =
        "funding_SOL-PERP_1710000000_1710028800",
    std::int64_t rate = 10'000,
    std::int64_t rate_scale = 1'000'000) {
  std::ostringstream json;
  json << R"json({
  "type": "FundingRateUpdated",
  "payload": {
    "input_id": ")json"
       << input_id << R"json(",
    "market_id": 1,
    "funding_interval_id": ")json"
       << interval_id << R"json(",
    "rate": )json"
       << rate << R"json(,
    "rate_scale": )json"
       << rate_scale << R"json(,
    "interval_start_ms": 1710000000000,
    "interval_end_ms": 1710028800000,
    "source_timestamp_ms": 1710000001000
  }
})json";
  return json.str();
}

std::string funding_settlement_tick_json(
    const std::string& input_id = "input_funding_settle_001",
    const std::string& interval_id =
        "funding_SOL-PERP_1710000000_1710028800") {
  std::ostringstream json;
  json << R"json({
  "type": "FundingSettlementTick",
  "payload": {
    "input_id": ")json"
       << input_id << R"json(",
    "market_id": 1,
    "funding_interval_id": ")json"
       << interval_id << R"json(",
    "settle_at_ms": 1710028800000
  }
})json";
  return json.str();
}

std::string mark_price_json(const std::string& input_id,
                            std::int64_t mark_price,
                            std::int64_t index_price,
                            std::int64_t source_sequence = 45'050) {
  std::ostringstream json;
  json << R"json({
  "type": "MarkPriceUpdated",
  "payload": {
    "input_id": ")json"
       << input_id << R"json(",
    "market_id": 1,
    "mark_price": )json"
       << mark_price << R"json(,
    "index_price": )json"
       << index_price << R"json(,
    "source_timestamp_ms": 1710000000000,
    "published_at_ms": 1710000000100,
    "valid_until_ms": 1710000005100,
    "source_sequence": )json"
       << source_sequence << R"json(,
    "source_status": "VALID"
  }
})json";
  return json.str();
}

std::string liquidation_json(
    const std::string& input_id = "input_liq_001",
    const std::string& request_id = "req_liq_001",
    const std::string& idempotency_key = "liquidate-001",
    const std::string& liquidation_id = "liq_001",
    std::int64_t liquidated_user_id = 43,
    const std::string& position_side = "SHORT",
    std::int64_t quantity = 10,
    std::int64_t price = 95) {
  std::ostringstream json;
  json << R"json({
  "type": "LiquidatePosition",
  "payload": {
    "input_id": ")json"
       << input_id << R"json(",
    "envelope": {
      "request_id": ")json"
       << request_id << R"json(",
      "idempotency_key": ")json"
       << idempotency_key << R"json(",
      "user_id": 0,
      "reply_partition": 0
    },
    "liquidation_id": ")json"
       << liquidation_id << R"json(",
    "market_id": 1,
    "market_name": "SOL-PERP",
    "liquidated_user_id": )json"
       << liquidated_user_id << R"json(,
    "position_side": ")json"
       << position_side << R"json(",
    "quantity": )json"
       << quantity << R"json(,
    "price": )json"
       << price << R"json(,
    "request_source": "KEEPER_HINT"
  }
})json";
  return json.str();
}

void open_opposite_positions(EngineRuntime& runtime) {
  const auto resting = runtime.process(make_record(
      place_order_json("input_funding_maker_001",
                       "req_funding_maker_001",
                       "idem-funding-maker-001",
                       9101,
                       "res_funding_maker_001"),
      1701));
  assert(resting.status == EngineProcessStatus::Processed);
  const auto crossing = runtime.process(make_record(crossing_order_json(), 1702));
  assert(crossing.status == EngineProcessStatus::Processed);
  assert_position_state(runtime, 42, 10, 100, 100);
  assert_position_state(runtime, 43, -10, 100, 100);
}

void open_liquidatable_short_position(EngineRuntime& runtime) {
  open_opposite_positions(runtime);
  auto snapshot = runtime.snapshot_state();
  snapshot.mark_prices[1] = MarkPriceState{
      .market_id = 1,
      .mark_price = 110,
      .index_price = 109,
      .source_timestamp_ms = 1'710'000'000'000LL,
      .published_at_ms = 1'710'000'000'100LL,
      .valid_until_ms = 1'710'000'005'100LL,
      .source_sequence = 45'011,
      .source_status = "VALID",
  };
  set_snapshot_position(snapshot, 42, 10, 100, 100, 110);
  set_snapshot_position(snapshot, 43, -10, 100, 100, 110);
  snapshot.public_sequences[1] = 10;
  runtime.restore_state(snapshot);
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 43, .market_id = 1})
             .equity == 0);
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

void test_parses_liquidate_position_fixture() {
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-liquidate-position.command.json");

  EngineInputParser parser;
  const auto parsed = parser.parse(raw);
  assert(parsed.kind == ParsedEngineInputKind::LiquidatePosition);

  const auto& input =
      std::get<cex::adapter::LiquidatePositionInput>(parsed.value);
  assert(input.input_id == "input_liq_001");
  assert(input.envelope.request_id == "req_liq_001");
  assert(input.envelope.idempotency_key == "liquidate-001");
  assert(input.envelope.user_id == 0);
  assert(input.liquidation_id == "liq_001");
  assert(input.market_id == 1);
  assert(input.market_name == "SOL-PERP");
  assert(input.liquidated_user_id == 42);
  assert(input.position_side == cex::adapter::AdapterSide::Long);
  assert(input.quantity == 10);
  assert(input.price == 0);
  assert(input.request_source == "KEEPER_HINT");
}

void test_parses_mark_price_updated_fixture() {
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-mark-price-updated.input.json");

  EngineInputParser parser;
  const auto parsed = parser.parse(raw);
  assert(parsed.kind == ParsedEngineInputKind::MarkPriceUpdated);

  const auto& input =
      std::get<cex::adapter::MarkPriceUpdatedInput>(parsed.value);
  assert(input.input_id == "input_mark_001");
  assert(input.market_id == 1);
  assert(input.mark_price == 100);
  assert(input.index_price == 99);
  assert(input.source_timestamp_ms == 1'710'000'000'000LL);
  assert(input.published_at_ms == 1'710'000'000'100LL);
  assert(input.valid_until_ms == 1'710'000'005'100LL);
  assert(input.source_sequence == 45'001);
  assert(input.source_status == "VALID");
}

void test_parses_funding_rate_updated_fixture() {
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-funding-rate-updated.input.json");

  EngineInputParser parser;
  const auto parsed = parser.parse(raw);
  assert(parsed.kind == ParsedEngineInputKind::FundingRateUpdated);

  const auto& input =
      std::get<cex::adapter::FundingRateUpdatedInput>(parsed.value);
  assert(input.input_id == "input_funding_rate_001");
  assert(input.market_id == 1);
  assert(input.funding_interval_id ==
         "funding_SOL-PERP_1710000000_1710028800");
  assert(input.rate == 25);
  assert(input.rate_scale == 1'000'000);
  assert(input.interval_start_ms == 1'710'000'000'000LL);
  assert(input.interval_end_ms == 1'710'028'800'000LL);
  assert(input.source_timestamp_ms == 1'710'000'001'000LL);
}

void test_parses_funding_settlement_tick_fixture() {
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-funding-settlement-tick.input.json");

  EngineInputParser parser;
  const auto parsed = parser.parse(raw);
  assert(parsed.kind == ParsedEngineInputKind::FundingSettlementTick);

  const auto& input =
      std::get<cex::adapter::FundingSettlementTickInput>(parsed.value);
  assert(input.input_id == "input_funding_settle_001");
  assert(input.market_id == 1);
  assert(input.funding_interval_id ==
         "funding_SOL-PERP_1710000000_1710028800");
  assert(input.settle_at_ms == 1'710'028'800'000LL);
}

void test_liquidate_position_validation() {
  EngineInputParser parser;

  try {
    (void)parser.parse(R"json({
  "type": "LiquidatePosition",
  "payload": {
    "input_id": "input_liq_bad_side",
    "envelope": {
      "request_id": "req_liq_bad_side",
      "idempotency_key": "liquidate-bad-side",
      "user_id": 0,
      "reply_partition": 0
    },
    "liquidation_id": "liq_bad_side",
    "market_id": 1,
    "market_name": "SOL-PERP",
    "liquidated_user_id": 42,
    "position_side": "FLAT",
    "quantity": 10,
    "price": 95
  }
})json");
    assert(false);
  } catch (const EngineInputParserError& error) {
    assert(std::string(error.what()).find("position_side") !=
           std::string::npos);
  }
}

void test_funding_rate_updated_validation() {
  EngineInputParser parser;

  try {
    (void)parser.parse(R"json({
  "type": "FundingRateUpdated",
  "payload": {
    "input_id": "input_funding_rate_bad_scale",
    "market_id": 1,
    "funding_interval_id": "funding_bad",
    "rate": 25,
    "rate_scale": 0,
    "interval_start_ms": 1710000000000,
    "interval_end_ms": 1710028800000,
    "source_timestamp_ms": 1710000001000
  }
})json");
    assert(false);
  } catch (const EngineInputParserError& error) {
    assert(std::string(error.what()).find("rate_scale") != std::string::npos);
  }

  try {
    (void)parser.parse(R"json({
  "type": "FundingRateUpdated",
  "payload": {
    "input_id": "input_funding_rate_bad_interval",
    "market_id": 1,
    "funding_interval_id": "funding_bad",
    "rate": 25,
    "rate_scale": 1000000,
    "interval_start_ms": 1710028800000,
    "interval_end_ms": 1710028800000,
    "source_timestamp_ms": 1710000001000
  }
})json");
    assert(false);
  } catch (const EngineInputParserError& error) {
    assert(std::string(error.what()).find("interval_end_ms") !=
           std::string::npos);
  }
}

void test_funding_settlement_tick_validation() {
  EngineInputParser parser;

  try {
    (void)parser.parse(R"json({
  "type": "FundingSettlementTick",
  "payload": {
    "input_id": "input_funding_settle_bad",
    "market_id": 1,
    "funding_interval_id": "funding_bad"
  }
})json");
    assert(false);
  } catch (const EngineInputParserError& error) {
    assert(std::string(error.what()).find("settle_at_ms") !=
           std::string::npos);
  }
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
  assert(reply.payload.at("source_input_topic") == EngineInputTopic);
  assert(reply.payload.at("source_input_partition").as_number()->text == "0");
  assert(reply.payload.at("source_input_offset") == "1201");
  assert(reply.payload.at("order_id") == "9001");
  assert(reply.payload.at("reservation_id") == "res_place_001");

  const auto accepted_message = parse_serialized_output(reply);
  assert_payload_string(accepted_message, "request_id", "req_place_001");
  assert_payload_string(accepted_message, "source_input_id", "input_place_001");
  assert_payload_number(accepted_message, "source_input_offset", "1201");
  assert_payload_number(accepted_message, "order_id", "9001");
  assert_payload_string(accepted_message, "reservation_id", "res_place_001");

  const auto* opened = find_record(result.events, "OrderOpened");
  assert(opened != nullptr);
  assert(opened->payload.at("engine_sequence") == "1");
  assert(opened->payload.at("engine_timestamp_ms") == "1710000000000");
  assert(opened->payload.at("request_id") == "req_place_001");
  assert(opened->payload.at("source_input_topic") == EngineInputTopic);
  assert(opened->payload.at("source_input_partition").as_number()->text == "0");
  assert(opened->payload.at("source_input_offset") == "1201");
  assert(opened->payload.at("order_id") == "9001");
  assert(opened->payload.at("market_id") == "1");

  const auto opened_message = parse_serialized_output(*opened);
  assert_payload_number(opened_message, "engine_sequence", "1");
  assert_payload_number(opened_message, "engine_timestamp_ms", "1710000000000");
  assert_payload_number(opened_message, "source_input_offset", "1201");
  assert_payload_number(opened_message, "order_id", "9001");
  assert_payload_number(opened_message, "market_id", "1");

  const auto* delta = find_record(result.events, "OrderBookDelta");
  assert(delta != nullptr);
  assert(delta->payload.at("engine_sequence") == "2");
  assert(delta->payload.at("market_id") == "1");
  assert(delta->payload.at("side") == "LONG");
  assert(delta->payload.at("price") == "100");
  assert(delta->payload.at("quantity") == "10");
  assert(runtime.metadata_store().find(9001) != nullptr);

  const auto delta_message = parse_serialized_output(*delta);
  assert_payload_number(delta_message, "engine_sequence", "2");
  assert_payload_number(delta_message, "market_id", "1");
  assert_payload_number(delta_message, "source_input_offset", "1201");
  assert_payload_string(delta_message, "side", "LONG");
  assert_payload_number(delta_message, "price", "100");
  assert_payload_number(delta_message, "quantity", "10");
  const auto& delta_bids = payload_array(delta_message, "bids");
  assert(delta_bids.size() == 1);
  assert_price_level_delta(delta_bids[0], "100", "10");
  const auto& delta_asks = payload_array(delta_message, "asks");
  assert(delta_asks.empty());
}

void test_runtime_crossing_order_emits_trade() {
  auto runtime = make_runtime();
  const auto resting = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  (void)runtime.process(make_record(resting, 1201));

  const auto result = runtime.process(make_record(crossing_order_json(), 1202));
  assert(result.replies.size() == 1);
  assert(result.events.size() == 6);
  assert(result.replies[0].type == "OrderAccepted");
  assert(result.replies[0].payload.at("request_id") == "req_place_002");
  assert(find_record(result.events, "OrderOpened") == nullptr);
  assert(result.events[0].type == "TradeExecuted");
  assert(result.events[1].type == "PositionChanged");
  assert(result.events[2].type == "RiskStateUpdated");
  assert(result.events[3].type == "PositionChanged");
  assert(result.events[4].type == "RiskStateUpdated");
  assert(result.events[5].type == "OrderBookDelta");

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

  const auto trade_message = parse_serialized_output(*trade);
  assert_payload_number(trade_message, "engine_sequence", "3");
  assert_payload_number(trade_message, "market_id", "1");
  assert_payload_number(trade_message, "source_input_offset", "1202");
  assert_payload_number(trade_message, "fill_id", "1");
  assert_payload_number(trade_message, "price", "100");
  assert_payload_number(trade_message, "quantity", "10");
  assert_payload_number(trade_message, "maker_order_id", "9001");
  assert_payload_number(trade_message, "taker_order_id", "9002");
  assert_payload_string(trade_message,
                        "maker_reservation_id",
                        "res_place_001");
  assert_payload_string(trade_message,
                        "taker_reservation_id",
                        "res_taker_001");
  assert(payload_array(trade_message, "fee_deltas").empty());
  const auto& settlements = payload_array(trade_message, "settlements");
  assert(settlements.size() == 2);
  assert_trade_settlement(settlements[0], "res_place_001", "USDC", "100");
  assert_trade_settlement(settlements[1], "res_taker_001", "USDC", "100");

  const auto& maker_position = result.events[1];
  assert(maker_position.payload.at("engine_sequence") == "4");
  assert(maker_position.payload.at("position_id") == "pos_42_1");
  assert(payload_number_text(maker_position, "user_id") == "42");
  assert(maker_position.payload.at("side") == "LONG");
  assert(payload_number_text(maker_position, "signed_quantity") == "10");
  assert(payload_number_text(maker_position, "average_entry_price") == "100");
  assert(payload_number_text(maker_position, "mark_price") == "100");
  assert(payload_number_text(maker_position, "isolated_margin") == "100");
  assert(payload_number_text(maker_position, "realized_pnl") == "0");
  assert(payload_number_text(maker_position, "unrealized_pnl") == "0");
  assert(payload_number_text(maker_position, "maintenance_margin") == "50");
  assert(payload_number_text(maker_position, "liquidation_price") == "0");
  assert(maker_position.payload.at("reason") == "TRADE");
  const auto maker_position_message =
      parse_serialized_output(maker_position);
  assert_payload_string(maker_position_message, "position_id", "pos_42_1");
  assert_payload_number(maker_position_message, "user_id", "42");
  assert_payload_number(maker_position_message, "signed_quantity", "10");
  assert_payload_number(maker_position_message, "average_entry_price", "100");
  assert_payload_number(maker_position_message, "mark_price", "100");
  assert_payload_number(maker_position_message, "isolated_margin", "100");
  assert_payload_number(maker_position_message, "maintenance_margin", "50");
  assert_payload_number(maker_position_message, "liquidation_price", "0");
  assert_payload_string(maker_position_message, "reason", "TRADE");

  const auto& maker_risk = result.events[2];
  assert(maker_risk.payload.at("engine_sequence") == "5");
  assert(maker_risk.payload.at("position_id") == "pos_42_1");
  assert(payload_number_text(maker_risk, "user_id") == "42");
  assert(maker_risk.payload.at("status") == "HEALTHY");
  assert(payload_number_text(maker_risk, "mark_price") == "100");
  assert(payload_number_text(maker_risk, "unrealized_pnl") == "0");
  assert(payload_number_text(maker_risk, "equity") == "100");
  assert(payload_number_text(maker_risk, "maintenance_margin") == "50");
  assert(payload_number_text(maker_risk, "margin_ratio") == "20000");

  const auto& taker_position = result.events[3];
  assert(taker_position.payload.at("engine_sequence") == "6");
  assert(taker_position.payload.at("position_id") == "pos_43_1");
  assert(payload_number_text(taker_position, "user_id") == "43");
  assert(taker_position.payload.at("side") == "SHORT");
  assert(payload_number_text(taker_position, "signed_quantity") == "-10");
  assert(payload_number_text(taker_position, "average_entry_price") == "100");
  assert(payload_number_text(taker_position, "mark_price") == "100");
  assert(payload_number_text(taker_position, "isolated_margin") == "100");
  assert(payload_number_text(taker_position, "realized_pnl") == "0");
  assert(payload_number_text(taker_position, "unrealized_pnl") == "0");
  assert(payload_number_text(taker_position, "maintenance_margin") == "50");
  assert(payload_number_text(taker_position, "liquidation_price") == "0");
  assert(taker_position.payload.at("reason") == "TRADE");

  const auto& taker_risk = result.events[4];
  assert(taker_risk.payload.at("engine_sequence") == "7");
  assert(taker_risk.payload.at("position_id") == "pos_43_1");
  assert(payload_number_text(taker_risk, "user_id") == "43");
  assert(taker_risk.payload.at("status") == "HEALTHY");
  assert(payload_number_text(taker_risk, "mark_price") == "100");
  assert(payload_number_text(taker_risk, "unrealized_pnl") == "0");
  assert(payload_number_text(taker_risk, "equity") == "100");
  assert(payload_number_text(taker_risk, "maintenance_margin") == "50");
  assert(payload_number_text(taker_risk, "margin_ratio") == "20000");

  const auto* delta = find_record(result.events, "OrderBookDelta");
  assert(delta != nullptr);
  assert(delta->payload.at("engine_sequence") == "8");
  assert(delta->payload.at("quantity") == "0");
  const auto delta_message = parse_serialized_output(*delta);
  const auto& delta_bids = payload_array(delta_message, "bids");
  assert(delta_bids.size() == 1);
  assert_price_level_delta(delta_bids[0], "100", "0");
  assert(payload_array(delta_message, "asks").empty());
  assert_position_state(runtime, 42, 10, 100, 100);
  assert_position_state(runtime, 43, -10, 100, 100);
  assert_risk_state(runtime, 42, 10, 100, 0);
  assert_risk_state(runtime, 43, -10, 100, 0);
  const auto snapshot = runtime.snapshot_state();
  assert(snapshot.positions.size() == 2);
  assert(snapshot.risk_states.size() == 2);
  auto restored = make_runtime();
  restored.restore_state(snapshot);
  assert_position_state(restored, 42, 10, 100, 100);
  assert_position_state(restored, 43, -10, 100, 100);
  assert_risk_state(restored, 42, 10, 100, 0);
  assert_risk_state(restored, 43, -10, 100, 0);
  assert(runtime.metadata_store().empty());
}

void test_runtime_uses_configured_first_trade_id() {
  auto runtime = EngineRuntime(EngineRuntimeConfig{
      .symbols = {make_symbol()},
      .first_public_sequence = 1,
      .first_trade_id = 9'000'000'000'001ULL,
      .clock = [] { return 1'710'000'000'000LL; },
  });
  const auto resting = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  (void)runtime.process(make_record(resting, 1201));

  const auto result = runtime.process(make_record(crossing_order_json(), 1202));

  const auto* trade = find_record(result.events, "TradeExecuted");
  assert(trade != nullptr);
  assert(trade->payload.at("fill_id") == "9000000000001");
  const auto trade_message = parse_serialized_output(*trade);
  assert_payload_number(trade_message, "fill_id", "9000000000001");
}

void test_reduce_only_rejects_without_position() {
  auto runtime = make_runtime();
  const auto reduce_only = custom_place_order_json("input_reduce_no_pos",
                                                   "req_reduce_no_pos",
                                                   "idem-reduce-no-pos",
                                                   9201,
                                                   "res_reduce_no_pos",
                                                   42,
                                                   "SHORT",
                                                   5,
                                                   100,
                                                   true,
                                                   50);

  const auto result = runtime.process(make_record(reduce_only, 1211));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.replies[0].type == "OrderRejected");
  assert(result.replies[0].payload.at("reason") == "reduce_only_no_position");
  assert(payload_number_text(result.replies[0], "order_id") == "9201");
  assert(result.events.size() == 1);
  assert(result.events[0].type == "ReservationReleased");
  assert(payload_number_text(result.events[0], "released_amount") == "50");
  assert_book_does_not_have_order(runtime, 9201);
  assert(runtime.metadata_store().empty());
}

void test_reduce_only_rejects_wrong_side() {
  auto runtime = make_runtime();
  open_opposite_positions(runtime);
  const auto reduce_only = custom_place_order_json("input_reduce_wrong_side",
                                                   "req_reduce_wrong_side",
                                                   "idem-reduce-wrong-side",
                                                   9202,
                                                   "res_reduce_wrong_side",
                                                   42,
                                                   "LONG",
                                                   5,
                                                   100,
                                                   true,
                                                   50);

  const auto result = runtime.process(make_record(reduce_only, 1212));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.replies[0].type == "OrderRejected");
  assert(result.replies[0].payload.at("reason") == "reduce_only_wrong_side");
  assert(result.events.size() == 1);
  assert(result.events[0].type == "ReservationReleased");
  assert_book_does_not_have_order(runtime, 9202);
  assert_position_state(runtime, 42, 10, 100, 100);
}

void test_reduce_only_exact_close_does_not_rest() {
  auto runtime = make_runtime();
  open_opposite_positions(runtime);
  const auto bid = custom_place_order_json("input_reduce_exact_bid",
                                           "req_reduce_exact_bid",
                                           "idem-reduce-exact-bid",
                                           9203,
                                           "res_reduce_exact_bid",
                                           44,
                                           "LONG",
                                           10,
                                           100,
                                           false);
  const auto bid_result = runtime.process(make_record(bid, 1213));
  assert(bid_result.status == EngineProcessStatus::Processed);
  assert_book_has_order(runtime, 9203);

  const auto reduce_only = custom_place_order_json("input_reduce_exact",
                                                   "req_reduce_exact",
                                                   "idem-reduce-exact",
                                                   9204,
                                                   "res_reduce_exact",
                                                   42,
                                                   "SHORT",
                                                   10,
                                                   100,
                                                   true,
                                                   100);
  const auto result = runtime.process(make_record(reduce_only, 1214));

  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.replies[0].type == "OrderAccepted");
  assert(find_record(result.events, "OrderOpened") == nullptr);
  assert(find_record(result.events, "OrderExpired") == nullptr);
  const auto* trade = find_record(result.events, "TradeExecuted");
  assert(trade != nullptr);
  assert(payload_number_text(*trade, "quantity") == "10");
  assert_book_does_not_have_order(runtime, 9204);
  assert(runtime.metadata_store().find(9204) == nullptr);
  assert_position_state(runtime, 42, 0, 0, 0);
}

void test_reduce_only_oversized_close_is_capped_and_excess_expires() {
  auto runtime = make_runtime();
  open_opposite_positions(runtime);
  const auto bid = custom_place_order_json("input_reduce_over_bid",
                                           "req_reduce_over_bid",
                                           "idem-reduce-over-bid",
                                           9205,
                                           "res_reduce_over_bid",
                                           44,
                                           "LONG",
                                           10,
                                           100,
                                           false);
  (void)runtime.process(make_record(bid, 1215));
  assert_book_has_order(runtime, 9205);

  const auto reduce_only = custom_place_order_json("input_reduce_over",
                                                   "req_reduce_over",
                                                   "idem-reduce-over",
                                                   9206,
                                                   "res_reduce_over",
                                                   42,
                                                   "SHORT",
                                                   15,
                                                   100,
                                                   true,
                                                   150);
  const auto result = runtime.process(make_record(reduce_only, 1216));

  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.replies[0].type == "OrderAccepted");
  assert(find_record(result.events, "OrderOpened") == nullptr);
  const auto* trade = find_record(result.events, "TradeExecuted");
  assert(trade != nullptr);
  assert(payload_number_text(*trade, "quantity") == "10");
  const auto* expired = find_record(result.events, "OrderExpired");
  assert(expired != nullptr);
  assert(payload_number_text(*expired, "expired_quantity") == "5");
  assert(payload_number_text(*expired, "released_amount") == "50");
  assert(expired->payload.at("reason") == "REDUCE_ONLY_REMAINDER");
  assert_book_does_not_have_order(runtime, 9206);
  assert(runtime.metadata_store().find(9206) == nullptr);
  assert_position_state(runtime, 42, 0, 0, 0);
}

void test_mark_price_affects_fill_risk_state() {
  auto runtime = make_runtime();
  const auto mark = R"json({
  "type": "MarkPriceUpdated",
  "payload": {
    "input_id": "input_mark_before_fill",
    "market_id": 1,
    "mark_price": 104,
    "index_price": 103,
    "source_timestamp_ms": 1710000000000,
    "published_at_ms": 1710000000100,
    "valid_until_ms": 1710000005100,
    "source_sequence": 45010,
    "source_status": "VALID"
  }
})json";
  (void)runtime.process(make_record(mark, 1199));
  const auto resting = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  (void)runtime.process(make_record(resting, 1201));

  const auto result = runtime.process(make_record(crossing_order_json(), 1202));
  assert(result.events.size() == 6);
  assert(result.events[1].type == "PositionChanged");
  assert(payload_number_text(result.events[1], "mark_price") == "104");
  assert(payload_number_text(result.events[1], "unrealized_pnl") == "40");
  assert(payload_number_text(result.events[1], "maintenance_margin") == "52");
  assert(result.events[2].type == "RiskStateUpdated");
  assert(payload_number_text(result.events[2], "mark_price") == "104");
  assert(payload_number_text(result.events[2], "unrealized_pnl") == "40");
  assert(payload_number_text(result.events[2], "equity") == "140");
  assert(payload_number_text(result.events[2], "maintenance_margin") == "52");
  assert(payload_number_text(result.events[2], "margin_ratio") == "26923");
  assert(result.events[3].type == "PositionChanged");
  assert(payload_number_text(result.events[3], "mark_price") == "104");
  assert(payload_number_text(result.events[3], "unrealized_pnl") == "-40");
  assert(payload_number_text(result.events[3], "maintenance_margin") == "52");
  assert(result.events[4].type == "RiskStateUpdated");
  assert(payload_number_text(result.events[4], "mark_price") == "104");
  assert(payload_number_text(result.events[4], "unrealized_pnl") == "-40");
  assert(payload_number_text(result.events[4], "equity") == "60");
  assert(payload_number_text(result.events[4], "maintenance_margin") == "52");
  assert(payload_number_text(result.events[4], "margin_ratio") == "11538");
  assert_risk_state(runtime, 42, 10, 104, 40);
  assert_risk_state(runtime, 43, -10, 104, -40);
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
  assert(payload_number_text(*cancelled, "released_amount") == "100");
  assert(runtime.metadata_store().empty());
}

void test_runtime_mark_price_updated_emits_event_without_reply() {
  auto runtime = make_runtime();
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-mark-price-updated.input.json");

  const auto result = runtime.process(make_record(raw, 1601));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.duplicate == std::nullopt);
  assert(result.replies.empty());
  assert(result.events.size() == 1);

  const auto& event = result.events[0];
  assert(event.topic == EngineEventsTopic);
  assert(event.type == "MarkPriceUpdated");
  assert(event.key == "1");
  assert(event.partition == std::nullopt);
  assert(event.payload.at("engine_event_id") == "eng_1_1");
  assert(event.payload.at("engine_sequence") == "1");
  assert(event.payload.at("engine_timestamp_ms") == "1710000000000");
  assert(event.payload.at("source_input_id") == "input_mark_001");
  assert(event.payload.at("source_input_topic") == EngineInputTopic);
  assert(event.payload.at("source_input_partition").as_number()->text == "0");
  assert(event.payload.at("source_input_offset") == "1601");
  assert(event.payload.at("market_id") == "1");
  assert(event.payload.at("mark_price") == "100");
  assert(event.payload.at("index_price") == "99");
  assert(event.payload.at("valid_until_ms") == "1710000005100");
  assert(event.payload.at("source_sequence") == "45001");
  assert(event.payload.at("source_status") == "VALID");

  const auto message = parse_serialized_output(event);
  assert_payload_number(message, "engine_sequence", "1");
  assert_payload_number(message, "engine_timestamp_ms", "1710000000000");
  assert_payload_number(message, "source_input_offset", "1601");
  assert_payload_number(message, "market_id", "1");
  assert_payload_number(message, "mark_price", "100");
  assert_payload_number(message, "index_price", "99");
  assert_payload_number(message, "valid_until_ms", "1710000005100");
  assert_payload_number(message, "source_sequence", "45001");
  assert_payload_string(message, "source_status", "VALID");

  const auto mark = runtime.mark_prices().find(1);
  assert(mark != runtime.mark_prices().end());
  assert_mark_price_fixture_state(mark->second);
  assert(runtime.market_sequences().peek(1) == 2);

  const auto snapshot = runtime.snapshot_state();
  assert(snapshot.public_sequences.at(1) == 2);
  assert(snapshot.mark_prices.contains(1));
  assert_mark_price_fixture_state(snapshot.mark_prices.at(1));
  assert(snapshot.processed_input_ids.contains("input_mark_001"));
  assert(snapshot.processed_input_ids.at("input_mark_001").command_kind ==
         RuntimeCommandKind::MarkPriceUpdated);
  assert(snapshot.processed_input_ids.at("input_mark_001").idempotency_key.empty());
  assert(snapshot.processed_idempotency_keys.empty());

  const auto duplicate = runtime.process(make_record(raw, 1602));
  assert_duplicate_result(duplicate,
                          EngineDuplicateReason::InputId,
                          "input_mark_001",
                          1601,
                          "input_mark_001");
  assert(runtime.market_sequences().peek(1) == 2);
}

void test_runtime_funding_rate_updated_emits_event_without_reply() {
  auto runtime = make_runtime();
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-funding-rate-updated.input.json");

  const auto result = runtime.process(make_record(raw, 1621));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.duplicate == std::nullopt);
  assert(result.replies.empty());
  assert(result.events.size() == 1);

  const auto& event = result.events[0];
  assert(event.topic == EngineEventsTopic);
  assert(event.type == "FundingRateUpdated");
  assert(event.key == "1");
  assert(event.partition == std::nullopt);
  assert(event.payload.at("engine_event_id") == "eng_1_1");
  assert(event.payload.at("engine_sequence") == "1");
  assert(event.payload.at("engine_timestamp_ms") == "1710000000000");
  assert(event.payload.at("source_input_id") == "input_funding_rate_001");
  assert(event.payload.at("source_input_offset") == "1621");
  assert(event.payload.at("market_id") == "1");
  assert(event.payload.at("funding_interval_id") ==
         "funding_SOL-PERP_1710000000_1710028800");
  assert(event.payload.at("rate") == "25");
  assert(event.payload.at("rate_scale") == "1000000");
  assert(event.payload.at("interval_start_ms") == "1710000000000");
  assert(event.payload.at("interval_end_ms") == "1710028800000");

  const auto message = parse_serialized_output(event);
  assert_payload_number(message, "engine_sequence", "1");
  assert_payload_number(message, "engine_timestamp_ms", "1710000000000");
  assert_payload_number(message, "source_input_offset", "1621");
  assert_payload_number(message, "market_id", "1");
  assert_payload_string(message,
                        "funding_interval_id",
                        "funding_SOL-PERP_1710000000_1710028800");
  assert_payload_number(message, "rate", "25");
  assert_payload_number(message, "rate_scale", "1000000");
  assert_payload_number(message, "interval_start_ms", "1710000000000");
  assert_payload_number(message, "interval_end_ms", "1710028800000");

  const auto funding = runtime.funding_rates().find(1);
  assert(funding != runtime.funding_rates().end());
  assert_funding_rate_fixture_state(funding->second);
  assert(runtime.market_sequences().peek(1) == 2);

  const auto snapshot = runtime.snapshot_state();
  assert(snapshot.public_sequences.at(1) == 2);
  assert(snapshot.funding_rates.contains(1));
  assert_funding_rate_fixture_state(snapshot.funding_rates.at(1));
  assert(snapshot.processed_input_ids.contains("input_funding_rate_001"));
  assert(snapshot.processed_input_ids.at("input_funding_rate_001")
             .command_kind == RuntimeCommandKind::FundingRateUpdated);
  assert(snapshot.processed_input_ids.at("input_funding_rate_001")
             .idempotency_key.empty());
  assert(snapshot.processed_idempotency_keys.empty());

  auto restored = make_runtime();
  restored.restore_state(snapshot);
  assert(restored.market_sequences().peek(1) == 2);
  assert_funding_rate_fixture_state(restored.funding_rates().at(1));

  const auto duplicate = restored.process(make_record(raw, 1622));
  assert_duplicate_result(duplicate,
                          EngineDuplicateReason::InputId,
                          "input_funding_rate_001",
                          1621,
                          "input_funding_rate_001");
  assert(restored.market_sequences().peek(1) == 2);
}

void test_runtime_funding_settlement_applies_payments_and_updates_risk() {
  auto runtime = make_runtime();
  open_opposite_positions(runtime);
  (void)runtime.process(
      make_record(funding_rate_settlement_json(), 1703));

  const auto result = runtime.process(
      make_record(funding_settlement_tick_json(), 1704));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.duplicate == std::nullopt);
  assert(result.replies.empty());
  assert(result.events.size() == 3);
  assert(result.events[0].type == "FundingPaymentApplied");
  assert(result.events[1].type == "RiskStateUpdated");
  assert(result.events[2].type == "RiskStateUpdated");

  const auto& funding = result.events[0];
  assert(funding.topic == EngineEventsTopic);
  assert(funding.key == "1");
  assert(funding.payload.at("engine_event_id") == "eng_1_10");
  assert(payload_number_text(funding, "engine_sequence") == "10");
  assert(payload_number_text(funding, "engine_timestamp_ms") ==
         "1710000000000");
  assert(payload_number_text(funding, "source_input_offset") == "1704");
  assert(payload_number_text(funding, "market_id") == "1");
  assert(funding.payload.at("source_input_id") == "input_funding_settle_001");
  assert(funding.payload.at("funding_interval_id") ==
         "funding_SOL-PERP_1710000000_1710028800");

  const auto message = parse_serialized_output(funding);
  assert_payload_number(message, "engine_sequence", "10");
  assert_payload_number(message, "source_input_offset", "1704");
  assert_payload_number(message, "market_id", "1");
  assert_payload_string(message, "source_input_id", "input_funding_settle_001");
  const auto& payments = payload_array(message, "payments");
  assert(payments.size() == 2);
  assert_json_number_field(payments[0], "user_id", "42");
  assert_json_string_field(payments[0], "position_id", "pos_42_1");
  assert_json_string_field(payments[0], "side", "LONG");
  assert_json_string_field(payments[0], "asset", "USDC");
  assert_json_number_field(payments[0], "amount", "-10");
  assert_json_number_field(payments[1], "user_id", "43");
  assert_json_string_field(payments[1], "position_id", "pos_43_1");
  assert_json_string_field(payments[1], "side", "SHORT");
  assert_json_string_field(payments[1], "asset", "USDC");
  assert_json_number_field(payments[1], "amount", "10");

  assert(result.events[1].payload.at("position_id") == "pos_42_1");
  assert(payload_number_text(result.events[1], "equity") == "90");
  assert(result.events[1].payload.at("status") == "HEALTHY");
  assert(result.events[2].payload.at("position_id") == "pos_43_1");
  assert(payload_number_text(result.events[2], "equity") == "110");
  assert(result.events[2].payload.at("status") == "HEALTHY");

  const auto maker_key = PositionRiskKey{.user_id = 42, .market_id = 1};
  const auto taker_key = PositionRiskKey{.user_id = 43, .market_id = 1};
  assert(runtime.risk_states().at(maker_key).equity == 90);
  assert(runtime.risk_states().at(taker_key).equity == 110);
  assert(runtime.settled_funding_intervals().contains(FundingSettlementKey{
      .market_id = 1,
      .funding_interval_id = "funding_SOL-PERP_1710000000_1710028800",
  }));
  assert(runtime.snapshot_state().processed_input_ids.contains(
      "input_funding_settle_001"));
}

void test_runtime_funding_settlement_duplicate_input_and_interval_noop() {
  auto runtime = make_runtime();
  open_opposite_positions(runtime);
  (void)runtime.process(
      make_record(funding_rate_settlement_json(), 1721));
  const auto first = runtime.process(
      make_record(funding_settlement_tick_json(), 1722));
  assert(first.status == EngineProcessStatus::Processed);

  const auto duplicate_input = runtime.process(
      make_record(funding_settlement_tick_json(), 1723));
  assert_duplicate_result(duplicate_input,
                          EngineDuplicateReason::InputId,
                          "input_funding_settle_001",
                          1722,
                          "input_funding_settle_001");

  const auto duplicate_interval = runtime.process(make_record(
      funding_settlement_tick_json("input_funding_settle_002"), 1724));
  assert(duplicate_interval.status == EngineProcessStatus::Processed);
  assert(duplicate_interval.empty());
  assert(runtime.settled_funding_intervals().size() == 1);

  const auto maker_key = PositionRiskKey{.user_id = 42, .market_id = 1};
  const auto taker_key = PositionRiskKey{.user_id = 43, .market_id = 1};
  assert(runtime.risk_states().at(maker_key).equity == 90);
  assert(runtime.risk_states().at(taker_key).equity == 110);
  assert(runtime.snapshot_state().processed_input_ids.contains(
      "input_funding_settle_002"));
}

void test_runtime_funding_settlement_missing_or_mismatched_rate_rejected() {
  auto runtime = make_runtime();
  open_opposite_positions(runtime);

  const auto missing = runtime.process(make_record(
      funding_settlement_tick_json("input_funding_missing_001"), 1731));
  assert(missing.status == EngineProcessStatus::Rejected);
  assert(missing.empty());
  assert(runtime.settled_funding_intervals().empty());
  assert(runtime.snapshot_state().processed_input_ids.contains(
      "input_funding_missing_001"));

  (void)runtime.process(make_record(funding_rate_settlement_json(
                              "input_funding_rate_other_001",
                              "funding_other_interval"),
                          1732));
  const auto mismatched = runtime.process(make_record(
      funding_settlement_tick_json("input_funding_mismatch_001"), 1733));
  assert(mismatched.status == EngineProcessStatus::Rejected);
  assert(mismatched.empty());
  assert(runtime.settled_funding_intervals().empty());

  (void)runtime.process(
      make_record(funding_rate_settlement_json(), 1734));
  const auto valid = runtime.process(make_record(
      funding_settlement_tick_json("input_funding_after_reject_001"), 1735));
  assert(valid.status == EngineProcessStatus::Processed);
  assert(valid.events.size() == 3);
  assert(runtime.settled_funding_intervals().size() == 1);
}

void test_mark_price_update_triggers_automatic_liquidation() {
  auto runtime = make_runtime();
  open_opposite_positions(runtime);

  const auto result = runtime.process(make_record(
      mark_price_json("input_mark_auto_liq", 110, 109, 45'060), 1744));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.empty());
  assert(result.events[0].type == "MarkPriceUpdated");
  assert(count_records(result.events, "RiskStateUpdated") >= 2);
  assert(count_records(result.events, "LiquidationStarted") == 1);
  assert(count_records(result.events, "LiquidationCompleted") == 1);
  assert(find_record(result.events, "AdlExecuted") != nullptr);
  assert(find_record(result.events, "LiquidationExecuted") == nullptr);

  const auto* started = find_record(result.events, "LiquidationStarted");
  assert(started != nullptr);
  assert(started->payload.at("source_input_id") == "input_mark_auto_liq");
  assert(payload_number_text(*started, "user_id") == "43");
  assert(started->payload.at("side") == "SHORT");

  const auto* completed = find_record(result.events, "LiquidationCompleted");
  assert(completed != nullptr);
  assert(completed->payload.at("final_status") == "FLAT");
  assert(payload_number_text(*completed, "remaining_quantity") == "0");

  assert_position_state(runtime, 42, 0, 0, 0);
  assert_position_state(runtime, 43, 0, 0, 0);
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 43, .market_id = 1})
             .status == "FLAT");
}

void test_funding_settlement_triggers_automatic_liquidation() {
  auto runtime = make_runtime();
  open_opposite_positions(runtime);

  const auto mark_result = runtime.process(make_record(
      mark_price_json("input_mark_before_funding_auto", 99, 98, 45'061),
      1745));
  assert(mark_result.status == EngineProcessStatus::Processed);
  assert(mark_result.replies.empty());
  assert(find_record(mark_result.events, "LiquidationStarted") == nullptr);
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 42, .market_id = 1})
             .status == "HEALTHY");

  (void)runtime.process(make_record(
      funding_rate_settlement_json("input_funding_rate_auto_liq",
                                   "funding_auto_liq",
                                   50'000),
      1746));

  const auto result = runtime.process(make_record(
      funding_settlement_tick_json("input_funding_settle_auto_liq",
                                   "funding_auto_liq"),
      1747));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.empty());
  assert(result.events[0].type == "FundingPaymentApplied");
  assert(count_records(result.events, "RiskStateUpdated") >= 2);
  assert(count_records(result.events, "LiquidationStarted") == 1);
  assert(count_records(result.events, "LiquidationCompleted") == 1);
  assert(find_record(result.events, "AdlExecuted") != nullptr);

  const auto* started = find_record(result.events, "LiquidationStarted");
  assert(started != nullptr);
  assert(payload_number_text(*started, "user_id") == "42");
  assert(started->payload.at("side") == "LONG");
  assert(started->payload.at("source_input_id") ==
         "input_funding_settle_auto_liq");

  assert_position_state(runtime, 42, 0, 0, 0);
  assert_position_state(runtime, 43, 0, 0, 0);
  assert(runtime.settled_funding_intervals().contains(FundingSettlementKey{
      .market_id = 1,
      .funding_interval_id = "funding_auto_liq",
  }));
}

void test_post_trade_automatic_check_leaves_healthy_users_alone() {
  auto runtime = make_runtime();
  const auto resting = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  (void)runtime.process(make_record(resting, 1748));

  const auto result = runtime.process(make_record(crossing_order_json(), 1749));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(find_record(result.events, "LiquidationStarted") == nullptr);
  assert(find_record(result.events, "LiquidationCompleted") == nullptr);
  assert_position_state(runtime, 42, 10, 100, 100);
  assert_position_state(runtime, 43, -10, 100, 100);
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 42, .market_id = 1})
             .status == "HEALTHY");
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 43, .market_id = 1})
             .status == "HEALTHY");
}

void test_automatic_partial_liquidation_does_not_reenter_same_trigger() {
  auto runtime = make_runtime();
  open_liquidatable_short_position(runtime);
  auto snapshot = runtime.snapshot_state();
  set_snapshot_position(snapshot, 42, 4, 100, 40, 110);
  runtime.restore_state(snapshot);

  const auto result = runtime.evaluate_automatic_liquidations_for_market(
      1, make_record("", 1750));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.empty());
  assert(count_records(result.events, "LiquidationStarted") == 1);
  assert(count_records(result.events, "AdlExecuted") == 1);
  assert(count_records(result.events, "LiquidationCompleted") == 1);

  const auto* completed = find_record(result.events, "LiquidationCompleted");
  assert(completed != nullptr);
  assert(completed->payload.at("final_status") == "PARTIAL");
  assert(payload_number_text(*completed, "remaining_quantity") == "6");
  assert_position_state(runtime, 42, 0, 0, 0);
  assert_position_state(runtime, 43, -6, 100, 60);
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 43, .market_id = 1})
             .status == "LIQUIDATABLE");
}

void test_replay_silent_automatic_liquidation_updates_state_without_outputs() {
  auto runtime = make_runtime();
  open_opposite_positions(runtime);

  const auto result = runtime.process_replay(make_record(
      mark_price_json("input_mark_auto_liq_replay", 110, 109, 45'062),
      1751));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.empty());
  assert_position_state(runtime, 42, 0, 0, 0);
  assert_position_state(runtime, 43, 0, 0, 0);
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 43, .market_id = 1})
             .status == "FLAT");
}

void test_recovery_catchup_hook_runs_automatic_liquidation_scan() {
  auto prepared = make_runtime();
  open_liquidatable_short_position(prepared);

  auto runtime = make_runtime();
  runtime.restore_state(prepared.snapshot_state());
  const auto result =
      runtime.evaluate_all_automatic_liquidations(make_record("", 1752));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.empty());
  assert(count_records(result.events, "LiquidationStarted") == 1);
  assert(count_records(result.events, "LiquidationCompleted") == 1);
  assert(find_record(result.events, "AdlExecuted") != nullptr);
  assert_position_state(runtime, 42, 0, 0, 0);
  assert_position_state(runtime, 43, 0, 0, 0);
}

void test_runtime_liquidation_rejected_for_no_position_or_healthy_risk() {
  auto no_position_runtime = make_runtime();
  const auto no_position = no_position_runtime.process(
      make_record(liquidation_json("input_liq_missing",
                                   "req_liq_missing",
                                   "idem-liq-missing",
                                   "liq_missing"),
                  1750));
  assert(no_position.status == EngineProcessStatus::Processed);
  assert(no_position.replies.size() == 1);
  assert(no_position.events.empty());
  assert(no_position.replies[0].type == "LiquidationRejected");
  assert(no_position.replies[0].payload.at("request_id") == "req_liq_missing");
  assert(no_position.replies[0].payload.at("source_input_id") ==
         "input_liq_missing");
  assert(payload_number_text(no_position.replies[0],
                             "source_input_offset") == "1750");
  assert(no_position.replies[0].payload.at("liquidation_id") == "liq_missing");
  assert(no_position.replies[0].payload.at("reason") ==
         "position is not liquidatable");
  assert(no_position_runtime.snapshot_state().processed_input_ids.contains(
      "input_liq_missing"));
  assert(no_position_runtime.snapshot_state().processed_idempotency_keys
             .contains("idem-liq-missing"));

  auto healthy_runtime = make_runtime();
  open_opposite_positions(healthy_runtime);
  const auto healthy = healthy_runtime.process(
      make_record(liquidation_json("input_liq_healthy",
                                   "req_liq_healthy",
                                   "idem-liq-healthy",
                                   "liq_healthy"),
                  1751));
  assert(healthy.status == EngineProcessStatus::Processed);
  assert(healthy.replies.size() == 1);
  assert(healthy.events.empty());
  assert(healthy.replies[0].type == "LiquidationRejected");
  assert(healthy.replies[0].payload.at("reason") ==
         "position is not liquidatable");
  assert_position_state(healthy_runtime, 43, -10, 100, 100);

  auto no_liquidity_runtime = make_runtime();
  open_liquidatable_short_position(no_liquidity_runtime);
  auto no_liquidity_snapshot = no_liquidity_runtime.snapshot_state();
  no_liquidity_snapshot.positions.erase(PositionRiskKey{.user_id = 42,
                                                        .market_id = 1});
  no_liquidity_snapshot.risk_states.erase(PositionRiskKey{.user_id = 42,
                                                          .market_id = 1});
  no_liquidity_runtime.restore_state(no_liquidity_snapshot);
  const auto no_liquidity = no_liquidity_runtime.process(
      make_record(liquidation_json("input_liq_no_liquidity",
                                   "req_liq_no_liquidity",
                                   "idem-liq-no-liquidity",
                                   "liq_no_liquidity"),
                  1752));
  assert(no_liquidity.status == EngineProcessStatus::Processed);
  assert(no_liquidity.replies.size() == 1);
  assert(no_liquidity.events.empty());
  assert(no_liquidity.replies[0].type == "LiquidationRejected");
  assert(no_liquidity.replies[0].payload.at("reason") ==
         "no liquidation liquidity");
  assert_position_state(no_liquidity_runtime, 43, -10, 100, 100);
  assert(no_liquidity_runtime.positions().find(
             PositionRiskKey{.user_id = 42, .market_id = 1}) ==
         no_liquidity_runtime.positions().end());
}

void test_runtime_liquidation_accepted_flattens_state_and_emits_lifecycle() {
  auto runtime = make_runtime();
  open_liquidatable_short_position(runtime);
  const auto liquidity = runtime.process(make_record(
      liquidation_ask_liquidity_json("input_liq_ask_full",
                                     "req_liq_ask_full",
                                     "idem-liq-ask-full",
                                     9201,
                                     "res_liq_ask_full"),
      1752));
  assert(liquidity.status == EngineProcessStatus::Processed);

  const auto result =
      runtime.process(make_record(liquidation_json(), 1753));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.events.size() == 10);
  assert(result.replies[0].type == "LiquidationAccepted");
  assert(result.replies[0].payload.at("request_id") == "req_liq_001");
  assert(result.replies[0].payload.at("source_input_id") == "input_liq_001");
  assert(result.replies[0].payload.at("source_input_offset") == "1753");
  assert(result.replies[0].payload.at("liquidation_id") == "liq_001");
  assert(result.replies[0].payload.at("order_id").as_string() != nullptr);

  assert(result.events[0].type == "LiquidationStarted");
  assert(payload_number_text(result.events[0], "engine_sequence") == "12");
  assert(payload_number_text(result.events[0], "user_id") == "43");
  assert(result.events[0].payload.at("position_id") == "pos_43_1");
  assert(result.events[0].payload.at("side") == "SHORT");
  assert(payload_number_text(result.events[0], "quantity") == "10");
  assert(payload_number_text(result.events[0], "mark_price") == "110");
  assert(payload_number_text(result.events[0], "maintenance_margin") == "55");
  assert(payload_number_text(result.events[0], "equity") == "0");

  assert(result.events[1].type == "TradeExecuted");
  assert(payload_number_text(result.events[1], "engine_sequence") == "13");
  assert(payload_number_text(result.events[1], "fill_id") == "2");
  assert(payload_number_text(result.events[1], "price") == "95");
  assert(payload_number_text(result.events[1], "quantity") == "10");
  assert(result.events[1].payload.at("execution_reason") == "LIQUIDATION");
  assert(result.events[1].payload.at("liquidation_id") == "liq_001");
  assert(payload_number_text(result.events[1], "liquidated_user_id") == "43");
  assert(result.events[1].payload.at("position_side") == "SHORT");
  assert(payload_number_text(result.events[1], "liquidation_fee") == "4");
  assert(result.events[1].payload.at("maker_reservation_id") ==
         "res_liq_ask_full");
  const auto trade_message = parse_serialized_output(result.events[1]);
  const auto& fee_deltas = payload_array(trade_message, "fee_deltas");
  assert(fee_deltas.size() == 1);
  assert_json_number_field(fee_deltas[0], "amount", "4");
  assert_json_string_field(fee_deltas[0], "fee_type", "LIQUIDATION");
  assert(payload_array(trade_message, "settlements").size() == 1);

  assert(result.events[2].type == "LiquidationExecuted");
  assert(payload_number_text(result.events[2], "engine_sequence") == "14");
  assert(payload_number_text(result.events[2], "fill_id") == "2");
  assert(payload_number_text(result.events[2], "liquidation_fee") == "4");

  assert(result.events[3].type == "PositionChanged");
  assert(payload_number_text(result.events[3], "engine_sequence") == "15");
  assert(payload_number_text(result.events[3], "user_id") == "44");
  assert(result.events[3].payload.at("side") == "SHORT");
  assert(result.events[3].payload.at("reason") == "LIQUIDATION");

  assert(result.events[5].type == "AccountDelta");
  assert(payload_number_text(result.events[5], "engine_sequence") == "17");
  const auto liquidation_order_id =
      payload_number_text(result.replies[0], "order_id");
  assert(result.events[5].payload.at("account_delta_id") ==
         "acct_fill_2_43_" + liquidation_order_id);
  assert(payload_number_text(result.events[5], "user_id") == "43");
  assert(result.events[5].payload.at("asset") == "USDC");
  assert(payload_number_text(result.events[5], "total_delta") == "50");
  assert(payload_number_text(result.events[5], "locked_delta") == "0");
  assert(result.events[5].payload.at("reason") == "LIQUIDATION_SETTLEMENT");
  assert(result.events[5].payload.at("reference_id") == "liq_001");

  assert(result.events[6].type == "PositionChanged");
  assert(payload_number_text(result.events[6], "engine_sequence") == "18");
  assert(payload_number_text(result.events[6], "user_id") == "43");
  assert(result.events[6].payload.at("side") == "FLAT");
  assert(payload_number_text(result.events[6], "signed_quantity") == "0");
  assert(payload_number_text(result.events[6], "quantity") == "0");
  assert(payload_number_text(result.events[6], "isolated_margin") == "0");
  assert(payload_number_text(result.events[6], "realized_pnl") == "50");
  assert(result.events[6].payload.at("reason") == "LIQUIDATION");

  assert(result.events[7].type == "RiskStateUpdated");
  assert(payload_number_text(result.events[7], "engine_sequence") == "19");
  assert(result.events[7].payload.at("status") == "FLAT");
  assert(payload_number_text(result.events[7], "quantity") == "0");
  assert(payload_number_text(result.events[7], "equity") == "0");
  assert(payload_number_text(result.events[7], "maintenance_margin") == "0");

  assert(result.events[8].type == "OrderBookDelta");
  assert(payload_number_text(result.events[8], "engine_sequence") == "20");

  assert(result.events[9].type == "LiquidationCompleted");
  assert(payload_number_text(result.events[9], "engine_sequence") == "21");
  assert(result.events[9].payload.at("final_status") == "FLAT");
  assert(payload_number_text(result.events[9], "remaining_quantity") == "0");
  assert(payload_number_text(result.events[9], "insurance_fund_delta") == "0");
  assert(payload_number_text(result.events[9], "bad_debt") == "0");
  assert(find_record(result.events, "AdlExecuted") == nullptr);

  const auto key = PositionRiskKey{.user_id = 43, .market_id = 1};
  assert(runtime.positions().at(key).signed_quantity == 0);
  assert(runtime.positions().at(key).isolated_margin == 0);
  assert(runtime.risk_states().at(key).status == "FLAT");
  assert(runtime.risk_states().at(key).equity == 0);
  assert(runtime.market_sequences().peek(1) == 22);
  assert(runtime.metadata_store().find(9201) == nullptr);
}

void test_liquidation_duplicate_input_and_idempotency_do_not_reapply() {
  auto runtime = make_runtime();
  open_liquidatable_short_position(runtime);
  (void)runtime.process(make_record(
      liquidation_ask_liquidity_json("input_liq_dup_ask",
                                     "req_liq_dup_ask",
                                     "idem-liq-dup-ask",
                                     9202,
                                     "res_liq_dup_ask"),
      1759));

  const auto first =
      runtime.process(make_record(liquidation_json(), 1760));
  assert(first.status == EngineProcessStatus::Processed);
  assert(first.events.size() == 10);
  assert(runtime.market_sequences().peek(1) == 22);

  const auto same_input =
      runtime.process(make_record(liquidation_json(), 1761));
  assert_duplicate_result(same_input,
                          EngineDuplicateReason::InputId,
                          "input_liq_001",
                          1760,
                          "input_liq_001");
  assert(runtime.market_sequences().peek(1) == 22);

  const auto same_idempotency = runtime.process(
      make_record(liquidation_json("input_liq_002",
                                   "req_liq_002",
                                   "liquidate-001",
                                   "liq_002"),
                  1762));
  assert_duplicate_result(same_idempotency,
                          EngineDuplicateReason::IdempotencyKey,
                          "liquidate-001",
                          1760,
                          "input_liq_001");
  assert(runtime.positions()
             .at(PositionRiskKey{.user_id = 43, .market_id = 1})
             .signed_quantity == 0);
}

void test_runtime_liquidation_residual_book_fill_uses_adl() {
  auto runtime = make_runtime();
  open_liquidatable_short_position(runtime);
  (void)runtime.process(make_record(
      liquidation_ask_liquidity_json("input_liq_ask_partial",
                                     "req_liq_ask_partial",
                                     "idem-liq-ask-partial",
                                     9203,
                                     "res_liq_ask_partial",
                                     4,
                                     95),
      1765));

  const auto result = runtime.process(make_record(
      liquidation_json("input_liq_partial",
                       "req_liq_partial",
                       "idem-liq-partial",
                       "liq_partial",
                       43,
                       "SHORT",
                       10,
                       95),
      1766));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.replies[0].type == "LiquidationAccepted");
  assert(result.events.size() == 18);
  assert(count_records(result.events, "AccountDelta") == 3);
  const auto* trade = find_record(result.events, "TradeExecuted");
  assert(trade != nullptr);
  assert(payload_number_text(*trade, "quantity") == "4");
  assert(trade->payload.at("execution_reason") == "LIQUIDATION");
  const auto* adl = find_record(result.events, "AdlExecuted");
  assert(adl != nullptr);
  assert(payload_number_text(*adl, "quantity") == "6");
  assert(payload_number_text(*adl, "liquidated_user_id") == "43");
  assert(payload_number_text(*adl, "deleveraged_user_id") == "42");
  assert(payload_number_text(*adl, "rank") == "1");
  assert(adl->payload.at("reason") == "INSURANCE_FUND_INSUFFICIENT");

  const auto& completed = result.events.back();
  assert(completed.type == "LiquidationCompleted");
  assert(completed.payload.at("final_status") == "FLAT");
  assert(payload_number_text(completed, "remaining_quantity") == "0");
  assert(payload_number_text(completed, "bad_debt") == "0");

  const auto key = PositionRiskKey{.user_id = 43, .market_id = 1};
  assert(runtime.positions().at(key).signed_quantity == 0);
  assert(runtime.risk_states().at(key).signed_quantity == 0);
  assert_position_state(runtime, 42, 4, 100, 40);
  assert_position_state(runtime, 44, -4, 95, 100);
}

void test_runtime_liquidation_full_adl_without_book_liquidity() {
  auto runtime = make_runtime();
  open_liquidatable_short_position(runtime);
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 42, .market_id = 1})
             .unrealized_pnl > 0);

  const auto result = runtime.process(make_record(
      liquidation_json("input_liq_full_adl",
                       "req_liq_full_adl",
                       "idem-liq-full-adl",
                       "liq_full_adl",
                       43,
                       "SHORT",
                       10,
                       95),
      1767));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.replies[0].type == "LiquidationAccepted");
  assert(result.events.size() == 10);
  assert(count_records(result.events, "AccountDelta") == 2);
  assert(result.events[0].type == "LiquidationStarted");
  assert(find_record(result.events, "LiquidationExecuted") == nullptr);
  assert(find_record(result.events, "OrderBookDelta") == nullptr);

  const auto* trade = find_record(result.events, "TradeExecuted");
  assert(trade != nullptr);
  assert(trade->payload.at("execution_reason") == "LIQUIDATION");
  assert(payload_number_text(*trade, "quantity") == "10");
  assert(payload_number_text(*trade, "counterparty_user_id") == "42");
  assert(payload_number_text(*trade, "liquidation_fee") == "0");
  const auto trade_message = parse_serialized_output(*trade);
  assert(payload_array(trade_message, "fee_deltas").empty());
  assert(payload_array(trade_message, "settlements").empty());

  const auto* adl = find_record(result.events, "AdlExecuted");
  assert(adl != nullptr);
  assert(payload_number_text(*adl, "quantity") == "10");
  assert(payload_number_text(*adl, "liquidated_user_id") == "43");
  assert(payload_number_text(*adl, "deleveraged_user_id") == "42");
  assert(payload_number_text(*adl, "rank") == "1");
  assert(adl->payload.at("position_side") == "SHORT");
  assert(adl->payload.at("reason") == "INSURANCE_FUND_INSUFFICIENT");

  const auto& completed = result.events.back();
  assert(completed.type == "LiquidationCompleted");
  assert(completed.payload.at("final_status") == "FLAT");
  assert(payload_number_text(completed, "remaining_quantity") == "0");
  assert(payload_number_text(completed, "bad_debt") == "0");

  assert_position_state(runtime, 42, 0, 0, 0);
  assert_position_state(runtime, 43, 0, 0, 0);
  assert(runtime.market_sequences().peek(1) == 20);
}

void test_runtime_liquidation_ignores_non_profitable_adl_candidates() {
  auto runtime = make_runtime();
  open_liquidatable_short_position(runtime);
  auto snapshot = runtime.snapshot_state();
  set_snapshot_position(snapshot, 42, 6, 110, 60);
  set_snapshot_position(snapshot, 50, 4, 120, 40);
  runtime.restore_state(snapshot);

  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 42, .market_id = 1})
             .unrealized_pnl == 0);
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 50, .market_id = 1})
             .unrealized_pnl < 0);

  const auto result = runtime.process(make_record(
      liquidation_json("input_liq_unprofitable_adl",
                       "req_liq_unprofitable_adl",
                       "idem-liq-unprofitable-adl",
                       "liq_unprofitable_adl",
                       43,
                       "SHORT",
                       10,
                       95),
      1769));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.events.empty());
  assert(result.replies[0].type == "LiquidationRejected");
  assert(result.replies[0].payload.at("reason") ==
         "no liquidation liquidity");
  assert_position_state(runtime, 42, 6, 110, 60);
  assert_position_state(runtime, 50, 4, 120, 40);
  assert_position_state(runtime, 43, -10, 100, 100);
}

void test_runtime_liquidation_partial_adl_when_opposing_exposure_shortfall() {
  auto runtime = make_runtime();
  open_liquidatable_short_position(runtime);
  auto snapshot = runtime.snapshot_state();
  set_snapshot_position(snapshot, 42, 4, 100, 40);
  set_snapshot_position(snapshot, 50, 6, 120, 60);
  runtime.restore_state(snapshot);

  const auto result = runtime.process(make_record(
      liquidation_json("input_liq_partial_adl",
                       "req_liq_partial_adl",
                       "idem-liq-partial-adl",
                       "liq_partial_adl",
                       43,
                       "SHORT",
                       10,
                       95),
      1768));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.replies[0].type == "LiquidationAccepted");
  assert(result.events.size() == 10);
  assert(count_records(result.events, "AccountDelta") == 2);

  const auto* adl = find_record(result.events, "AdlExecuted");
  assert(adl != nullptr);
  assert(payload_number_text(*adl, "quantity") == "4");
  assert(payload_number_text(*adl, "deleveraged_user_id") == "42");
  assert((adl_counterparty_order(result) == std::vector<std::int64_t>{42}));

  const auto& completed = result.events.back();
  assert(completed.type == "LiquidationCompleted");
  assert(completed.payload.at("final_status") == "PARTIAL");
  assert(payload_number_text(completed, "remaining_quantity") == "6");
  assert(payload_number_text(completed, "bad_debt") == "0");

  assert_position_state(runtime, 42, 0, 0, 0);
  assert_position_state(runtime, 50, 6, 120, 60);
  assert_position_state(runtime, 43, -6, 100, 60);
  assert(runtime.risk_states()
             .at(PositionRiskKey{.user_id = 43, .market_id = 1})
             .signed_quantity == -6);
}

void test_runtime_liquidation_adl_ordering_and_idempotency() {
  auto prepared = make_runtime();
  open_liquidatable_short_position(prepared);
  auto snapshot = prepared.snapshot_state();
  set_snapshot_position(snapshot, 42, 6, 100, 60, 110, 50);
  set_snapshot_position(snapshot, 50, 4, 80, 40, 110, 1);

  auto runtime_a = make_runtime();
  runtime_a.restore_state(snapshot);
  const auto command = liquidation_json("input_liq_adl_order",
                                        "req_liq_adl_order",
                                        "idem-liq-adl-order",
                                        "liq_adl_order",
                                        43,
                                        "SHORT",
                                        10,
                                        95);
  const auto first = runtime_a.process(make_record(command, 1772));
  assert(first.status == EngineProcessStatus::Processed);
  assert((adl_counterparty_order(first) == std::vector<std::int64_t>{50, 42}));
  assert_position_state(runtime_a, 50, 0, 0, 0, 1);
  assert_position_state(runtime_a, 42, 0, 0, 0, 50);
  assert_position_state(runtime_a, 43, 0, 0, 0);
  const auto next_sequence = runtime_a.market_sequences().peek(1);

  const auto duplicate = runtime_a.process(make_record(command, 1773));
  assert_duplicate_result(duplicate,
                          EngineDuplicateReason::InputId,
                          "input_liq_adl_order",
                          1772,
                          "input_liq_adl_order");
  assert(runtime_a.market_sequences().peek(1) == next_sequence);
  assert_position_state(runtime_a, 43, 0, 0, 0);

  auto runtime_b = make_runtime();
  runtime_b.restore_state(snapshot);
  const auto replayed = runtime_b.process(make_record(command, 1772));
  assert(replayed.status == EngineProcessStatus::Processed);
  assert((adl_counterparty_order(replayed) ==
          std::vector<std::int64_t>{50, 42}));
  assert_position_state(runtime_b, 43, 0, 0, 0);
}

void test_replay_silent_resting_order_updates_state_without_outputs() {
  auto runtime = make_runtime();
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");

  const auto result = runtime.process(make_record(raw, 1501),
                                      ProcessingMode::ReplaySilent);
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.duplicate == std::nullopt);
  assert(result.empty());
  assert(runtime.metadata_store().find(9001) != nullptr);
  assert_book_has_order(runtime, 9001);
  assert(runtime.market_sequences().peek(1) == 3);

  const auto snapshot = runtime.snapshot_state();
  assert(snapshot.public_sequences.at(1) == 3);
  assert(snapshot.processed_input_ids.contains("input_place_001"));
  assert(snapshot.processed_idempotency_keys.contains("client-order-001"));

  auto restored = make_runtime();
  restored.restore_state(snapshot);
  const auto duplicate = restored.process_replay(make_record(raw, 1502));
  assert_duplicate_result(duplicate,
                          EngineDuplicateReason::InputId,
                          "input_place_001",
                          1501,
                          "input_place_001");
  assert(restored.market_sequences().peek(1) == 3);
  assert_book_has_order(restored, 9001);
}

void test_replay_silent_liquidation_flattens_state_without_outputs() {
  auto runtime = make_runtime();
  open_liquidatable_short_position(runtime);
  (void)runtime.process_replay(make_record(
      liquidation_ask_liquidity_json("input_liq_replay_ask",
                                     "req_liq_replay_ask",
                                     "idem-liq-replay-ask",
                                     9204,
                                     "res_liq_replay_ask"),
      1769));

  const auto result = runtime.process_replay(
      make_record(liquidation_json(), 1770));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.empty());
  assert(runtime.market_sequences().peek(1) == 22);
  const auto key = PositionRiskKey{.user_id = 43, .market_id = 1};
  assert(runtime.positions().at(key).signed_quantity == 0);
  assert(runtime.risk_states().at(key).status == "FLAT");

  auto restored = make_runtime();
  restored.restore_state(runtime.snapshot_state());
  const auto duplicate = restored.process_replay(
      make_record(liquidation_json(), 1771));
  assert_duplicate_result(duplicate,
                          EngineDuplicateReason::InputId,
                          "input_liq_001",
                          1770,
                          "input_liq_001");
  assert(restored.market_sequences().peek(1) == 22);
  assert(restored.risk_states().at(key).status == "FLAT");
}

void test_replay_silent_crossing_order_updates_state_without_outputs() {
  auto runtime = make_runtime();
  const auto resting = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  (void)runtime.process_replay(make_record(resting, 1511));
  assert(runtime.metadata_store().find(9001) != nullptr);
  assert_book_has_order(runtime, 9001);

  const auto result = runtime.process_replay(
      make_record(crossing_order_json(), 1512));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.duplicate == std::nullopt);
  assert(result.empty());
  assert(runtime.metadata_store().empty());
  assert_book_does_not_have_order(runtime, 9001);
  assert_book_does_not_have_order(runtime, 9002);
  assert_position_state(runtime, 42, 10, 100, 100);
  assert_position_state(runtime, 43, -10, 100, 100);
  assert_risk_state(runtime, 42, 10, 100, 0);
  assert_risk_state(runtime, 43, -10, 100, 0);
  assert(runtime.market_sequences().peek(1) == 9);
}

void test_replay_silent_mark_price_updates_state_without_outputs() {
  auto runtime = make_runtime();
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-mark-price-updated.input.json");

  const auto result = runtime.process_replay(make_record(raw, 1611));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.duplicate == std::nullopt);
  assert(result.empty());
  const auto mark = runtime.mark_prices().find(1);
  assert(mark != runtime.mark_prices().end());
  assert_mark_price_fixture_state(mark->second);
  assert(runtime.market_sequences().peek(1) == 2);

  const auto snapshot = runtime.snapshot_state();
  assert(snapshot.public_sequences.at(1) == 2);
  assert(snapshot.mark_prices.contains(1));
  assert_mark_price_fixture_state(snapshot.mark_prices.at(1));
  assert(snapshot.processed_input_ids.contains("input_mark_001"));
  assert(snapshot.processed_idempotency_keys.empty());

  auto restored = make_runtime();
  restored.restore_state(snapshot);
  const auto duplicate = restored.process_replay(make_record(raw, 1612));
  assert_duplicate_result(duplicate,
                          EngineDuplicateReason::InputId,
                          "input_mark_001",
                          1611,
                          "input_mark_001");
  assert(restored.market_sequences().peek(1) == 2);
  assert_mark_price_fixture_state(restored.mark_prices().at(1));
}

void test_replay_silent_funding_rate_updates_state_without_outputs() {
  auto runtime = make_runtime();
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-funding-rate-updated.input.json");

  const auto result = runtime.process_replay(make_record(raw, 1631));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.duplicate == std::nullopt);
  assert(result.empty());
  const auto funding = runtime.funding_rates().find(1);
  assert(funding != runtime.funding_rates().end());
  assert_funding_rate_fixture_state(funding->second);
  assert(runtime.market_sequences().peek(1) == 2);

  const auto snapshot = runtime.snapshot_state();
  assert(snapshot.public_sequences.at(1) == 2);
  assert(snapshot.funding_rates.contains(1));
  assert_funding_rate_fixture_state(snapshot.funding_rates.at(1));
  assert(snapshot.processed_input_ids.contains("input_funding_rate_001"));
  assert(snapshot.processed_input_ids.at("input_funding_rate_001")
             .command_kind == RuntimeCommandKind::FundingRateUpdated);
  assert(snapshot.processed_idempotency_keys.empty());

  auto restored = make_runtime();
  restored.restore_state(snapshot);
  const auto duplicate = restored.process_replay(make_record(raw, 1632));
  assert_duplicate_result(duplicate,
                          EngineDuplicateReason::InputId,
                          "input_funding_rate_001",
                          1631,
                          "input_funding_rate_001");
  assert(restored.market_sequences().peek(1) == 2);
  assert_funding_rate_fixture_state(restored.funding_rates().at(1));
}

void test_replay_silent_funding_settlement_updates_state_without_outputs() {
  auto runtime = make_runtime();
  (void)runtime.process_replay(make_record(
      place_order_json("input_replay_funding_maker_001",
                       "req_replay_funding_maker_001",
                       "idem-replay-funding-maker-001",
                       9401,
                       "res_replay_funding_maker_001"),
      1641));
  (void)runtime.process_replay(make_record(crossing_order_json(), 1642));
  (void)runtime.process_replay(
      make_record(funding_rate_settlement_json(), 1643));

  const auto result = runtime.process_replay(
      make_record(funding_settlement_tick_json(), 1644));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.duplicate == std::nullopt);
  assert(result.empty());
  assert(runtime.settled_funding_intervals().contains(FundingSettlementKey{
      .market_id = 1,
      .funding_interval_id = "funding_SOL-PERP_1710000000_1710028800",
  }));

  const auto maker_key = PositionRiskKey{.user_id = 42, .market_id = 1};
  const auto taker_key = PositionRiskKey{.user_id = 43, .market_id = 1};
  assert(runtime.risk_states().at(maker_key).equity == 90);
  assert(runtime.risk_states().at(taker_key).equity == 110);
  assert(runtime.snapshot_state().processed_input_ids.contains(
      "input_funding_settle_001"));

  auto restored = make_runtime();
  restored.restore_state(runtime.snapshot_state());
  const auto duplicate = restored.process_replay(
      make_record(funding_settlement_tick_json(), 1645));
  assert_duplicate_result(duplicate,
                          EngineDuplicateReason::InputId,
                          "input_funding_settle_001",
                          1644,
                          "input_funding_settle_001");
  assert(restored.risk_states().at(maker_key).equity == 90);
  assert(restored.risk_states().at(taker_key).equity == 110);
}

void test_live_sequence_continues_after_replayed_events() {
  auto runtime = make_runtime();
  const auto replayed = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  const auto silent = runtime.process_replay(make_record(replayed, 1521));
  assert(silent.empty());
  assert(runtime.market_sequences().peek(1) == 3);

  const auto live = place_order_json("input_after_replay_001",
                                     "req_after_replay_001",
                                     "idem-after-replay-001",
                                     9301,
                                     "res_after_replay_001");
  const auto result = runtime.process(make_record(live, 1522));
  assert(result.status == EngineProcessStatus::Processed);
  assert(result.replies.size() == 1);
  assert(result.events.size() == 2);

  const auto* opened = find_record(result.events, "OrderOpened");
  assert(opened != nullptr);
  assert(opened->payload.at("engine_sequence") == "3");

  const auto* delta = find_record(result.events, "OrderBookDelta");
  assert(delta != nullptr);
  assert(delta->payload.at("engine_sequence") == "4");
  assert(delta->payload.at("quantity") == "20");
  assert(runtime.market_sequences().peek(1) == 5);
}

void test_place_order_duplicate_input_id_is_silent() {
  auto runtime = make_runtime();
  const auto place = place_order_json("input_dedupe_place_001",
                                      "req_dedupe_place_001",
                                      "idem-dedupe-place-001",
                                      9101,
                                      "res_dedupe_place_001");

  const auto first = runtime.process(make_record(place, 1301));
  assert(first.status == EngineProcessStatus::Processed);
  assert(first.duplicate == std::nullopt);
  assert(first.replies.size() == 1);
  assert(first.events.size() == 2);
  assert(runtime.metadata_store().find(9101) != nullptr);

  const auto second = runtime.process(make_record(place, 1301));
  assert_duplicate_result(second,
                          EngineDuplicateReason::InputId,
                          "input_dedupe_place_001",
                          1301,
                          "input_dedupe_place_001");
  assert(runtime.metadata_store().find(9101) != nullptr);
}

void test_place_order_duplicate_idempotency_is_silent() {
  auto runtime = make_runtime();
  const auto first_place = place_order_json("input_dedupe_idem_001",
                                           "req_dedupe_idem_001",
                                           "idem-shared-place-001",
                                           9102,
                                           "res_dedupe_idem_001");
  const auto duplicate_place = place_order_json("input_dedupe_idem_002",
                                               "req_dedupe_idem_002",
                                               "idem-shared-place-001",
                                               9103,
                                               "res_dedupe_idem_002");

  const auto first = runtime.process(make_record(first_place, 1311));
  assert(first.status == EngineProcessStatus::Processed);
  assert(first.replies.size() == 1);
  assert(first.events.size() == 2);

  const auto duplicate = runtime.process(make_record(duplicate_place, 1312));
  assert_duplicate_result(duplicate,
                          EngineDuplicateReason::IdempotencyKey,
                          "idem-shared-place-001",
                          1311,
                          "input_dedupe_idem_001");
  assert(runtime.metadata_store().find(9102) != nullptr);
  assert(runtime.metadata_store().find(9103) == nullptr);
}

void test_same_order_id_different_idempotency_reaches_core() {
  auto runtime = make_runtime();
  const auto first_place = place_order_json("input_same_order_001",
                                           "req_same_order_001",
                                           "idem-same-order-001",
                                           9104,
                                           "res_same_order_001");
  const auto duplicate_order = place_order_json("input_same_order_002",
                                                "req_same_order_002",
                                                "idem-same-order-002",
                                                9104,
                                                "res_same_order_002");

  (void)runtime.process(make_record(first_place, 1321));
  const auto result = runtime.process(make_record(duplicate_order, 1322));

  assert(result.status == EngineProcessStatus::Processed);
  assert(result.duplicate == std::nullopt);
  assert(result.replies.size() == 1);
  assert(result.events.size() == 1);

  const auto* rejected = find_record(result.replies, "OrderRejected");
  assert(rejected != nullptr);
  assert(rejected->payload.at("request_id") == "req_same_order_002");
  assert(rejected->payload.at("source_input_id") == "input_same_order_002");
  assert(rejected->payload.at("source_input_offset") == "1322");
  assert(rejected->payload.at("order_id") == "9104");
  assert(rejected->payload.at("reason") == "duplicate_order");

  const auto* released = find_record(result.events, "ReservationReleased");
  assert(released != nullptr);
  assert(released->payload.at("reservation_id") == "res_same_order_002");
  assert(payload_number_text(*released, "user_id") == "42");
  assert(released->payload.at("asset") == "USDC");
  assert(payload_number_text(*released, "released_amount") == "100");
  assert(released->payload.at("reason") ==
         "ORDER_REJECTED_AFTER_RESERVATION");
}

void test_cancel_duplicate_input_id_and_idempotency_are_silent() {
  auto runtime = make_runtime();
  const auto place = place_order_json("input_cancel_dedupe_place",
                                      "req_cancel_dedupe_place",
                                      "idem-cancel-dedupe-place",
                                      9105,
                                      "res_cancel_dedupe_place");
  const auto cancel = cancel_order_json("input_cancel_dedupe_001",
                                        "req_cancel_dedupe_001",
                                        "idem-cancel-dedupe-001",
                                        9105);
  const auto cancel_same_idempotency =
      cancel_order_json("input_cancel_dedupe_002",
                        "req_cancel_dedupe_002",
                        "idem-cancel-dedupe-001",
                        9105);

  (void)runtime.process(make_record(place, 1331));
  const auto first_cancel = runtime.process(make_record(cancel, 1332));
  assert(first_cancel.status == EngineProcessStatus::Processed);
  assert(find_record(first_cancel.replies, "CancelAccepted") != nullptr);
  assert(find_record(first_cancel.events, "OrderCancelled") != nullptr);
  assert(runtime.metadata_store().empty());

  const auto same_input = runtime.process(make_record(cancel, 1333));
  assert_duplicate_result(same_input,
                          EngineDuplicateReason::InputId,
                          "input_cancel_dedupe_001",
                          1332,
                          "input_cancel_dedupe_001");

  const auto same_idempotency =
      runtime.process(make_record(cancel_same_idempotency, 1334));
  assert_duplicate_result(same_idempotency,
                          EngineDuplicateReason::IdempotencyKey,
                          "idem-cancel-dedupe-001",
                          1332,
                          "input_cancel_dedupe_001");
}

void test_outbox_publishes_replies_then_events_with_routing_and_json() {
  auto runtime = make_runtime();
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  const auto result = runtime.process(make_record(raw, 1401));

  CapturingPublisher publisher;
  EngineOutbox outbox(publisher);
  const auto publish_result = outbox.publish(result);

  assert(publish_result.ok());
  assert(publish_result.attempted == 3);
  assert(publish_result.published == 3);
  assert(publish_result.failures.empty());
  assert(publisher.records.size() == 3);

  const auto& reply = publisher.records[0].record;
  assert(reply.topic == EngineRepliesTopic);
  assert(reply.key == "req_place_001");
  assert(reply.partition == 0);
  assert(reply.type == "OrderAccepted");

  const auto reply_message = parse_serialized_output_json(
      reply, publisher.records[0].serialized_json);
  assert_payload_string(reply_message, "request_id", "req_place_001");
  assert_payload_number(reply_message, "source_input_offset", "1401");

  const auto& opened = publisher.records[1].record;
  assert(opened.topic == EngineEventsTopic);
  assert(opened.key == "1");
  assert(opened.partition == std::nullopt);
  assert(opened.type == "OrderOpened");

  const auto opened_message = parse_serialized_output_json(
      opened, publisher.records[1].serialized_json);
  assert_payload_number(opened_message, "market_id", "1");
  assert_payload_number(opened_message, "engine_sequence", "1");

  const auto& delta = publisher.records[2].record;
  assert(delta.topic == EngineEventsTopic);
  assert(delta.key == "1");
  assert(delta.partition == std::nullopt);
  assert(delta.type == "OrderBookDelta");

  const auto delta_message = parse_serialized_output_json(
      delta, publisher.records[2].serialized_json);
  assert_payload_number(delta_message, "market_id", "1");
  assert_payload_number(delta_message, "engine_sequence", "2");
  const auto& delta_bids = payload_array(delta_message, "bids");
  assert(delta_bids.size() == 1);
  assert_price_level_delta(delta_bids[0], "100", "10");
  assert(payload_array(delta_message, "asks").empty());
}

void test_outbox_skips_duplicate_and_no_output_results() {
  auto runtime = make_runtime();
  const auto place = place_order_json("input_outbox_dedupe_001",
                                      "req_outbox_dedupe_001",
                                      "idem-outbox-dedupe-001",
                                      9201,
                                      "res_outbox_dedupe_001");

  (void)runtime.process(make_record(place, 1411));
  const auto duplicate = runtime.process(make_record(place, 1412));

  CapturingPublisher publisher;
  EngineOutbox outbox(publisher);
  const auto duplicate_publish = outbox.publish(duplicate);
  assert(duplicate_publish.ok());
  assert(duplicate_publish.attempted == 0);
  assert(duplicate_publish.published == 0);
  assert(publisher.records.empty());

  const EngineProcessResult no_output;
  const auto no_output_publish = outbox.publish(no_output);
  assert(no_output_publish.ok());
  assert(no_output_publish.attempted == 0);
  assert(no_output_publish.published == 0);
  assert(publisher.records.empty());
}

void test_outbox_reports_publish_failures() {
  auto runtime = make_runtime();
  const auto raw = read_file(
      std::filesystem::path(PROTOCOL_EXAMPLES_DIR) /
      "engine-place-order.command.json");
  const auto result = runtime.process(make_record(raw, 1421));

  CapturingPublisher publisher;
  publisher.next_error = "publisher unavailable";
  EngineOutbox outbox(publisher);
  const auto publish_result = outbox.publish(result);

  assert(!publish_result.ok());
  assert(publish_result.attempted == 3);
  assert(publish_result.published == 2);
  assert(publish_result.failures.size() == 1);
  assert(publish_result.failures[0].topic == EngineRepliesTopic);
  assert(publish_result.failures[0].key == "req_place_001");
  assert(publish_result.failures[0].partition == 0);
  assert(publish_result.failures[0].type == "OrderAccepted");
  assert(publish_result.failures[0].error == "publisher unavailable");
}

}  // namespace

int main() {
  try {
    test_parses_place_order_fixture();
    test_parses_liquidate_position_fixture();
    test_parses_mark_price_updated_fixture();
    test_parses_funding_rate_updated_fixture();
    test_parses_funding_settlement_tick_fixture();
    test_liquidate_position_validation();
    test_funding_rate_updated_validation();
    test_funding_settlement_tick_validation();
    test_runtime_resting_limit_order();
    test_runtime_crossing_order_emits_trade();
    test_runtime_uses_configured_first_trade_id();
    test_reduce_only_rejects_without_position();
    test_reduce_only_rejects_wrong_side();
    test_reduce_only_exact_close_does_not_rest();
    test_reduce_only_oversized_close_is_capped_and_excess_expires();
    test_mark_price_affects_fill_risk_state();
    test_runtime_cancel_order();
    test_runtime_mark_price_updated_emits_event_without_reply();
    test_runtime_funding_rate_updated_emits_event_without_reply();
    test_runtime_funding_settlement_applies_payments_and_updates_risk();
    test_runtime_funding_settlement_duplicate_input_and_interval_noop();
    test_runtime_funding_settlement_missing_or_mismatched_rate_rejected();
    test_mark_price_update_triggers_automatic_liquidation();
    test_funding_settlement_triggers_automatic_liquidation();
    test_post_trade_automatic_check_leaves_healthy_users_alone();
    test_automatic_partial_liquidation_does_not_reenter_same_trigger();
    test_replay_silent_automatic_liquidation_updates_state_without_outputs();
    test_recovery_catchup_hook_runs_automatic_liquidation_scan();
    test_runtime_liquidation_rejected_for_no_position_or_healthy_risk();
    test_runtime_liquidation_accepted_flattens_state_and_emits_lifecycle();
    test_liquidation_duplicate_input_and_idempotency_do_not_reapply();
    test_runtime_liquidation_residual_book_fill_uses_adl();
    test_runtime_liquidation_full_adl_without_book_liquidity();
    test_runtime_liquidation_ignores_non_profitable_adl_candidates();
    test_runtime_liquidation_partial_adl_when_opposing_exposure_shortfall();
    test_runtime_liquidation_adl_ordering_and_idempotency();
    test_replay_silent_resting_order_updates_state_without_outputs();
    test_replay_silent_liquidation_flattens_state_without_outputs();
    test_replay_silent_crossing_order_updates_state_without_outputs();
    test_replay_silent_mark_price_updates_state_without_outputs();
    test_replay_silent_funding_rate_updates_state_without_outputs();
    test_replay_silent_funding_settlement_updates_state_without_outputs();
    test_live_sequence_continues_after_replayed_events();
    test_place_order_duplicate_input_id_is_silent();
    test_place_order_duplicate_idempotency_is_silent();
    test_same_order_id_different_idempotency_reaches_core();
    test_cancel_duplicate_input_id_and_idempotency_are_silent();
    test_outbox_publishes_replies_then_events_with_routing_and_json();
    test_outbox_skips_duplicate_and_no_output_results();
    test_outbox_reports_publish_failures();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
