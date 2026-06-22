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
  assert(payload_array(trade_message, "settlements").empty());

  const auto* delta = find_record(result.events, "OrderBookDelta");
  assert(delta != nullptr);
  assert(delta->payload.at("engine_sequence") == "4");
  assert(delta->payload.at("quantity") == "0");
  const auto delta_message = parse_serialized_output(*delta);
  const auto& delta_bids = payload_array(delta_message, "bids");
  assert(delta_bids.size() == 1);
  assert_price_level_delta(delta_bids[0], "100", "0");
  assert(payload_array(delta_message, "asks").empty());
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
  assert(runtime.market_sequences().peek(1) == 5);
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
  assert(result.events.empty());

  const auto* rejected = find_record(result.replies, "OrderRejected");
  assert(rejected != nullptr);
  assert(rejected->payload.at("request_id") == "req_same_order_002");
  assert(rejected->payload.at("source_input_id") == "input_same_order_002");
  assert(rejected->payload.at("source_input_offset") == "1322");
  assert(rejected->payload.at("order_id") == "9104");
  assert(rejected->payload.at("reason") == "duplicate_order");
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
    test_parses_mark_price_updated_fixture();
    test_parses_funding_rate_updated_fixture();
    test_funding_rate_updated_validation();
    test_runtime_resting_limit_order();
    test_runtime_crossing_order_emits_trade();
    test_runtime_cancel_order();
    test_runtime_mark_price_updated_emits_event_without_reply();
    test_runtime_funding_rate_updated_emits_event_without_reply();
    test_replay_silent_resting_order_updates_state_without_outputs();
    test_replay_silent_crossing_order_updates_state_without_outputs();
    test_replay_silent_mark_price_updates_state_without_outputs();
    test_replay_silent_funding_rate_updates_state_without_outputs();
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
