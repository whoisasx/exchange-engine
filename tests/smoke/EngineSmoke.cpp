#include "protocol/Message.hpp"
#include "runtime/EngineOutbox.hpp"
#include "runtime/EngineRuntime.hpp"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifndef PROTOCOL_EXAMPLES_DIR
#error "PROTOCOL_EXAMPLES_DIR must be defined"
#endif

using namespace cex::runtime;

namespace {

class SmokeFailure final : public std::runtime_error {
 public:
  explicit SmokeFailure(const std::string& message)
      : std::runtime_error(message) {}
};

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
    return std::nullopt;
  }

  std::vector<PublishedRecord> records;
};

struct ProcessedStep {
  EngineProcessResult result;
  EnginePublishResult publish_result;
  std::vector<CapturingPublisher::PublishedRecord> published;
};

void require(bool condition, const std::string& message) {
  if (!condition) {
    throw SmokeFailure(message);
  }
}

void require_eq(const std::string& actual,
                const std::string& expected,
                const std::string& label) {
  if (actual != expected) {
    throw SmokeFailure(label + ": expected '" + expected + "', got '" +
                       actual + "'");
  }
}

void require_eq(std::int64_t actual,
                std::int64_t expected,
                const std::string& label) {
  if (actual != expected) {
    throw SmokeFailure(label + ": expected " + std::to_string(expected) +
                       ", got " + std::to_string(actual));
  }
}

std::string read_file(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) {
    throw SmokeFailure("failed to open " + path.string());
  }

  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

std::string fixture_json(const std::string& filename) {
  return read_file(std::filesystem::path(PROTOCOL_EXAMPLES_DIR) / filename);
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

protocol::ProtocolMessage parse_published_output(
    const CapturingPublisher::PublishedRecord& published) {
  const protocol::JsonValue root =
      protocol::parse_json(published.serialized_json);
  require(root.is_object(), published.record.type + " output root is not JSON object");

  const auto* type = root.find("type");
  require(type != nullptr, published.record.type + " output missing type");
  require(type->as_string() != nullptr,
          published.record.type + " output type is not a string");
  require_eq(*type->as_string(), published.record.type, "serialized type");

  const auto* payload = root.find("payload");
  require(payload != nullptr, published.record.type + " output missing payload");
  require(payload->is_object(),
          published.record.type + " output payload is not an object");

  protocol::ProtocolMessage message =
      protocol::parse_protocol_message(published.serialized_json);
  require_eq(message.type, published.record.type, "protocol message type");
  require(message.payload.is_object(),
          published.record.type + " protocol payload is not an object");
  return message;
}

std::string payload_text(const protocol::ProtocolMessage& message,
                         const std::string& field) {
  const auto* value = message.payload.find(field);
  require(value != nullptr, message.type + " payload missing " + field);
  if (const auto* text = value->as_string(); text != nullptr) {
    return *text;
  }
  if (const auto* number = value->as_number(); number != nullptr) {
    return number->text;
  }
  if (const auto* boolean = value->as_bool(); boolean != nullptr) {
    return *boolean ? "true" : "false";
  }
  throw SmokeFailure(message.type + " payload field " + field +
                     " is not scalar");
}

void require_payload(const protocol::ProtocolMessage& message,
                     const std::string& field,
                     const std::string& expected) {
  require_eq(payload_text(message, field), expected,
             message.type + " payload." + field);
}

const protocol::JsonValue::Array& require_payload_array(
    const protocol::ProtocolMessage& message,
    const std::string& field) {
  const auto* value = message.payload.find(field);
  require(value != nullptr, message.type + " payload missing " + field);
  const auto* array = value->as_array();
  require(array != nullptr,
          message.type + " payload." + field + " is not an array");
  return *array;
}

void require_price_level_delta(const protocol::JsonValue& value,
                               const std::string& expected_price,
                               const std::string& expected_quantity,
                               const std::string& label) {
  require(value.as_object() != nullptr, label + " is not an object");
  const auto* price = value.find("price");
  require(price != nullptr, label + " missing price");
  require(price->as_number() != nullptr, label + ".price is not a number");
  require_eq(price->as_number()->text, expected_price, label + ".price");

  const auto* quantity = value.find("quantity");
  require(quantity != nullptr, label + " missing quantity");
  require(quantity->as_number() != nullptr,
          label + ".quantity is not a number");
  require_eq(quantity->as_number()->text,
             expected_quantity,
             label + ".quantity");
}

void require_json_string_field(const protocol::JsonValue& value,
                               const std::string& field,
                               const std::string& expected,
                               const std::string& label) {
  const auto* child = value.find(field);
  require(child != nullptr, label + " missing " + field);
  require(child->as_string() != nullptr, label + "." + field +
                                         " is not a string");
  require_eq(*child->as_string(), expected, label + "." + field);
}

void require_json_number_field(const protocol::JsonValue& value,
                               const std::string& field,
                               const std::string& expected,
                               const std::string& label) {
  const auto* child = value.find(field);
  require(child != nullptr, label + " missing " + field);
  require(child->as_number() != nullptr, label + "." + field +
                                         " is not a number");
  require_eq(child->as_number()->text, expected, label + "." + field);
}

void require_trade_settlement(const protocol::JsonValue& value,
                              const std::string& reservation_id,
                              const std::string& asset,
                              const std::string& amount,
                              const std::string& label) {
  require(value.as_object() != nullptr, label + " is not an object");
  require_json_string_field(value, "reservation_id", reservation_id, label);
  require_json_string_field(value, "debit_asset", asset, label);
  require_json_number_field(value, "debit_amount", amount, label);
  require_json_string_field(value, "credit_asset", asset, label);
  require_json_number_field(value, "credit_amount", amount, label);
}

const CapturingPublisher::PublishedRecord& require_published_type(
    const std::vector<CapturingPublisher::PublishedRecord>& records,
    const std::string& type) {
  for (const auto& record : records) {
    if (record.record.type == type) {
      return record;
    }
  }
  throw SmokeFailure("missing published record type " + type);
}

void require_no_published_type(
    const std::vector<CapturingPublisher::PublishedRecord>& records,
    const std::string& type) {
  for (const auto& record : records) {
    require(record.record.type != type,
            "unexpected published record type " + type);
  }
}

void require_reply_routing(const CapturingPublisher::PublishedRecord& published,
                           const std::string& request_id) {
  require_eq(published.record.topic, EngineRepliesTopic, published.record.type + " topic");
  require_eq(published.record.key, request_id, published.record.type + " key");
  require(published.record.partition.has_value(),
          published.record.type + " reply partition missing");
  require_eq(*published.record.partition, 0, published.record.type + " partition");
}

void require_event_routing(const CapturingPublisher::PublishedRecord& published) {
  require_eq(published.record.topic, EngineEventsTopic, published.record.type + " topic");
  require_eq(published.record.key, "1", published.record.type + " key");
  require(!published.record.partition.has_value(),
          published.record.type + " event partition should be unset");
}

ProcessedStep process_and_publish(EngineRuntime& runtime,
                                  std::string raw_json,
                                  std::int64_t offset) {
  auto result = runtime.process(make_record(std::move(raw_json), offset));

  CapturingPublisher publisher;
  EngineOutbox outbox(publisher);
  auto publish_result = outbox.publish(result);

  require(publish_result.ok(), "outbox publish failed");
  require_eq(static_cast<std::int64_t>(publish_result.published),
             static_cast<std::int64_t>(publisher.records.size()),
             "published count");
  require_eq(static_cast<std::int64_t>(publish_result.attempted),
             static_cast<std::int64_t>(publisher.records.size()),
             "attempted count");

  for (const auto& published : publisher.records) {
    (void)parse_published_output(published);
  }

  return ProcessedStep{
      .result = std::move(result),
      .publish_result = std::move(publish_result),
      .published = std::move(publisher.records),
  };
}

void run_resting_crossing_and_duplicate_smoke() {
  auto runtime = make_runtime();

  const auto resting = process_and_publish(
      runtime, fixture_json("engine-place-order.command.json"), 1201);
  require(resting.result.status == EngineProcessStatus::Processed,
          "resting order was not processed");
  require_eq(static_cast<std::int64_t>(resting.published.size()), 3,
             "resting published count");
  require_eq(resting.published[0].record.type, "OrderAccepted",
             "resting first output");
  require_eq(resting.published[1].record.type, "OrderOpened",
             "resting second output");
  require_eq(resting.published[2].record.type, "OrderBookDelta",
             "resting third output");

  const auto& accepted = require_published_type(resting.published, "OrderAccepted");
  require_reply_routing(accepted, "req_place_001");
  const auto accepted_message = parse_published_output(accepted);
  require_payload(accepted_message, "request_id", "req_place_001");
  require_payload(accepted_message, "source_input_id", "input_place_001");
  require_payload(accepted_message, "source_input_offset", "1201");
  require_payload(accepted_message, "order_id", "9001");
  require_payload(accepted_message, "reservation_id", "res_place_001");

  const auto& opened = require_published_type(resting.published, "OrderOpened");
  require_event_routing(opened);
  const auto opened_message = parse_published_output(opened);
  require_payload(opened_message, "engine_sequence", "1");
  require_payload(opened_message, "engine_timestamp_ms", "1710000000000");
  require_payload(opened_message, "market_id", "1");
  require_payload(opened_message, "order_id", "9001");
  require_payload(opened_message, "side", "LONG");
  require_payload(opened_message, "price", "100");
  require_payload(opened_message, "quantity", "10");

  const auto& resting_delta =
      require_published_type(resting.published, "OrderBookDelta");
  require_event_routing(resting_delta);
  const auto resting_delta_message = parse_published_output(resting_delta);
  require_payload(resting_delta_message, "engine_sequence", "2");
  require_payload(resting_delta_message, "side", "LONG");
  require_payload(resting_delta_message, "price", "100");
  require_payload(resting_delta_message, "quantity", "10");
  const auto& resting_bids = require_payload_array(resting_delta_message, "bids");
  require_eq(static_cast<std::int64_t>(resting_bids.size()),
             1,
             "resting delta bids count");
  require_price_level_delta(resting_bids[0],
                            "100",
                            "10",
                            "resting delta bids[0]");
  require_eq(static_cast<std::int64_t>(
                 require_payload_array(resting_delta_message, "asks").size()),
             0,
             "resting delta asks count");
  require(runtime.metadata_store().find(9001) != nullptr,
          "resting order metadata was not retained");

  const auto crossing =
      process_and_publish(runtime, crossing_order_json(), 1202);
  require(crossing.result.status == EngineProcessStatus::Processed,
          "crossing order was not processed");
  require_eq(static_cast<std::int64_t>(crossing.published.size()), 7,
             "crossing published count");
  require_eq(crossing.published[0].record.type, "OrderAccepted",
             "crossing first output");
  require_eq(crossing.published[1].record.type, "TradeExecuted",
             "crossing second output");
  require_eq(crossing.published[2].record.type, "PositionChanged",
             "crossing third output");
  require_eq(crossing.published[3].record.type, "RiskStateUpdated",
             "crossing fourth output");
  require_eq(crossing.published[4].record.type, "PositionChanged",
             "crossing fifth output");
  require_eq(crossing.published[5].record.type, "RiskStateUpdated",
             "crossing sixth output");
  require_eq(crossing.published[6].record.type, "OrderBookDelta",
             "crossing seventh output");
  require_no_published_type(crossing.published, "OrderOpened");

  const auto& crossing_reply =
      require_published_type(crossing.published, "OrderAccepted");
  require_reply_routing(crossing_reply, "req_place_002");
  const auto crossing_reply_message = parse_published_output(crossing_reply);
  require_payload(crossing_reply_message, "request_id", "req_place_002");
  require_payload(crossing_reply_message, "source_input_id", "input_place_002");
  require_payload(crossing_reply_message, "order_id", "9002");

  const auto& trade = require_published_type(crossing.published, "TradeExecuted");
  require_event_routing(trade);
  const auto trade_message = parse_published_output(trade);
  require_payload(trade_message, "engine_sequence", "3");
  require_payload(trade_message, "source_input_offset", "1202");
  require_payload(trade_message, "trade_id", "1");
  require_payload(trade_message, "fill_id", "1");
  require_payload(trade_message, "price", "100");
  require_payload(trade_message, "quantity", "10");
  require_payload(trade_message, "maker_order_id", "9001");
  require_payload(trade_message, "taker_order_id", "9002");
  require_payload(trade_message, "maker_reservation_id", "res_place_001");
  require_payload(trade_message, "taker_reservation_id", "res_taker_001");
  require_eq(static_cast<std::int64_t>(
                 require_payload_array(trade_message, "fee_deltas").size()),
             0,
             "trade fee_deltas count");
  const auto& settlements = require_payload_array(trade_message, "settlements");
  require_eq(static_cast<std::int64_t>(settlements.size()),
             2,
             "trade settlements count");
  require_trade_settlement(settlements[0],
                           "res_place_001",
                           "USDC",
                           "100",
                           "maker settlement");
  require_trade_settlement(settlements[1],
                           "res_taker_001",
                           "USDC",
                           "100",
                           "taker settlement");

  const auto maker_position_message =
      parse_published_output(crossing.published[2]);
  require_payload(maker_position_message, "engine_sequence", "4");
  require_payload(maker_position_message, "position_id", "pos_42_1");
  require_payload(maker_position_message, "user_id", "42");
  require_payload(maker_position_message, "side", "LONG");
  require_payload(maker_position_message, "signed_quantity", "10");
  require_payload(maker_position_message, "average_entry_price", "100");
  require_payload(maker_position_message, "mark_price", "100");
  require_payload(maker_position_message, "isolated_margin", "100");
  require_payload(maker_position_message, "realized_pnl", "0");
  require_payload(maker_position_message, "unrealized_pnl", "0");
  require_payload(maker_position_message, "maintenance_margin", "50");
  require_payload(maker_position_message, "liquidation_price", "0");
  require_payload(maker_position_message, "reason", "TRADE");

  const auto maker_risk_message =
      parse_published_output(crossing.published[3]);
  require_payload(maker_risk_message, "engine_sequence", "5");
  require_payload(maker_risk_message, "position_id", "pos_42_1");
  require_payload(maker_risk_message, "user_id", "42");
  require_payload(maker_risk_message, "status", "HEALTHY");
  require_payload(maker_risk_message, "mark_price", "100");
  require_payload(maker_risk_message, "unrealized_pnl", "0");
  require_payload(maker_risk_message, "equity", "100");
  require_payload(maker_risk_message, "maintenance_margin", "50");
  require_payload(maker_risk_message, "margin_ratio", "20000");

  const auto taker_position_message =
      parse_published_output(crossing.published[4]);
  require_payload(taker_position_message, "engine_sequence", "6");
  require_payload(taker_position_message, "position_id", "pos_43_1");
  require_payload(taker_position_message, "user_id", "43");
  require_payload(taker_position_message, "side", "SHORT");
  require_payload(taker_position_message, "signed_quantity", "-10");
  require_payload(taker_position_message, "average_entry_price", "100");
  require_payload(taker_position_message, "mark_price", "100");
  require_payload(taker_position_message, "isolated_margin", "100");
  require_payload(taker_position_message, "realized_pnl", "0");
  require_payload(taker_position_message, "unrealized_pnl", "0");
  require_payload(taker_position_message, "maintenance_margin", "50");
  require_payload(taker_position_message, "liquidation_price", "0");
  require_payload(taker_position_message, "reason", "TRADE");

  const auto taker_risk_message =
      parse_published_output(crossing.published[5]);
  require_payload(taker_risk_message, "engine_sequence", "7");
  require_payload(taker_risk_message, "position_id", "pos_43_1");
  require_payload(taker_risk_message, "user_id", "43");
  require_payload(taker_risk_message, "status", "HEALTHY");
  require_payload(taker_risk_message, "mark_price", "100");
  require_payload(taker_risk_message, "unrealized_pnl", "0");
  require_payload(taker_risk_message, "equity", "100");
  require_payload(taker_risk_message, "maintenance_margin", "50");
  require_payload(taker_risk_message, "margin_ratio", "20000");

  const auto& crossing_delta =
      require_published_type(crossing.published, "OrderBookDelta");
  require_event_routing(crossing_delta);
  const auto crossing_delta_message = parse_published_output(crossing_delta);
  require_payload(crossing_delta_message, "engine_sequence", "8");
  require_payload(crossing_delta_message, "side", "LONG");
  require_payload(crossing_delta_message, "price", "100");
  require_payload(crossing_delta_message, "quantity", "0");
  const auto& crossing_bids =
      require_payload_array(crossing_delta_message, "bids");
  require_eq(static_cast<std::int64_t>(crossing_bids.size()),
             1,
             "crossing delta bids count");
  require_price_level_delta(crossing_bids[0],
                            "100",
                            "0",
                            "crossing delta bids[0]");
  require_eq(static_cast<std::int64_t>(
                 require_payload_array(crossing_delta_message, "asks").size()),
             0,
             "crossing delta asks count");
  require(runtime.metadata_store().empty(),
          "filled maker metadata was not cleaned up");
  require_eq(static_cast<std::int64_t>(runtime.market_sequences().peek(1)), 9,
             "next market sequence after trade");

  const auto duplicate =
      process_and_publish(runtime, crossing_order_json(), 1203);
  require(duplicate.result.status == EngineProcessStatus::Duplicate,
          "duplicate crossing order was not marked duplicate");
  require(duplicate.result.duplicate.has_value(),
          "duplicate info was not populated");
  require(duplicate.result.duplicate->reason == EngineDuplicateReason::InputId,
          "duplicate reason was not input_id");
  require_eq(duplicate.result.duplicate->key, "input_place_002",
             "duplicate key");
  require_eq(duplicate.result.duplicate->original_offset, 1202,
             "duplicate original offset");
  require_eq(static_cast<std::int64_t>(duplicate.published.size()), 0,
             "duplicate published count");
  require_eq(static_cast<std::int64_t>(duplicate.publish_result.attempted), 0,
             "duplicate publish attempts");
}

void run_cancel_smoke() {
  auto runtime = make_runtime();
  (void)process_and_publish(
      runtime, fixture_json("engine-place-order.command.json"), 2201);

  const auto cancel = process_and_publish(
      runtime, fixture_json("engine-cancel-order.command.json"), 2202);
  require(cancel.result.status == EngineProcessStatus::Processed,
          "cancel was not processed");
  require_eq(static_cast<std::int64_t>(cancel.published.size()), 3,
             "cancel published count");
  require_eq(cancel.published[0].record.type, "CancelAccepted",
             "cancel first output");
  require_eq(cancel.published[1].record.type, "OrderCancelled",
             "cancel second output");
  require_eq(cancel.published[2].record.type, "OrderBookDelta",
             "cancel third output");

  const auto& reply = require_published_type(cancel.published, "CancelAccepted");
  require_reply_routing(reply, "req_cancel_001");
  const auto reply_message = parse_published_output(reply);
  require_payload(reply_message, "request_id", "req_cancel_001");
  require_payload(reply_message, "source_input_id", "input_cancel_001");
  require_payload(reply_message, "source_input_offset", "2202");
  require_payload(reply_message, "order_id", "9001");

  const auto& cancelled =
      require_published_type(cancel.published, "OrderCancelled");
  require_event_routing(cancelled);
  const auto cancelled_message = parse_published_output(cancelled);
  require_payload(cancelled_message, "engine_sequence", "3");
  require_payload(cancelled_message, "order_id", "9001");
  require_payload(cancelled_message, "reservation_id", "res_place_001");
  require_payload(cancelled_message, "released_quantity", "10");
  require_payload(cancelled_message, "released_amount", "100");
  require_payload(cancelled_message, "market_id", "1");

  const auto& delta = require_published_type(cancel.published, "OrderBookDelta");
  require_event_routing(delta);
  const auto delta_message = parse_published_output(delta);
  require_payload(delta_message, "engine_sequence", "4");
  require_payload(delta_message, "side", "LONG");
  require_payload(delta_message, "price", "100");
  require_payload(delta_message, "quantity", "0");
  const auto& bids = require_payload_array(delta_message, "bids");
  require_eq(static_cast<std::int64_t>(bids.size()),
             1,
             "cancel delta bids count");
  require_price_level_delta(bids[0], "100", "0", "cancel delta bids[0]");
  require_eq(static_cast<std::int64_t>(
                 require_payload_array(delta_message, "asks").size()),
             0,
             "cancel delta asks count");
  require(runtime.metadata_store().empty(),
          "cancelled order metadata was not cleaned up");
}

}  // namespace

int main() {
  try {
    run_resting_crossing_and_duplicate_smoke();
    run_cancel_smoke();
  } catch (const std::exception& error) {
    std::cerr << "engine_smoke failed: " << error.what() << '\n';
    return EXIT_FAILURE;
  }

  std::cout << "engine_smoke passed\n";
  return EXIT_SUCCESS;
}
