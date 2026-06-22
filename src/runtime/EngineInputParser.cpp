#include "runtime/EngineInputParser.hpp"

#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace cex::runtime {
namespace {

[[nodiscard]] EngineInputParserError parser_error(std::string message) {
  return EngineInputParserError(std::move(message));
}

[[nodiscard]] const protocol::JsonValue& require_field(
    const protocol::JsonValue& object,
    std::string_view field,
    std::string_view path) {
  const auto* value = object.find(field);
  if (value == nullptr) {
    throw parser_error(std::string(path) + "." + std::string(field) +
                       " is required");
  }
  return *value;
}

[[nodiscard]] const protocol::JsonValue& require_payload_field(
    const protocol::ProtocolMessage& message,
    std::string_view field) {
  return require_field(message.payload, field, "payload");
}

[[nodiscard]] std::string require_string(const protocol::JsonValue& object,
                                         std::string_view field,
                                         std::string_view path) {
  const auto& value = require_field(object, field, path);
  const auto* text = value.as_string();
  if (text == nullptr || text->empty()) {
    throw parser_error(std::string(path) + "." + std::string(field) +
                       " must be a non-empty string");
  }
  return *text;
}

[[nodiscard]] std::string require_payload_string(
    const protocol::ProtocolMessage& message,
    std::string_view field) {
  const auto& value = require_payload_field(message, field);
  const auto* text = value.as_string();
  if (text == nullptr || text->empty()) {
    throw parser_error("payload." + std::string(field) +
                       " must be a non-empty string");
  }
  return *text;
}

[[nodiscard]] std::optional<std::string> optional_payload_string(
    const protocol::ProtocolMessage& message,
    std::string_view field) {
  const auto* value = message.payload.find(field);
  if (value == nullptr || value->is_null()) {
    return std::nullopt;
  }

  const auto* text = value->as_string();
  if (text == nullptr || text->empty()) {
    throw parser_error("payload." + std::string(field) +
                       " must be a non-empty string when present");
  }
  return *text;
}

[[nodiscard]] std::int64_t parse_integer_text(std::string_view text,
                                              std::string_view path) {
  std::int64_t parsed{0};
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto result = std::from_chars(begin, end, parsed);
  if (result.ec != std::errc{} || result.ptr != end) {
    throw parser_error(std::string(path) + " must be an integer");
  }
  return parsed;
}

[[nodiscard]] std::int64_t require_i64(const protocol::JsonValue& object,
                                       std::string_view field,
                                       std::string_view path) {
  const auto& value = require_field(object, field, path);
  const std::string field_path =
      std::string(path) + "." + std::string(field);
  if (const auto* number = value.as_number(); number != nullptr) {
    return parse_integer_text(number->text, field_path);
  }
  if (const auto* text = value.as_string(); text != nullptr) {
    return parse_integer_text(*text, field_path);
  }
  throw parser_error(field_path + " must be an integer");
}

[[nodiscard]] std::int32_t require_i32(const protocol::JsonValue& object,
                                       std::string_view field,
                                       std::string_view path) {
  const auto value = require_i64(object, field, path);
  if (value < std::numeric_limits<std::int32_t>::min() ||
      value > std::numeric_limits<std::int32_t>::max()) {
    throw parser_error(std::string(path) + "." + std::string(field) +
                       " is outside int32 range");
  }
  return static_cast<std::int32_t>(value);
}

[[nodiscard]] bool require_bool(const protocol::JsonValue& object,
                                std::string_view field,
                                std::string_view path) {
  const auto& value = require_field(object, field, path);
  const auto* boolean = value.as_bool();
  if (boolean == nullptr) {
    throw parser_error(std::string(path) + "." + std::string(field) +
                       " must be a boolean");
  }
  return *boolean;
}

[[nodiscard]] cex::adapter::CommandEnvelope require_envelope(
    const protocol::JsonValue& payload) {
  const auto& envelope_value = require_field(payload, "envelope", "payload");
  if (!envelope_value.is_object()) {
    throw parser_error("payload.envelope must be an object");
  }

  return cex::adapter::CommandEnvelope{
      .request_id =
          require_string(envelope_value, "request_id", "payload.envelope"),
      .idempotency_key =
          require_string(envelope_value, "idempotency_key", "payload.envelope"),
      .user_id =
          require_i64(envelope_value, "user_id", "payload.envelope"),
      .reply_partition =
          require_i32(envelope_value, "reply_partition", "payload.envelope"),
  };
}

[[nodiscard]] cex::adapter::AdapterSide parse_side(std::string_view value) {
  if (value == "LONG") {
    return cex::adapter::AdapterSide::Long;
  }
  if (value == "SHORT") {
    return cex::adapter::AdapterSide::Short;
  }
  throw parser_error("payload.side must be LONG or SHORT");
}

[[nodiscard]] cex::adapter::AdapterOrderType parse_order_type(
    std::string_view value) {
  if (value == "LIMIT") {
    return cex::adapter::AdapterOrderType::Limit;
  }
  if (value == "MARKET") {
    return cex::adapter::AdapterOrderType::Market;
  }
  throw parser_error("payload.order_type must be LIMIT or MARKET");
}

[[nodiscard]] cex::adapter::AdapterTimeInForce parse_time_in_force(
    std::string_view value) {
  if (value == "GTC") {
    return cex::adapter::AdapterTimeInForce::Gtc;
  }
  if (value == "IOC") {
    return cex::adapter::AdapterTimeInForce::Ioc;
  }
  if (value == "FOK") {
    return cex::adapter::AdapterTimeInForce::Fok;
  }
  if (value == "POST_ONLY" || value == "PO") {
    return cex::adapter::AdapterTimeInForce::PostOnly;
  }
  throw parser_error(
      "payload.time_in_force must be GTC, IOC, FOK, POST_ONLY, or PO");
}

[[nodiscard]] cex::adapter::AdapterTimeInForce optional_time_in_force(
    const protocol::JsonValue& payload) {
  const auto* value = payload.find("time_in_force");
  if (value == nullptr || value->is_null()) {
    return cex::adapter::AdapterTimeInForce::Gtc;
  }
  const auto* text = value->as_string();
  if (text == nullptr || text->empty()) {
    throw parser_error("payload.time_in_force must be a non-empty string");
  }
  return parse_time_in_force(*text);
}

}  // namespace

EngineInputParserError::EngineInputParserError(std::string message)
    : std::runtime_error(std::move(message)) {}

ParsedEngineInput EngineInputParser::parse(std::string_view raw_json) const {
  protocol::ProtocolMessage message;
  try {
    message = protocol::parse_protocol_message(raw_json);
  } catch (const std::exception& error) {
    throw parser_error(std::string("failed to parse engine input: ") +
                       error.what());
  }

  if (message.type == "PlaceOrder") {
    return ParsedEngineInput{
        .kind = ParsedEngineInputKind::PlaceOrder,
        .value = parse_place_order(message),
    };
  }
  if (message.type == "CancelOrder") {
    return ParsedEngineInput{
        .kind = ParsedEngineInputKind::CancelOrder,
        .value = parse_cancel_order(message),
    };
  }
  if (message.type == "MarkPriceUpdated") {
    return ParsedEngineInput{
        .kind = ParsedEngineInputKind::MarkPriceUpdated,
        .value = parse_mark_price_updated(message),
    };
  }
  if (message.type == "FundingRateUpdated") {
    return ParsedEngineInput{
        .kind = ParsedEngineInputKind::FundingRateUpdated,
        .value = parse_funding_rate_updated(message),
    };
  }

  throw parser_error("unsupported engine input type '" + message.type + "'");
}

cex::adapter::PlaceOrderInput EngineInputParser::parse_place_order(
    const protocol::ProtocolMessage& message) const {
  if (message.type != "PlaceOrder") {
    throw parser_error("expected PlaceOrder message");
  }

  const auto& payload = message.payload;
  return cex::adapter::PlaceOrderInput{
      .input_id = optional_payload_string(message, "input_id"),
      .envelope = require_envelope(payload),
      .order_id = require_i64(payload, "order_id", "payload"),
      .reservation_id = require_payload_string(message, "reservation_id"),
      .market_id = require_i64(payload, "market_id", "payload"),
      .market_name = require_payload_string(message, "market_name"),
      .side = parse_side(require_payload_string(message, "side")),
      .order_type =
          parse_order_type(require_payload_string(message, "order_type")),
      .time_in_force = optional_time_in_force(payload),
      .price = require_i64(payload, "price", "payload"),
      .quantity = require_i64(payload, "quantity", "payload"),
      .reduce_only = require_bool(payload, "reduce_only", "payload"),
      .margin_asset = require_payload_string(message, "margin_asset"),
      .reserved_margin_amount =
          require_i64(payload, "reserved_margin_amount", "payload"),
      .leverage = require_i32(payload, "leverage", "payload"),
      .source = std::nullopt,
  };
}

cex::adapter::CancelOrderInput EngineInputParser::parse_cancel_order(
    const protocol::ProtocolMessage& message) const {
  if (message.type != "CancelOrder") {
    throw parser_error("expected CancelOrder message");
  }

  const auto& payload = message.payload;
  return cex::adapter::CancelOrderInput{
      .input_id = optional_payload_string(message, "input_id"),
      .envelope = require_envelope(payload),
      .market_id = require_i64(payload, "market_id", "payload"),
      .order_id = require_i64(payload, "order_id", "payload"),
      .source = std::nullopt,
  };
}

cex::adapter::MarkPriceUpdatedInput
EngineInputParser::parse_mark_price_updated(
    const protocol::ProtocolMessage& message) const {
  if (message.type != "MarkPriceUpdated") {
    throw parser_error("expected MarkPriceUpdated message");
  }

  const auto& payload = message.payload;
  return cex::adapter::MarkPriceUpdatedInput{
      .input_id = optional_payload_string(message, "input_id"),
      .market_id = require_i64(payload, "market_id", "payload"),
      .mark_price = require_i64(payload, "mark_price", "payload"),
      .index_price = require_i64(payload, "index_price", "payload"),
      .source_timestamp_ms =
          require_i64(payload, "source_timestamp_ms", "payload"),
      .published_at_ms = require_i64(payload, "published_at_ms", "payload"),
      .valid_until_ms = require_i64(payload, "valid_until_ms", "payload"),
      .source_sequence = require_i64(payload, "source_sequence", "payload"),
      .source_status = require_payload_string(message, "source_status"),
      .source = std::nullopt,
  };
}

cex::adapter::FundingRateUpdatedInput
EngineInputParser::parse_funding_rate_updated(
    const protocol::ProtocolMessage& message) const {
  if (message.type != "FundingRateUpdated") {
    throw parser_error("expected FundingRateUpdated message");
  }

  const auto& payload = message.payload;
  auto input = cex::adapter::FundingRateUpdatedInput{
      .input_id = optional_payload_string(message, "input_id"),
      .market_id = require_i64(payload, "market_id", "payload"),
      .funding_interval_id =
          require_payload_string(message, "funding_interval_id"),
      .rate = require_i64(payload, "rate", "payload"),
      .rate_scale = require_i64(payload, "rate_scale", "payload"),
      .interval_start_ms =
          require_i64(payload, "interval_start_ms", "payload"),
      .interval_end_ms = require_i64(payload, "interval_end_ms", "payload"),
      .source_timestamp_ms =
          require_i64(payload, "source_timestamp_ms", "payload"),
      .source = std::nullopt,
  };

  if (input.rate_scale <= 0) {
    throw parser_error("payload.rate_scale must be greater than zero");
  }
  if (input.interval_end_ms <= input.interval_start_ms) {
    throw parser_error(
        "payload.interval_end_ms must be greater than interval_start_ms");
  }

  return input;
}

}  // namespace cex::runtime
