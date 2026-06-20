#include "protocol/Message.hpp"
#include "runtime/EngineOutputSerializer.hpp"

#include <cassert>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

using namespace cex::runtime;

namespace {

const protocol::JsonValue& require_field(const protocol::JsonValue& object,
                                         const std::string& field) {
  const auto* value = object.find(field);
  assert(value != nullptr);
  return *value;
}

void assert_number(const protocol::JsonValue& object,
                   const std::string& field,
                   const std::string& expected) {
  const auto* number = require_field(object, field).as_number();
  assert(number != nullptr);
  assert(number->text == expected);
}

void assert_string(const protocol::JsonValue& object,
                   const std::string& field,
                   const std::string& expected) {
  const auto* text = require_field(object, field).as_string();
  assert(text != nullptr);
  assert(*text == expected);
}

void test_serializes_structured_payload_values() {
  EngineOutputRecord record{
      .topic = EngineEventsTopic,
      .type = "StructuredPayloadTest",
      .key = "1",
      .partition = std::nullopt,
      .payload =
          PayloadFields{
              {"market_id", "1"},
              {"name", "SOL-PERP"},
              {"enabled", PayloadValue::boolean(true)},
              {"nothing", PayloadValue::null()},
              {"ratio", PayloadValue::number_text("12.5")},
              {"bids",
               PayloadValue::array(PayloadValue::Array{
                   PayloadValue::object(PayloadValue::Object{
                       {"price", PayloadValue::integer(100)},
                       {"quantity", PayloadValue::integer(10)},
                   }),
               })},
              {"meta",
               PayloadValue::object(PayloadValue::Object{
                   {"label", "top"},
                   {"sequence", PayloadValue::integer(7)},
               })},
          },
  };

  const protocol::ProtocolMessage message =
      protocol::parse_protocol_message(serialize_engine_output_record(record));

  assert(message.type == "StructuredPayloadTest");
  assert(message.payload.is_object());
  assert_number(message.payload, "market_id", "1");
  assert_string(message.payload, "name", "SOL-PERP");

  const auto* enabled = require_field(message.payload, "enabled").as_bool();
  assert(enabled != nullptr);
  assert(*enabled);
  assert(require_field(message.payload, "nothing").is_null());
  assert_number(message.payload, "ratio", "12.5");

  const auto* bids = require_field(message.payload, "bids").as_array();
  assert(bids != nullptr);
  assert(bids->size() == 1);
  const auto* bid = (*bids)[0].as_object();
  assert(bid != nullptr);
  assert_number((*bids)[0], "price", "100");
  assert_number((*bids)[0], "quantity", "10");

  const auto* meta = require_field(message.payload, "meta").as_object();
  assert(meta != nullptr);
  assert_string(require_field(message.payload, "meta"), "label", "top");
  assert_number(require_field(message.payload, "meta"), "sequence", "7");
}

}  // namespace

int main() {
  try {
    test_serializes_structured_payload_values();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
