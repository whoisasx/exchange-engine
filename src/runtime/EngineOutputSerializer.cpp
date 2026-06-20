#include "runtime/EngineOutputSerializer.hpp"

#include <algorithm>
#include <array>
#include <ostream>
#include <sstream>
#include <string_view>
#include <vector>

namespace cex::runtime {
namespace {

void append_json_string(std::ostream& output, std::string_view value) {
  output << '"';
  for (const unsigned char ch : value) {
    switch (ch) {
      case '"':
        output << "\\\"";
        break;
      case '\\':
        output << "\\\\";
        break;
      case '\b':
        output << "\\b";
        break;
      case '\f':
        output << "\\f";
        break;
      case '\n':
        output << "\\n";
        break;
      case '\r':
        output << "\\r";
        break;
      case '\t':
        output << "\\t";
        break;
      default:
        if (ch < 0x20U) {
          constexpr char hex[] = "0123456789abcdef";
          output << "\\u00" << hex[ch >> 4U] << hex[ch & 0x0FU];
          break;
        }
        output << static_cast<char>(ch);
        break;
    }
  }
  output << '"';
}

[[nodiscard]] bool is_integer_payload_field(std::string_view field) {
  static constexpr std::array<std::string_view, 33> fields{
      "account_id",
      "amount",
      "base_asset_id",
      "credit_amount",
      "debit_amount",
      "engine_sequence",
      "engine_timestamp_ms",
      "fill_id",
      "index_price",
      "leverage",
      "maker_order_id",
      "maker_user_id",
      "market_id",
      "order_id",
      "position_id",
      "price",
      "quantity",
      "quote_asset_id",
      "released_amount",
      "released_quantity",
      "source_input_offset",
      "source_sequence",
      "source_timestamp_ms",
      "taker_order_id",
      "taker_user_id",
      "trade_id",
      "user_id",
      "wallet_id",
      "entry_price",
      "exit_price",
      "mark_price",
      "published_at_ms",
      "valid_until_ms",
  };

  for (const auto candidate : fields) {
    if (candidate == field) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] bool is_json_integer(std::string_view value) {
  if (value.empty()) {
    return false;
  }

  std::size_t offset = 0;
  if (value.front() == '-') {
    if (value.size() == 1) {
      return false;
    }
    offset = 1;
  }

  if (value[offset] == '0' && value.size() - offset > 1) {
    return false;
  }

  for (std::size_t i = offset; i < value.size(); ++i) {
    if (value[i] < '0' || value[i] > '9') {
      return false;
    }
  }

  return true;
}

void append_payload_value(std::ostream& output,
                          std::string_view field,
                          std::string_view value) {
  if (is_integer_payload_field(field) && is_json_integer(value)) {
    output << value;
    return;
  }
  append_json_string(output, value);
}

}  // namespace

std::string serialize_engine_output_record(const EngineOutputRecord& record) {
  std::ostringstream output;
  output << "{\"type\":";
  append_json_string(output, record.type);
  output << ",\"payload\":{";

  std::vector<const PayloadFields::value_type*> fields;
  fields.reserve(record.payload.size());
  for (const auto& field : record.payload) {
    fields.push_back(&field);
  }
  std::sort(fields.begin(), fields.end(), [](const auto* lhs, const auto* rhs) {
    return lhs->first < rhs->first;
  });

  bool first = true;
  for (const auto* field_entry : fields) {
    const auto& [field, value] = *field_entry;
    if (!first) {
      output << ',';
    }
    first = false;
    append_json_string(output, field);
    output << ':';
    append_payload_value(output, field, value);
  }

  output << "}}";
  return output.str();
}

}  // namespace cex::runtime
