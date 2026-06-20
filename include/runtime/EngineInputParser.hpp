#pragma once

#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>

#include "adapter/EngineAdapter.hpp"
#include "protocol/Message.hpp"

namespace cex::runtime {

enum class ParsedEngineInputKind {
  PlaceOrder,
  CancelOrder,
  MarkPriceUpdated,
};

using ParsedEngineInputValue =
    std::variant<cex::adapter::PlaceOrderInput,
                 cex::adapter::CancelOrderInput,
                 cex::adapter::MarkPriceUpdatedInput>;

struct ParsedEngineInput {
  ParsedEngineInputKind kind{ParsedEngineInputKind::PlaceOrder};
  ParsedEngineInputValue value;
};

class EngineInputParserError : public std::runtime_error {
 public:
  explicit EngineInputParserError(std::string message);
};

class EngineInputParser {
 public:
  [[nodiscard]] ParsedEngineInput parse(std::string_view raw_json) const;

  [[nodiscard]] cex::adapter::PlaceOrderInput parse_place_order(
      const protocol::ProtocolMessage& message) const;
  [[nodiscard]] cex::adapter::CancelOrderInput parse_cancel_order(
      const protocol::ProtocolMessage& message) const;
  [[nodiscard]] cex::adapter::MarkPriceUpdatedInput parse_mark_price_updated(
      const protocol::ProtocolMessage& message) const;
};

}  // namespace cex::runtime
