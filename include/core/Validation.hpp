#pragma once

#include <optional>

#include "Command.hpp"
#include "FixedPoint.hpp"
#include "IdempotencyIndex.hpp"
#include "SymbolConfig.hpp"
#include "Types.hpp"

struct ValidationResult{
  bool valid;
  std::optional<RejectReason> reason;
};

ValidationResult validate_place_order(const PlaceOrderCommand& command, const SymbolConfig& symbolConfig, const IdempotencyIndex& idempotencyIndex);
ValidationResult validate_cancel_order(const CancelOrderCommand& command, const SymbolConfig& symbolConfig);
ValidationResult validate_price(Price price, const SymbolConfig& symbolConfig);
ValidationResult validate_quantity(Quantity quantity, const SymbolConfig& symbolConfig);
