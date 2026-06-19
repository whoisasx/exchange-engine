#include "Validation.hpp"

namespace {
[[nodiscard]] ValidationResult valid(){
  return ValidationResult{true, std::nullopt};
}

[[nodiscard]] ValidationResult invalid(RejectReason reason){
  return ValidationResult{false, reason};
}
}

ValidationResult validate_place_order(
    const PlaceOrderCommand& command,
    const SymbolConfig& symbolConfig,
    const IdempotencyIndex& idempotencyIndex
){
  if(idempotencyIndex.has_command(command.commandId)){
    return invalid(DuplicateCommand);
  }
  if(idempotencyIndex.has_order_id(command.orderId) || idempotencyIndex.has_client_order_id(command.clientOrderId)){
    return invalid(DuplicateOrder);
  }
  if(!symbolConfig.tradingEnabled){
    return invalid(MarketClosed);
  }

  const auto quantityResult=validate_quantity(command.quantity, symbolConfig);
  if(!quantityResult.valid){
    return quantityResult;
  }

  if(command.orderType==Limit){
    const auto priceResult=validate_price(command.price, symbolConfig);
    if(!priceResult.valid){
      return priceResult;
    }
  }

  return valid();
}

ValidationResult validate_cancel_order(const CancelOrderCommand&, const SymbolConfig&){
  return valid();
}

ValidationResult validate_price(Price price, const SymbolConfig& symbolConfig){
  if(!validate_symbol_price(price, symbolConfig)){
    return invalid(InvalidPrice);
  }
  return valid();
}

ValidationResult validate_quantity(Quantity quantity, const SymbolConfig& symbolConfig){
  if(!validate_symbol_quantity(quantity, symbolConfig)){
    return invalid(InvalidQuantity);
  }
  return valid();
}
