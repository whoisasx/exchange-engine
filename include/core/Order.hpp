#pragma once
#include"Types.hpp"
#include"FixedPoint.hpp"
struct PriceLevel;

struct Order{
  OrderId orderId;
  ClientOrderId clientOrderId;
  UserId userId;
  SymbolId symbolId;
  Side side;
  Price price;
  Quantity originalQuantity;
  Quantity remainingQuantity;
  EngineSequence sequenceAccepted;
  Order* previousOrder=nullptr;
  Order* nextOrder=nullptr;
  PriceLevel* priceLevel=nullptr;

  [[nodiscard]] constexpr bool is_filled() const {
    return remainingQuantity.is_zero();
  }
  constexpr void reduce(Quantity quantity){
    remainingQuantity.subtract_lots(quantity.lots());
  }
  constexpr void detach_links(){
    nextOrder=nullptr;
    previousOrder=nullptr;
    priceLevel=nullptr;
  }
};
