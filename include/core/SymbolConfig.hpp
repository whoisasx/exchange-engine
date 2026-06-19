#pragma once

#include <cstdint>

#include "Types.hpp"
#include "FixedPoint.hpp"

struct SymbolConfig{
  SymbolId symbolId;
  AssetId baseAssetId;
  AssetId quoteAssetId;
  Price tickSize;
  Quantity lotSize;
  Quantity minQuantity;
  Quantity maxQuantity;
  Price minPrice;
  Price maxPrice;

  uint64_t ringCapacityTicks;
  uint64_t thresholdPercentage;
  int64_t initialBaseTick;
  uint8_t priceScale;
  uint8_t quantityScale;

  FeeRate makerFeeRate;
  FeeRate takerFeeRate;

  bool tradingEnabled;
};

//helper functions
[[nodiscard]] constexpr bool validate_symbol_price(Price price, const SymbolConfig& config){
  return price.is_valid() && config.tickSize.ticks()>0 && price>=config.minPrice && price<=config.maxPrice && price.ticks() % config.tickSize.ticks()==0;
}
[[nodiscard]] constexpr bool validate_symbol_quantity(Quantity quantity, const SymbolConfig& config){
  return quantity.is_valid() && config.lotSize.lots()>0 && quantity>=config.minQuantity && quantity<=config.maxQuantity && quantity.lots() % config.lotSize.lots()==0;
}
[[nodiscard]] constexpr int64_t price_to_tick(Price price, const SymbolConfig& config){
  return price.ticks() / config.tickSize.ticks();
}
[[nodiscard]] constexpr uint64_t quantity_to_lot(Quantity quantity, const SymbolConfig& config){
  return quantity.lots() / config.lotSize.lots();
}
