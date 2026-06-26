#pragma once

#include <cstdint>

#include "FarPriceMap.hpp"
#include "FixedPoint.hpp"
#include "RingPriceLadder.hpp"
#include "Types.hpp"

struct PriceLevel;
struct Order;

struct SideBook{
  Side side;
  RingPriceLadder ringPriceLadder;
  FarPriceMap farPriceMap;
  PriceLevel* cachedBestLevel=nullptr;

  SideBook(Side side, int64_t tickSizeTicks, int64_t baseTick, uint64_t capacity);

  void insert_resting_order(Order* order);
  [[nodiscard]] PriceLevel* best_level() const;
  [[nodiscard]] Order* best_order() const;
  void remove_order(Order* order);
  void update_after_fill_or_cancel(PriceLevel* priceLevel);
  [[nodiscard]] bool price_crosses(Price incomingPrice) const;
  PriceLevel* get_or_create_level_for_price(Price price);
  void refresh_cached_best();
};
