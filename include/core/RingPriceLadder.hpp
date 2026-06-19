#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "FixedPoint.hpp"
#include "PriceLevel.hpp"
#include "Types.hpp"

struct RingPriceLadder{
  Side side;
  int64_t tickSizeTicks;
  int64_t baseTick;
  uint64_t headIndex;
  uint64_t capacity;

  std::vector<std::unique_ptr<PriceLevel>> slots;
  std::vector<uint64_t> activeSlotBitmap;

  RingPriceLadder(Side side, int64_t tickSizeTicks, int64_t baseTick, uint64_t capacity);

  [[nodiscard]] constexpr bool contains_tick(int64_t priceTick) const {
    if(capacity==0) return false;
    if(priceTick<baseTick) return false;
    const auto offset=static_cast<uint64_t>(priceTick-baseTick);
    return offset<capacity;
  }
  [[nodiscard]] constexpr uint64_t price_tick_to_slot(int64_t priceTick) const {
    const auto offset=static_cast<uint64_t>(priceTick-baseTick);
    return (headIndex+offset)%capacity;
  }

  PriceLevel* get_level(Price price);
  PriceLevel* get_or_create_level(Price price);
  void remove_level_if_empty(Price price);
  PriceLevel* best_level();
  PriceLevel* advance_best_after_empty();
  bool should_recenter(int64_t referenceTick) const;
  void recenter_around(int64_t referenceTick);
  void clear_slot(uint64_t slotIndex);

  void mark_active(uint64_t slotIndex);
  void mark_inactive(uint64_t slotIndex);
  [[nodiscard]] bool is_active(uint64_t slotIndex) const;
};
