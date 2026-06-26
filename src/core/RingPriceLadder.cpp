#include "RingPriceLadder.hpp"
#include <bit>
#include <memory>

namespace {
constexpr uint64_t kBitmapWordBits = 64;
[[nodiscard]] uint64_t bitmap_word_count(uint64_t capacity) {
  return (capacity + kBitmapWordBits - 1) / kBitmapWordBits;
}
[[nodiscard]] int64_t price_to_ladder_tick(Price price, int64_t tickSizeTicks) {
  if (tickSizeTicks <= 0) {
    return price.ticks();
  }
  return price.ticks() / tickSizeTicks;
}

[[nodiscard]] uint64_t bits_through(uint64_t bitOffset) {
  if (bitOffset == kBitmapWordBits - 1) {
    return ~0ULL;
  }
  return (1ULL << (bitOffset + 1)) - 1;
}

[[nodiscard]] PriceLevel* first_active_level_in_range(
    RingPriceLadder& ladder,
    uint64_t firstSlot,
    uint64_t lastSlot
) {
  if (firstSlot > lastSlot || ladder.activeSlotBitmap.empty()) {
    return nullptr;
  }

  const auto firstWord = firstSlot / kBitmapWordBits;
  const auto lastWord = lastSlot / kBitmapWordBits;

  for (uint64_t wordIndex = firstWord; wordIndex <= lastWord; ++wordIndex) {
    uint64_t bits = ladder.activeSlotBitmap[wordIndex];

    if (wordIndex == firstWord) {
      bits &= (~0ULL << (firstSlot % kBitmapWordBits));
    }
    if (wordIndex == lastWord) {
      bits &= bits_through(lastSlot % kBitmapWordBits);
    }

    while (bits != 0) {
      const auto bitOffset = static_cast<uint64_t>(std::countr_zero(bits));
      const auto slotIndex = (wordIndex * kBitmapWordBits) + bitOffset;

      if (slotIndex < ladder.capacity) {
        if (PriceLevel* level = ladder.slots[slotIndex].get()) {
          return level;
        }
        ladder.mark_inactive(slotIndex);
      }

      bits &= bits - 1;
    }
  }

  return nullptr;
}

[[nodiscard]] PriceLevel* last_active_level_in_range(
    RingPriceLadder& ladder,
    uint64_t firstSlot,
    uint64_t lastSlot
) {
  if (firstSlot > lastSlot || ladder.activeSlotBitmap.empty()) {
    return nullptr;
  }

  const auto firstWord = firstSlot / kBitmapWordBits;
  const auto lastWord = lastSlot / kBitmapWordBits;

  for (uint64_t wordIndex = lastWord;; --wordIndex) {
    uint64_t bits = ladder.activeSlotBitmap[wordIndex];

    if (wordIndex == firstWord) {
      bits &= (~0ULL << (firstSlot % kBitmapWordBits));
    }
    if (wordIndex == lastWord) {
      bits &= bits_through(lastSlot % kBitmapWordBits);
    }

    while (bits != 0) {
      const auto bitOffset = static_cast<uint64_t>(
          (kBitmapWordBits - 1) - std::countl_zero(bits)
      );
      const auto slotIndex = (wordIndex * kBitmapWordBits) + bitOffset;

      if (slotIndex < ladder.capacity) {
        if (PriceLevel* level = ladder.slots[slotIndex].get()) {
          return level;
        }
        ladder.mark_inactive(slotIndex);
      }

      bits &= ~(1ULL << bitOffset);
    }

    if (wordIndex == firstWord) {
      break;
    }
  }

  return nullptr;
}
}

RingPriceLadder::RingPriceLadder(Side side, int64_t tickSizeTicks, int64_t baseTick, uint64_t capacity):side(side), tickSizeTicks(tickSizeTicks),baseTick(baseTick), headIndex(0), capacity(capacity), slots(capacity), activeSlotBitmap(bitmap_word_count(capacity)){}

PriceLevel* RingPriceLadder::get_level(Price price) {
  if (capacity == 0) {
    return nullptr;
  } 
  const auto priceTick = price_to_ladder_tick(price, tickSizeTicks);
  if (!contains_tick(priceTick)) {
    return nullptr;
  }
  const auto slotIndex = price_tick_to_slot(priceTick);
  return slots[slotIndex].get();
}

PriceLevel* RingPriceLadder::get_or_create_level(Price price) {
  if (capacity == 0) {
    return nullptr;
  }
  const auto priceTick = price_to_ladder_tick(price, tickSizeTicks);
  if (!contains_tick(priceTick)) {
    return nullptr;
  }
  const auto slotIndex = price_tick_to_slot(priceTick);
  if (!slots[slotIndex]) {
    slots[slotIndex] = std::make_unique<PriceLevel>(price);
  }
  mark_active(slotIndex);
  return slots[slotIndex].get();
}

void RingPriceLadder::remove_level_if_empty(Price price) {
  if (capacity == 0) {
    return;
  }
  const auto priceTick = price_to_ladder_tick(price, tickSizeTicks);
  if (!contains_tick(priceTick)) {
    return;
  }
  const auto slotIndex = price_tick_to_slot(priceTick);
  PriceLevel* level = slots[slotIndex].get();
  if (!level) {
    mark_inactive(slotIndex);
    return;
  }
  if (level->empty() || level->totalQuantity.is_zero()) {
    clear_slot(slotIndex);
  }
}

PriceLevel* RingPriceLadder::best_level() {
  return advance_best_after_empty();
}

PriceLevel* RingPriceLadder::advance_best_after_empty() {
  if (capacity == 0) {
    return nullptr;
  }

  if (side == Buy) {
    if (headIndex > 0) {
      if (PriceLevel* level = last_active_level_in_range(*this, 0, headIndex - 1)) {
        return level;
      }
    }

    return last_active_level_in_range(*this, headIndex, capacity - 1);
  }

  if (PriceLevel* level = first_active_level_in_range(*this, headIndex, capacity - 1)) {
    return level;
  }

  if (headIndex > 0) {
    return first_active_level_in_range(*this, 0, headIndex - 1);
  }

  return nullptr;
}

void RingPriceLadder::clear_slot(uint64_t slotIndex) {
  if (slotIndex >= capacity) {
    return;
  }
  slots[slotIndex].reset();
  mark_inactive(slotIndex);
}
void RingPriceLadder::mark_active(uint64_t slotIndex) {
  if (slotIndex >= capacity) {
    return;
  }
  const auto wordIndex = slotIndex / kBitmapWordBits;
  const auto bitOffset = slotIndex % kBitmapWordBits;
  activeSlotBitmap[wordIndex] |= (1ULL << bitOffset);
}
void RingPriceLadder::mark_inactive(uint64_t slotIndex) {
  if (slotIndex >= capacity) {
    return;
  }
  const auto wordIndex = slotIndex / kBitmapWordBits;
  const auto bitOffset = slotIndex % kBitmapWordBits;
  activeSlotBitmap[wordIndex] &= ~(1ULL << bitOffset);
}
