#pragma once

#include <map>
#include <memory>

#include "FixedPoint.hpp"
#include "PriceLevel.hpp"
#include "Types.hpp"

struct FarPriceMap{
  Side side;
  std::map<Price, std::unique_ptr<PriceLevel>> levels;

  explicit FarPriceMap(Side side);

  PriceLevel* get_or_create_level(Price price);
  PriceLevel* get_level(Price price) const;
  void remove_level_if_empty(Price price);
  [[nodiscard]] PriceLevel* best_level();
  [[nodiscard]] bool empty() const;
};
