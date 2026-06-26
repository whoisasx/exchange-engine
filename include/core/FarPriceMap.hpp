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
  void remove_level_if_empty(Price price);
  [[nodiscard]] PriceLevel* best_level();
};
