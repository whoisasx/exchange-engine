#pragma once
#include "FixedPoint.hpp"
#include<cstdint>
struct Order;

struct PriceLevel{
  Price price;
  Quantity totalQuantity;
  Order* head=nullptr;
  Order* tail=nullptr;
  uint64_t orderCount=0;

  explicit PriceLevel(Price price):  price(price), totalQuantity(Quantity()){}

  [[nodiscard]] constexpr bool empty() const {
    return head==nullptr;
  }
  [[nodiscard]] Order* front() const {
    return head;
  }

  void push_back(Order*);
  Order* pop_front();

  void increase_total(Quantity quantity);
  void decrease_total(Quantity quantity);

  void clear();
};