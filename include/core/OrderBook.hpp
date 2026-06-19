#pragma once

#include <optional>
#include <unordered_map>
#include <vector>

#include "Command.hpp"
#include "Event.hpp"
#include "Order.hpp"
#include "SideBook.hpp"
#include "SymbolConfig.hpp"
#include "Types.hpp"

struct SequenceGenerator;

struct OrderBook{
  SymbolConfig symbolConfig;
  SideBook bids;
  SideBook asks;
  std::unordered_map<OrderId,Order*> orderById;
  std::unordered_map<ClientOrderId,OrderId> clientOrderIdToOrderId;
  std::optional<Price> lastTradePrice;

  explicit OrderBook(SymbolConfig config);

  std::vector<EngineEvent> process_place(const PlaceOrderCommand& command, SequenceGenerator& sequenceGenerator);
  std::vector<EngineEvent> process_cancel(const CancelOrderCommand& command, SequenceGenerator& sequenceGenerator);
  std::vector<EngineEvent> match_incoming_order(Order& incomingOrder, SequenceGenerator& sequenceGenerator);
  void insert_remaining_as_resting(Order* order);
  std::vector<EngineEvent> cancel_existing_order(OrderId orderId, SequenceGenerator& sequenceGenerator);

  [[nodiscard]] Order* find_order(OrderId orderId) const;
  [[nodiscard]] bool has_order(OrderId orderId) const;
  [[nodiscard]] PriceLevel* best_bid() const;
  [[nodiscard]] PriceLevel* best_ask() const;
};
