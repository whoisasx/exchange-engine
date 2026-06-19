#include"OrderBook.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <vector>

#include "SequenceGenerator.hpp"
#include "Validation.hpp"

namespace {
[[nodiscard]] std::optional<OrderId> optional_order_id(OrderId orderId){
  if(orderId==0){
    return std::nullopt;
  }
  return orderId;
}

[[nodiscard]] EngineEvent make_rejection(
    SequenceGenerator& sequenceGenerator,
    CommandId commandId,
    std::optional<OrderId> orderId,
    UserId userId,
    SymbolId symbolId,
    RejectReason reason
){
  return OrderRejected{
    sequenceGenerator.next_event_id(),
    sequenceGenerator.next_sequence(),
    commandId,
    orderId,
    userId,
    symbolId,
    reason,
  };
}

[[nodiscard]] Quantity filled_quantity(const Order& order){
  return Quantity::from_lots(order.originalQuantity.lots()-order.remainingQuantity.lots());
}

[[nodiscard]] SideBook& book_for_side(OrderBook& orderBook, Side side){
  return side==Buy ? orderBook.bids : orderBook.asks;
}

[[nodiscard]] SideBook& opposing_book(OrderBook& orderBook, Side incomingSide){
  return incomingSide==Buy ? orderBook.asks : orderBook.bids;
}

[[nodiscard]] bool crosses_level(Side restingSide, Price incomingPrice, Price restingPrice){
  if(restingSide==Sell){
    return incomingPrice>=restingPrice;
  }
  return incomingPrice<=restingPrice;
}

[[nodiscard]] std::vector<const PriceLevel*> active_levels_sorted(const SideBook& sideBook){
  std::vector<const PriceLevel*> levels;

  for(const auto& slot : sideBook.ringPriceLadder.slots){
    if(slot && !slot->empty()){
      levels.push_back(slot.get());
    }
  }
  for(const auto& entry : sideBook.farPriceMap.levels){
    if(entry.second && !entry.second->empty()){
      levels.push_back(entry.second.get());
    }
  }

  std::sort(levels.begin(), levels.end(), [&sideBook](const PriceLevel* left, const PriceLevel* right){
    if(sideBook.side==Buy){
      return left->price>right->price;
    }
    return left->price<right->price;
  });

  return levels;
}

[[nodiscard]] uint64_t available_crossing_lots(const SideBook& sideBook, Price incomingPrice, uint64_t stopAtLots){
  uint64_t total=0;
  for(const auto* level : active_levels_sorted(sideBook)){
    if(!crosses_level(sideBook.side, incomingPrice, level->price)){
      break;
    }
    const auto lots=level->totalQuantity.lots();
    if(std::numeric_limits<uint64_t>::max()-total<lots){
      return std::numeric_limits<uint64_t>::max();
    }
    total+=lots;
    if(total>=stopAtLots){
      break;
    }
  }
  return total;
}

[[nodiscard]] Price matching_price_for_command(const PlaceOrderCommand& command){
  if(command.orderType==Market){
    return command.side==Buy ? Price::max() : Price::min();
  }
  return command.price;
}
}

OrderBook::OrderBook(SymbolConfig config)
  : symbolConfig(config),
    bids(Buy, config.tickSize.ticks(), config.initialBaseTick, config.ringCapacityTicks),
    asks(Sell, config.tickSize.ticks(), config.initialBaseTick, config.ringCapacityTicks){}

std::vector<EngineEvent> OrderBook::process_place(const PlaceOrderCommand& command, SequenceGenerator& sequenceGenerator){
  if(has_order(command.orderId) || clientOrderIdToOrderId.find(command.clientOrderId)!=clientOrderIdToOrderId.end()){
    return {make_rejection(sequenceGenerator, command.commandId, optional_order_id(command.orderId), command.userId, command.symbolId, DuplicateOrder)};
  }
  if(!symbolConfig.tradingEnabled){
    return {make_rejection(sequenceGenerator, command.commandId, optional_order_id(command.orderId), command.userId, command.symbolId, MarketClosed)};
  }
  if(!validate_symbol_quantity(command.quantity, symbolConfig)){
    return {make_rejection(sequenceGenerator, command.commandId, optional_order_id(command.orderId), command.userId, command.symbolId, InvalidQuantity)};
  }
  if(command.orderType==Limit && !validate_symbol_price(command.price, symbolConfig)){
    return {make_rejection(sequenceGenerator, command.commandId, optional_order_id(command.orderId), command.userId, command.symbolId, InvalidPrice)};
  }

  const auto matchPrice=matching_price_for_command(command);
  auto& opposite=opposing_book(*this, command.side);

  if(command.timeInForce==PO && opposite.price_crosses(command.price)){
    return {make_rejection(sequenceGenerator, command.commandId, optional_order_id(command.orderId), command.userId, command.symbolId, InvalidPrice)};
  }

  if(command.timeInForce==FOK){
    const auto availableLots=available_crossing_lots(opposite, matchPrice, command.quantity.lots());
    if(availableLots<command.quantity.lots()){
      return {make_rejection(sequenceGenerator, command.commandId, optional_order_id(command.orderId), command.userId, command.symbolId, InsufficientBalanceLiquidity)};
    }
  }

  std::vector<EngineEvent> events;
  const auto acceptedSequence=sequenceGenerator.next_sequence();
  events.push_back(OrderAccepted{
    sequenceGenerator.next_event_id(),
    acceptedSequence,
    command.orderId,
    command.clientOrderId,
    command.userId,
    command.symbolId,
    command.side,
    command.price,
    command.quantity,
  });

  Order incoming{};
  incoming.orderId=command.orderId;
  incoming.clientOrderId=command.clientOrderId;
  incoming.userId=command.userId;
  incoming.symbolId=command.symbolId;
  incoming.side=command.side;
  incoming.price=matchPrice;
  incoming.originalQuantity=command.quantity;
  incoming.remainingQuantity=command.quantity;
  incoming.sequenceAccepted=acceptedSequence;

  auto matchEvents=match_incoming_order(incoming, sequenceGenerator);
  events.insert(events.end(), matchEvents.begin(), matchEvents.end());

  const bool canRest=command.orderType==Limit && (command.timeInForce==GTC || command.timeInForce==PO);
  if(canRest && !incoming.remainingQuantity.is_zero()){
    auto* restingOrder=new Order{};
    *restingOrder=incoming;
    restingOrder->price=command.price;
    restingOrder->detach_links();

    insert_remaining_as_resting(restingOrder);

    events.push_back(OrderBookDelta{
      sequenceGenerator.next_event_id(),
      sequenceGenerator.next_sequence(),
      command.symbolId,
      command.side,
      restingOrder->price,
      restingOrder->priceLevel->totalQuantity,
    });
  }

  return events;
}

std::vector<EngineEvent> OrderBook::process_cancel(const CancelOrderCommand& command, SequenceGenerator& sequenceGenerator){
  auto targetOrderId=command.orderId;
  if(targetOrderId==0){
    const auto it=clientOrderIdToOrderId.find(command.clientOrderId);
    if(it!=clientOrderIdToOrderId.end()){
      targetOrderId=it->second;
    }
  }

  auto* order=find_order(targetOrderId);
  if(order==nullptr || order->userId!=command.userId){
    return {make_rejection(sequenceGenerator, command.commandId, optional_order_id(targetOrderId), command.userId, command.symbolId, OrderNotFound)};
  }

  return cancel_existing_order(targetOrderId, sequenceGenerator);
}

std::vector<EngineEvent> OrderBook::match_incoming_order(Order& incomingOrder, SequenceGenerator& sequenceGenerator){
  std::vector<EngineEvent> events;
  auto& opposite=opposing_book(*this, incomingOrder.side);

  while(!incomingOrder.remainingQuantity.is_zero()){
    auto* maker=opposite.best_order();
    if(maker==nullptr || !opposite.price_crosses(incomingOrder.price)){
      break;
    }

    auto* level=maker->priceLevel;
    const auto fillQuantity=Quantity::min(incomingOrder.remainingQuantity, maker->remainingQuantity);
    const auto executionPrice=maker->price;
    const auto makerSide=maker->side;
    const auto makerOrderId=maker->orderId;
    const auto makerClientOrderId=maker->clientOrderId;

    maker->reduce(fillQuantity);
    incomingOrder.reduce(fillQuantity);
    if(level!=nullptr){
      level->decrease_total(fillQuantity);
    }

    lastTradePrice=executionPrice;
    const auto totalAtPrice=level==nullptr ? Quantity{} : level->totalQuantity;

    events.push_back(TradeExecuted{
      sequenceGenerator.next_event_id(),
      sequenceGenerator.next_sequence(),
      sequenceGenerator.next_trade_id(),
      symbolConfig.symbolId,
      makerOrderId,
      incomingOrder.orderId,
      executionPrice,
      fillQuantity,
    });

    if(maker->is_filled()){
      orderById.erase(makerOrderId);
      clientOrderIdToOrderId.erase(makerClientOrderId);
      opposite.remove_order(maker);
      delete maker;

      events.push_back(OrderFilled{
        sequenceGenerator.next_event_id(),
        sequenceGenerator.next_sequence(),
        makerOrderId,
      });
    }
    else{
      opposite.update_after_fill_or_cancel(level);
      events.push_back(OrderPartial{
        sequenceGenerator.next_event_id(),
        sequenceGenerator.next_sequence(),
        makerOrderId,
        filled_quantity(*maker),
        maker->remainingQuantity,
      });
    }

    if(incomingOrder.is_filled()){
      events.push_back(OrderFilled{
        sequenceGenerator.next_event_id(),
        sequenceGenerator.next_sequence(),
        incomingOrder.orderId,
      });
    }
    else{
      events.push_back(OrderPartial{
        sequenceGenerator.next_event_id(),
        sequenceGenerator.next_sequence(),
        incomingOrder.orderId,
        filled_quantity(incomingOrder),
        incomingOrder.remainingQuantity,
      });
    }

    events.push_back(OrderBookDelta{
      sequenceGenerator.next_event_id(),
      sequenceGenerator.next_sequence(),
      symbolConfig.symbolId,
      makerSide,
      executionPrice,
      totalAtPrice,
    });
  }

  return events;
}

void OrderBook::insert_remaining_as_resting(Order* order){
  if(order==nullptr){
    return;
  }
  orderById[order->orderId]=order;
  clientOrderIdToOrderId[order->clientOrderId]=order->orderId;
  book_for_side(*this, order->side).insert_resting_order(order);
}

std::vector<EngineEvent> OrderBook::cancel_existing_order(OrderId orderId, SequenceGenerator& sequenceGenerator){
  auto* order=find_order(orderId);
  if(order==nullptr){
    return {make_rejection(sequenceGenerator, 0, optional_order_id(orderId), 0, symbolConfig.symbolId, OrderNotFound)};
  }

  auto& sideBook=book_for_side(*this, order->side);
  const auto side=order->side;
  const auto price=order->price;
  const auto releasedQuantity=order->remainingQuantity;
  auto* level=order->priceLevel;

  Quantity totalAfterCancel{};
  if(level!=nullptr && level->totalQuantity.lots()>releasedQuantity.lots()){
    totalAfterCancel=Quantity::from_lots(level->totalQuantity.lots()-releasedQuantity.lots());
  }

  orderById.erase(order->orderId);
  clientOrderIdToOrderId.erase(order->clientOrderId);
  sideBook.remove_order(order);
  delete order;

  return {
    OrderCancelled{
      sequenceGenerator.next_event_id(),
      sequenceGenerator.next_sequence(),
      orderId,
      releasedQuantity,
    },
    OrderBookDelta{
      sequenceGenerator.next_event_id(),
      sequenceGenerator.next_sequence(),
      symbolConfig.symbolId,
      side,
      price,
      totalAfterCancel,
    },
  };
}

Order* OrderBook::find_order(OrderId orderId) const{
  const auto it=orderById.find(orderId);
  if(it==orderById.end()){
    return nullptr;
  }
  return it->second;
}

bool OrderBook::has_order(OrderId orderId) const{
  return find_order(orderId)!=nullptr;
}

PriceLevel* OrderBook::best_bid() const{
  return bids.best_level();
}

PriceLevel* OrderBook::best_ask() const{
  return asks.best_level();
}
