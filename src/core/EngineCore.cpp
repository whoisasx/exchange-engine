#include "EngineCore.hpp"

#include <algorithm>
#include <optional>
#include <variant>

#include "Validation.hpp"

namespace {
void release_order_storage(OrderBook& orderBook){
  for(auto& entry : orderBook.orderById){
    delete entry.second;
  }
  orderBook.orderById.clear();
  orderBook.clientOrderIdToOrderId.clear();
}

[[nodiscard]] std::optional<OrderId> optional_order_id(OrderId orderId){
  if(orderId==0){
    return std::nullopt;
  }
  return orderId;
}

[[nodiscard]] EngineSequence event_sequence(const EngineEvent& event){
  return std::visit([](const auto& value) {
    return value.sequence;
  }, event);
}

[[nodiscard]] std::optional<RejectReason> rejected_reason(const std::vector<EngineEvent>& events){
  for(const auto& event : events){
    if(const auto* rejected=std::get_if<OrderRejected>(&event)){
      return rejected->reason;
    }
  }
  return std::nullopt;
}

[[nodiscard]] bool has_event_order_accepted(const std::vector<EngineEvent>& events){
  return std::any_of(events.begin(), events.end(), [](const EngineEvent& event){
    return std::holds_alternative<OrderAccepted>(event);
  });
}

[[nodiscard]] bool has_event_order_cancelled(const std::vector<EngineEvent>& events){
  return std::any_of(events.begin(), events.end(), [](const EngineEvent& event){
    return std::holds_alternative<OrderCancelled>(event);
  });
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

void append_level_snapshot(std::vector<PriceLevelSnapshot>& snapshots, const PriceLevel* level){
  if(level==nullptr || level->empty()){
    return;
  }

  PriceLevelSnapshot snapshot{};
  snapshot.price=level->price;
  snapshot.totalQuantity=level->totalQuantity;
  for(auto* order=level->head; order!=nullptr; order=order->nextOrder){
    snapshot.orderIds.push_back(order->orderId);
  }
  snapshots.push_back(std::move(snapshot));
}

void append_side_snapshots(const SideBook& sideBook, std::vector<PriceLevelSnapshot>& snapshots){
  for(const auto& slot : sideBook.ringPriceLadder.slots){
    append_level_snapshot(snapshots, slot.get());
  }
  for(const auto& entry : sideBook.farPriceMap.levels){
    append_level_snapshot(snapshots, entry.second.get());
  }

  std::sort(snapshots.begin(), snapshots.end(), [&sideBook](const PriceLevelSnapshot& left, const PriceLevelSnapshot& right){
    if(sideBook.side==Buy){
      return left.price>right.price;
    }
    return left.price<right.price;
  });
}

[[nodiscard]] OrderSnapshot snapshot_order(const Order& order){
  return OrderSnapshot{
    order.orderId,
    order.clientOrderId,
    order.userId,
    order.symbolId,
    order.side,
    order.price,
    order.originalQuantity,
    order.remainingQuantity,
    order.sequenceAccepted,
  };
}

[[nodiscard]] Order* restore_order(const OrderSnapshot& snapshot){
  auto* order=new Order{};
  order->orderId=snapshot.orderId;
  order->clientOrderId=snapshot.clientOrderId;
  order->userId=snapshot.userId;
  order->symbolId=snapshot.symbolId;
  order->side=snapshot.side;
  order->price=snapshot.price;
  order->originalQuantity=snapshot.originalQuantity;
  order->remainingQuantity=snapshot.remainingQuantity;
  order->sequenceAccepted=snapshot.sequenceAccepted;
  return order;
}

[[nodiscard]] CommandResultSummary command_summary(
    CommandId commandId,
    bool accepted,
    std::optional<OrderId> orderId,
    std::optional<RejectReason> rejectReason,
    const std::vector<EngineEvent>& events
){
  EngineSequence sequence=0;
  if(!events.empty()){
    sequence=event_sequence(events.front());
  }

  return CommandResultSummary{
    commandId,
    accepted,
    orderId,
    rejectReason,
    sequence,
  };
}
}

void EngineCore::add_symbol(SymbolConfig symbolConfig){
  if(auto it=orderBooks.find(symbolConfig.symbolId); it!=orderBooks.end()){
    release_order_storage(it->second);
    orderBooks.erase(it);
  }

  symbolConfigs.insert_or_assign(symbolConfig.symbolId, symbolConfig);
  orderBooks.emplace(symbolConfig.symbolId, OrderBook(symbolConfig));
}

std::vector<EngineEvent> EngineCore::process(const EngineCommand& command){
  if(command.kind==EngineCommandKind::PlaceOrder && command.placeOrder){
    const auto& place=*command.placeOrder;
    if(idempotencyIndex.has_command(place.commandId)){
      return {make_rejection(sequenceGenerator, place.commandId, optional_order_id(place.orderId), place.userId, place.symbolId, DuplicateCommand)};
    }

    auto configIt=symbolConfigs.find(place.symbolId);
    auto bookIt=orderBooks.find(place.symbolId);
    if(configIt==symbolConfigs.end() || bookIt==orderBooks.end()){
      auto events=std::vector<EngineEvent>{make_rejection(sequenceGenerator, place.commandId, optional_order_id(place.orderId), place.userId, place.symbolId, InvalidSymbol)};
      idempotencyIndex.record_command(place.commandId, command_summary(place.commandId, false, optional_order_id(place.orderId), InvalidSymbol, events));
      return events;
    }

    const auto validation=validate_place_order(place, configIt->second, idempotencyIndex);
    if(!validation.valid){
      auto events=std::vector<EngineEvent>{make_rejection(sequenceGenerator, place.commandId, optional_order_id(place.orderId), place.userId, place.symbolId, *validation.reason)};
      idempotencyIndex.record_command(place.commandId, command_summary(place.commandId, false, optional_order_id(place.orderId), validation.reason, events));
      return events;
    }

    auto events=bookIt->second.process_place(place, sequenceGenerator);
    const bool accepted=has_event_order_accepted(events);
    if(accepted){
      idempotencyIndex.record_order_id(place.orderId);
      idempotencyIndex.record_client_order_id(place.clientOrderId, place.orderId);
    }
    idempotencyIndex.record_command(place.commandId, command_summary(place.commandId, accepted, optional_order_id(place.orderId), rejected_reason(events), events));
    return events;
  }

  if(command.kind==EngineCommandKind::CancelOrder && command.cancelOrder){
    const auto& cancel=*command.cancelOrder;
    if(idempotencyIndex.has_command(cancel.commandId)){
      return {make_rejection(sequenceGenerator, cancel.commandId, optional_order_id(cancel.orderId), cancel.userId, cancel.symbolId, DuplicateCommand)};
    }

    auto configIt=symbolConfigs.find(cancel.symbolId);
    auto bookIt=orderBooks.find(cancel.symbolId);
    if(configIt==symbolConfigs.end() || bookIt==orderBooks.end()){
      auto events=std::vector<EngineEvent>{make_rejection(sequenceGenerator, cancel.commandId, optional_order_id(cancel.orderId), cancel.userId, cancel.symbolId, InvalidSymbol)};
      idempotencyIndex.record_command(cancel.commandId, command_summary(cancel.commandId, false, optional_order_id(cancel.orderId), InvalidSymbol, events));
      return events;
    }

    const auto validation=validate_cancel_order(cancel, configIt->second);
    if(!validation.valid){
      auto events=std::vector<EngineEvent>{make_rejection(sequenceGenerator, cancel.commandId, optional_order_id(cancel.orderId), cancel.userId, cancel.symbolId, *validation.reason)};
      idempotencyIndex.record_command(cancel.commandId, command_summary(cancel.commandId, false, optional_order_id(cancel.orderId), validation.reason, events));
      return events;
    }

    auto events=bookIt->second.process_cancel(cancel, sequenceGenerator);
    const bool accepted=has_event_order_cancelled(events);
    idempotencyIndex.record_command(cancel.commandId, command_summary(cancel.commandId, accepted, optional_order_id(cancel.orderId), rejected_reason(events), events));
    return events;
  }

  return {make_rejection(sequenceGenerator, 0, std::nullopt, 0, 0, InvalidSymbol)};
}

OrderBook* EngineCore::get_order_book(SymbolId symbolId){
  auto it=orderBooks.find(symbolId);
  if(it==orderBooks.end()){
    return nullptr;
  }
  return &it->second;
}

const OrderBook* EngineCore::get_order_book(SymbolId symbolId) const{
  auto it=orderBooks.find(symbolId);
  if(it==orderBooks.end()){
    return nullptr;
  }
  return &it->second;
}

EngineSnapshot EngineCore::snapshot() const{
  EngineSnapshot snapshot{};
  snapshot.sequenceState=sequenceGenerator.snapshot();
  snapshot.idempotencyState=idempotencyIndex.snapshot();

  for(const auto& entry : orderBooks){
    const auto& orderBook=entry.second;
    OrderBookSnapshot bookSnapshot{};
    bookSnapshot.symbolId=entry.first;
    bookSnapshot.symbolConfig=orderBook.symbolConfig;
    bookSnapshot.clientOrderIdToOrderId=orderBook.clientOrderIdToOrderId;

    append_side_snapshots(orderBook.bids, bookSnapshot.bidLevels);
    append_side_snapshots(orderBook.asks, bookSnapshot.askLevels);

    for(const auto& orderEntry : orderBook.orderById){
      if(orderEntry.second!=nullptr){
        bookSnapshot.activeOrders.push_back(snapshot_order(*orderEntry.second));
      }
    }
    std::sort(bookSnapshot.activeOrders.begin(), bookSnapshot.activeOrders.end(), [](const OrderSnapshot& left, const OrderSnapshot& right){
      return left.orderId<right.orderId;
    });

    snapshot.symbolSnapshots.push_back(std::move(bookSnapshot));
  }

  std::sort(snapshot.symbolSnapshots.begin(), snapshot.symbolSnapshots.end(), [](const OrderBookSnapshot& left, const OrderBookSnapshot& right){
    return left.symbolId<right.symbolId;
  });

  return snapshot;
}

void EngineCore::restore(const EngineSnapshot& snapshot){
  for(auto& entry : orderBooks){
    release_order_storage(entry.second);
  }
  orderBooks.clear();
  symbolConfigs.clear();

  sequenceGenerator.restore(snapshot.sequenceState);
  idempotencyIndex.restore(snapshot.idempotencyState);

  for(const auto& bookSnapshot : snapshot.symbolSnapshots){
    symbolConfigs.insert_or_assign(bookSnapshot.symbolId, bookSnapshot.symbolConfig);
    OrderBook orderBook(bookSnapshot.symbolConfig);

    for(const auto& orderSnapshot : bookSnapshot.activeOrders){
      orderBook.insert_remaining_as_resting(restore_order(orderSnapshot));
    }
    orderBook.clientOrderIdToOrderId=bookSnapshot.clientOrderIdToOrderId;

    orderBooks.emplace(bookSnapshot.symbolId, std::move(orderBook));
  }
}
