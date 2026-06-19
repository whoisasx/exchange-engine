#pragma once

#include <optional>
#include <variant>

#include "FixedPoint.hpp"
#include "Types.hpp"

struct OrderAccepted{
  EventId eventId;
  EngineSequence sequence;
  OrderId orderId;
  ClientOrderId clientOrderId;
  UserId userId;
  SymbolId symbolId;
  Side side;
  Price price;
  Quantity quantity;
};
struct OrderRejected{
  EventId eventId;
  EngineSequence sequence;
  CommandId commandId;
  std::optional<OrderId> orderId;
  UserId userId;
  SymbolId symbolId;
  RejectReason reason;
};
struct TradeExecuted{
  EventId eventId;
  EngineSequence sequence;
  TradeId tradeId;
  SymbolId symbolId;
  OrderId makerOrderId;
  OrderId takerOrderId;
  Price price;
  Quantity quantity;
};
struct OrderPartial{
  EventId eventId;
  EngineSequence sequence;
  OrderId orderId;
  Quantity filledQuantity;
  Quantity remainingQuantity;
};
struct OrderFilled{
  EventId eventId;
  EngineSequence sequence;
  OrderId orderId;
};
struct OrderCancelled{
  EventId eventId;
  EngineSequence sequence;
  OrderId orderId;
  Quantity releasedQuantity;
};
struct OrderBookDelta{
  EventId eventId;
  EngineSequence sequence;
  SymbolId symbolId;
  Side side;
  Price price;
  Quantity totalQuantityAtPrice;
};

using EngineEvent=std::variant<
  OrderAccepted,
  OrderRejected,
  TradeExecuted,
  OrderPartial,
  OrderFilled,
  OrderCancelled,
  OrderBookDelta
>;
