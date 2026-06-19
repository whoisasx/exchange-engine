#pragma once
#include<cstdint>
#include<optional>
#include"Types.hpp"
#include"FixedPoint.hpp"

struct PlaceOrderCommand{
  CommandId commandId;
  ClientOrderId clientOrderId;
  OrderId orderId;
  UserId userId;
  SymbolId symbolId;
  Side side;
  OrderType orderType;
  TimeInForce timeInForce;
  Price price;
  Quantity quantity;
  EngineSequence receivedSequence;
};

struct CancelOrderCommand{
  CommandId commandId;
  UserId userId;
  SymbolId symbolId;
  OrderId orderId;
  ClientOrderId clientOrderId;
  EngineSequence receivedSequence;
};

enum class EngineCommandKind{
  PlaceOrder,
  CancelOrder
};

struct EngineCommand{
  EngineCommandKind kind;
  std::optional<PlaceOrderCommand> placeOrder;
  std::optional<CancelOrderCommand> cancelOrder;
};

