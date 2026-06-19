#pragma once

#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "FixedPoint.hpp"
#include "SequenceGenerator.hpp"
#include "SymbolConfig.hpp"
#include "Types.hpp"

struct CommandResultSummary {
  CommandId commandId;
  bool accepted;
  std::optional<OrderId> orderId;
  std::optional<RejectReason> rejectReason;
  EngineSequence sequence;
};

struct IdempotencySnapshot {
  std::unordered_map<CommandId, CommandResultSummary> processedCommandIds;
  std::unordered_map<ClientOrderId, OrderId> clientOrderIdToOrderId;
  std::unordered_set<OrderId> orderIds;
};

struct OrderSnapshot{
  OrderId orderId;
  ClientOrderId clientOrderId;
  UserId userId;
  SymbolId symbolId;
  Side side;
  Price price;
  Quantity originalQuantity;
  Quantity remainingQuantity;
  EngineSequence sequenceAccepted;
};

struct PriceLevelSnapshot{
  Price price;
  Quantity totalQuantity;
  std::vector<OrderId> orderIds;
};

struct OrderBookSnapshot{
  SymbolId symbolId;
  SymbolConfig symbolConfig;
  std::vector<PriceLevelSnapshot> bidLevels;
  std::vector<PriceLevelSnapshot> askLevels;
  std::vector<OrderSnapshot> activeOrders;
  std::unordered_map<ClientOrderId,OrderId> clientOrderIdToOrderId;
};

struct EngineSnapshot{
  std::vector<OrderBookSnapshot> symbolSnapshots;
  SequenceState sequenceState;
  IdempotencySnapshot idempotencyState;
};
