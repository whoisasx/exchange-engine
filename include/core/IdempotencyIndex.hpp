#pragma once

#include <unordered_map>
#include <unordered_set>

#include "Snapshot.hpp"
#include "Types.hpp"

struct IdempotencyIndex{
  std::unordered_map<CommandId, CommandResultSummary> processedCommandIds;
  std::unordered_map<ClientOrderId,OrderId> clientOrderIdToOrderId;
  std::unordered_set<OrderId> orderIds;

  [[nodiscard]] bool has_command(CommandId commandId) const{
    return processedCommandIds.find(commandId)!=processedCommandIds.end();
  }
  void record_command(CommandId commandId, CommandResultSummary resultSummary);
  [[nodiscard]] bool has_order_id(OrderId orderId) const{ 
    return orderIds.find(orderId)!=orderIds.end();
  }
  [[nodiscard]] bool has_client_order_id(ClientOrderId clientOrderId) const{
    return clientOrderIdToOrderId.find(clientOrderId)!=clientOrderIdToOrderId.end();
  }
  void record_order_id(OrderId orderId) ;
  void record_client_order_id(ClientOrderId clientOrderId, OrderId orderId);
  IdempotencySnapshot snapshot() const;
  void restore(const IdempotencySnapshot& snapshot);
};
