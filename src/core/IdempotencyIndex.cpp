#include "IdempotencyIndex.hpp"

void IdempotencyIndex::record_command(CommandId commandId, CommandResultSummary resultSummary){
  resultSummary.commandId=commandId;
  processedCommandIds[commandId]=resultSummary;
}

void IdempotencyIndex::record_order_id(OrderId orderId){
  orderIds.insert(orderId);
}

void IdempotencyIndex::record_client_order_id(ClientOrderId clientOrderId, OrderId orderId){
  clientOrderIdToOrderId[clientOrderId]=orderId;
}

IdempotencySnapshot IdempotencyIndex::snapshot() const{
  return IdempotencySnapshot{
    processedCommandIds,
    clientOrderIdToOrderId,
    orderIds,
  };
}

void IdempotencyIndex::restore(const IdempotencySnapshot& snapshot){
  processedCommandIds=snapshot.processedCommandIds;
  clientOrderIdToOrderId=snapshot.clientOrderIdToOrderId;
  orderIds=snapshot.orderIds;
}
