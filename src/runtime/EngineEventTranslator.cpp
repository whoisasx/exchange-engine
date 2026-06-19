#include "runtime/EngineEventTranslator.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>

namespace cex::runtime {
namespace {

template <typename Value>
[[nodiscard]] std::string text(Value value) {
  return std::to_string(value);
}

[[nodiscard]] std::string side_text(Side side) {
  return side == Buy ? "LONG" : "SHORT";
}

[[nodiscard]] std::string reason_text(RejectReason reason) {
  switch (reason) {
    case InvalidSymbol:
      return "invalid_symbol";
    case InvalidPrice:
      return "invalid_price";
    case InvalidQuantity:
      return "invalid_quantity";
    case DuplicateCommand:
      return "duplicate_command";
    case DuplicateOrder:
      return "duplicate_order";
    case OrderNotFound:
      return "order_not_found";
    case WouldSelfTrade:
      return "would_self_trade";
    case MarketClosed:
      return "market_closed";
    case InsufficientBalanceLiquidity:
      return "insufficient_balance_liquidity";
  }
  return "unknown";
}

void add_source_fields(PayloadFields& payload,
                       const EngineEventTranslationContext& context) {
  if (context.input_id.has_value()) {
    payload.emplace("source_input_id", *context.input_id);
  }
  payload.emplace("source_input_offset", text(context.source.offset));
}

void add_engine_fields(PayloadFields& payload,
                       cex::adapter::MarketId market_id,
                       cex::adapter::MarketSequenceGenerator& market_sequences,
                       const EngineRuntimeClock& clock) {
  const EngineSequence sequence = market_sequences.next(market_id);
  payload.emplace("engine_sequence", text(sequence));
  payload.emplace("engine_event_id",
                  "eng_" + text(market_id) + "_" + text(sequence));
  payload.emplace("engine_timestamp_ms", text(clock ? clock() : 0));
}

[[nodiscard]] EngineOutputRecord make_reply(
    std::string type,
    const EngineEventTranslationContext& context,
    PayloadFields payload) {
  payload.emplace("request_id", context.request_id);
  add_source_fields(payload, context);

  return EngineOutputRecord{
      .topic = EngineRepliesTopic,
      .type = std::move(type),
      .key = context.request_id,
      .partition = context.reply_partition,
      .payload = std::move(payload),
  };
}

[[nodiscard]] EngineOutputRecord make_event(
    std::string type,
    cex::adapter::MarketId market_id,
    const EngineEventTranslationContext& context,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock,
    PayloadFields payload) {
  add_engine_fields(payload, market_id, market_sequences, clock);
  add_source_fields(payload, context);
  payload.emplace("market_id", text(market_id));

  return EngineOutputRecord{
      .topic = EngineEventsTopic,
      .type = std::move(type),
      .key = text(market_id),
      .partition = std::nullopt,
      .payload = std::move(payload),
  };
}

[[nodiscard]] const cex::adapter::OrderMetadata* find_metadata(
    OrderId order_id,
    const EngineEventTranslationContext& context,
    const cex::adapter::OrderMetadataStore& metadata_store) {
  if (context.pending_metadata.has_value() &&
      context.pending_metadata->order_id == order_id) {
    return &*context.pending_metadata;
  }
  return metadata_store.find(order_id);
}

[[nodiscard]] std::unordered_set<OrderId> filled_order_ids(
    const std::vector<EngineEvent>& core_events) {
  std::unordered_set<OrderId> filled;
  for (const auto& event : core_events) {
    if (const auto* order_filled = std::get_if<OrderFilled>(&event)) {
      filled.insert(order_filled->orderId);
    }
  }
  return filled;
}

void append_order_accepted(
    EngineProcessResult& result,
    const OrderAccepted& accepted,
    const std::unordered_set<OrderId>& filled,
    const EngineEventTranslationContext& context,
    const cex::adapter::OrderMetadataStore& metadata_store,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const auto* metadata = find_metadata(accepted.orderId, context, metadata_store);

  PayloadFields reply_payload{{"order_id", text(accepted.orderId)}};
  if (metadata != nullptr) {
    reply_payload.emplace("reservation_id", metadata->reservation_id);
  }
  result.replies.push_back(
      make_reply("OrderAccepted", context, std::move(reply_payload)));

  if (!context.can_open_resting_order || filled.contains(accepted.orderId)) {
    return;
  }

  PayloadFields event_payload{
      {"order_id", text(accepted.orderId)},
      {"user_id", text(accepted.userId)},
      {"side", side_text(accepted.side)},
      {"price", text(accepted.price.ticks())},
      {"quantity", text(accepted.quantity.lots())},
  };
  if (metadata != nullptr) {
    event_payload.emplace("reservation_id", metadata->reservation_id);
  }

  result.events.push_back(
      make_event("OrderOpened",
                 static_cast<cex::adapter::MarketId>(accepted.symbolId),
                 context,
                 market_sequences,
                 clock,
                 std::move(event_payload)));
}

void append_order_rejected(EngineProcessResult& result,
                           const OrderRejected& rejected,
                           const EngineEventTranslationContext& context,
                           const cex::adapter::OrderMetadataStore& metadata_store) {
  PayloadFields payload{{"reason", reason_text(rejected.reason)}};

  const auto order_id = rejected.orderId.value_or(context.order_id);
  if (order_id != 0) {
    payload.emplace("order_id", text(order_id));
  }

  if (const auto* metadata = find_metadata(order_id, context, metadata_store);
      metadata != nullptr) {
    payload.emplace("reservation_id", metadata->reservation_id);
  }

  const char* reply_type =
      context.command_kind == RuntimeCommandKind::CancelOrder ? "CancelRejected"
                                                             : "OrderRejected";
  result.replies.push_back(make_reply(reply_type, context, std::move(payload)));
}

void append_trade_executed(
    EngineProcessResult& result,
    const TradeExecuted& trade,
    const EngineEventTranslationContext& context,
    const cex::adapter::OrderMetadataStore& metadata_store,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const auto* maker = find_metadata(trade.makerOrderId, context, metadata_store);
  const auto* taker = find_metadata(trade.takerOrderId, context, metadata_store);

  PayloadFields payload{
      {"trade_id", text(trade.tradeId)},
      {"fill_id", text(trade.tradeId)},
      {"price", text(trade.price.ticks())},
      {"quantity", text(trade.quantity.lots())},
      {"maker_order_id", text(trade.makerOrderId)},
      {"taker_order_id", text(trade.takerOrderId)},
      {"execution_reason", "TRADE"},
  };
  if (maker != nullptr) {
    payload.emplace("maker_user_id", text(maker->user_id));
    payload.emplace("maker_reservation_id", maker->reservation_id);
  }
  if (taker != nullptr) {
    payload.emplace("taker_user_id", text(taker->user_id));
    payload.emplace("taker_reservation_id", taker->reservation_id);
  }

  result.events.push_back(
      make_event("TradeExecuted",
                 static_cast<cex::adapter::MarketId>(trade.symbolId),
                 context,
                 market_sequences,
                 clock,
                 std::move(payload)));
}

void append_order_cancelled(
    EngineProcessResult& result,
    const OrderCancelled& cancelled,
    const EngineEventTranslationContext& context,
    const cex::adapter::OrderMetadataStore& metadata_store,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const auto* metadata =
      find_metadata(cancelled.orderId, context, metadata_store);
  const auto market_id =
      metadata != nullptr ? metadata->market_id : context.market_id;

  result.replies.push_back(make_reply(
      "CancelAccepted", context, PayloadFields{{"order_id", text(cancelled.orderId)}}));

  PayloadFields payload{
      {"order_id", text(cancelled.orderId)},
      {"released_quantity", text(cancelled.releasedQuantity.lots())},
      {"released_amount", text(cancelled.releasedQuantity.lots())},
  };
  if (metadata != nullptr) {
    payload.emplace("reservation_id", metadata->reservation_id);
    payload.emplace("user_id", text(metadata->user_id));
  }

  result.events.push_back(make_event("OrderCancelled",
                                     market_id,
                                     context,
                                     market_sequences,
                                     clock,
                                     std::move(payload)));
}

void append_orderbook_delta(
    EngineProcessResult& result,
    const OrderBookDelta& delta,
    const EngineEventTranslationContext& context,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"side", side_text(delta.side)},
      {"price", text(delta.price.ticks())},
      {"quantity", text(delta.totalQuantityAtPrice.lots())},
  };

  result.events.push_back(make_event("OrderBookDelta",
                                     static_cast<cex::adapter::MarketId>(
                                         delta.symbolId),
                                     context,
                                     market_sequences,
                                     clock,
                                     std::move(payload)));
}

}  // namespace

EngineProcessResult EngineEventTranslator::translate(
    const std::vector<EngineEvent>& core_events,
    const EngineEventTranslationContext& context,
    const cex::adapter::OrderMetadataStore& metadata_store,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) const {
  EngineProcessResult result;
  const auto filled = filled_order_ids(core_events);

  for (const auto& event : core_events) {
    if (const auto* accepted = std::get_if<OrderAccepted>(&event)) {
      append_order_accepted(result,
                            *accepted,
                            filled,
                            context,
                            metadata_store,
                            market_sequences,
                            clock);
      continue;
    }
    if (const auto* rejected = std::get_if<OrderRejected>(&event)) {
      append_order_rejected(result, *rejected, context, metadata_store);
      continue;
    }
    if (const auto* trade = std::get_if<TradeExecuted>(&event)) {
      append_trade_executed(result,
                            *trade,
                            context,
                            metadata_store,
                            market_sequences,
                            clock);
      continue;
    }
    if (const auto* cancelled = std::get_if<OrderCancelled>(&event)) {
      append_order_cancelled(result,
                             *cancelled,
                             context,
                             metadata_store,
                             market_sequences,
                             clock);
      continue;
    }
    if (const auto* delta = std::get_if<OrderBookDelta>(&event)) {
      append_orderbook_delta(result, *delta, context, market_sequences, clock);
      continue;
    }
  }

  return result;
}

}  // namespace cex::runtime
