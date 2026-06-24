#include "runtime/EngineEventTranslator.hpp"

#include <algorithm>
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

[[nodiscard]] std::string adapter_side_text(cex::adapter::AdapterSide side) {
  return side == cex::adapter::AdapterSide::Long ? "LONG" : "SHORT";
}

[[nodiscard]] std::string position_side_text(std::int64_t signed_quantity) {
  if (signed_quantity > 0) {
    return "LONG";
  }
  if (signed_quantity < 0) {
    return "SHORT";
  }
  return "FLAT";
}

[[nodiscard]] std::int64_t abs_quantity(std::int64_t quantity) {
  return quantity < 0 ? -quantity : quantity;
}

[[nodiscard]] std::string position_id_text(
    cex::adapter::AdapterUserId user_id,
    cex::adapter::MarketId market_id) {
  return "pos_" + text(user_id) + "_" + text(market_id);
}

[[nodiscard]] std::int64_t notional_value(std::int64_t signed_quantity,
                                          cex::adapter::AdapterPrice price) {
  return abs_quantity(signed_quantity) * price;
}

[[nodiscard]] std::int64_t maintenance_margin_for(
    std::int64_t signed_quantity,
    cex::adapter::AdapterPrice price) {
  return notional_value(signed_quantity, price) / 20;
}

[[nodiscard]] std::int64_t margin_ratio_for(std::int64_t equity,
                                            std::int64_t maintenance_margin) {
  if (maintenance_margin == 0) {
    return 0;
  }
  return (equity * 10'000) / maintenance_margin;
}

[[nodiscard]] std::string risk_status_for(std::int64_t signed_quantity,
                                          std::int64_t equity,
                                          std::int64_t maintenance_margin) {
  if (signed_quantity == 0) {
    return "FLAT";
  }
  if (equity <= maintenance_margin) {
    return "LIQUIDATABLE";
  }
  return "HEALTHY";
}

[[nodiscard]] bool same_direction(std::int64_t left, std::int64_t right) {
  return (left > 0 && right > 0) || (left < 0 && right < 0);
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

[[nodiscard]] PayloadValue number_value(std::string value) {
  return PayloadValue::number_text(std::move(value));
}

template <typename Value>
[[nodiscard]] PayloadValue number_value(Value value) {
  return number_value(text(value));
}

[[nodiscard]] PayloadValue price_level_delta(Price price, Quantity quantity) {
  return PayloadValue::object(PayloadValue::Object{
      {"price", number_value(price.ticks())},
      {"quantity", number_value(quantity.lots())},
  });
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

[[nodiscard]] cex::adapter::OrderMetadata* find_metadata_mutable(
    OrderId order_id,
    EngineEventTranslationContext& context,
    cex::adapter::OrderMetadataStore& metadata_store) {
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

[[nodiscard]] std::int64_t fallback_margin(
    const cex::adapter::OrderMetadata& metadata,
    std::int64_t fill_quantity,
    cex::adapter::AdapterPrice price) {
  const auto leverage = metadata.leverage <= 0 ? 1 : metadata.leverage;
  return (price * fill_quantity) / leverage;
}

[[nodiscard]] std::int64_t preview_fill_margin(
    const cex::adapter::OrderMetadata& metadata,
    std::int64_t fill_quantity,
    cex::adapter::AdapterPrice price) {
  const auto original_quantity = metadata.original_quantity;
  if (metadata.reserved_margin_amount > 0 && original_quantity > 0) {
    if (metadata.remaining_quantity > 0 &&
        fill_quantity >= metadata.remaining_quantity) {
      return metadata.remaining_reserved_margin;
    }
    auto allocated =
        (metadata.reserved_margin_amount * fill_quantity) / original_quantity;
    if (allocated > metadata.remaining_reserved_margin) {
      allocated = metadata.remaining_reserved_margin;
    }
    return allocated;
  }
  return fallback_margin(metadata, fill_quantity, price);
}

[[nodiscard]] std::int64_t allocate_fill_margin(
    cex::adapter::OrderMetadata& metadata,
    std::int64_t fill_quantity,
    cex::adapter::AdapterPrice price) {
  const auto original_quantity = metadata.original_quantity;
  std::int64_t allocated = 0;
  if (metadata.reserved_margin_amount > 0 && original_quantity > 0) {
    if (metadata.remaining_quantity > 0 &&
        fill_quantity >= metadata.remaining_quantity) {
      allocated = metadata.remaining_reserved_margin;
    } else {
      allocated = (metadata.reserved_margin_amount * fill_quantity) /
                  original_quantity;
    }
    if (allocated > metadata.remaining_reserved_margin) {
      allocated = metadata.remaining_reserved_margin;
    }
  } else {
    allocated = fallback_margin(metadata, fill_quantity, price);
  }

  if (metadata.remaining_quantity > fill_quantity) {
    metadata.remaining_quantity -= fill_quantity;
  } else {
    metadata.remaining_quantity = 0;
  }
  if (metadata.remaining_reserved_margin > allocated) {
    metadata.remaining_reserved_margin -= allocated;
  } else {
    metadata.remaining_reserved_margin = 0;
  }
  return allocated;
}

[[nodiscard]] std::int64_t signed_fill_quantity(
    const cex::adapter::OrderMetadata& metadata,
    std::int64_t fill_quantity) {
  return metadata.side == cex::adapter::AdapterSide::Long ? fill_quantity
                                                          : -fill_quantity;
}

void apply_position_fill(IsolatedPositionState& position,
                         const cex::adapter::OrderMetadata& metadata,
                         std::int64_t signed_fill,
                         cex::adapter::AdapterPrice price,
                         std::int64_t fill_margin,
                         std::int64_t timestamp_ms) {
  const auto old_quantity = position.signed_quantity;
  const auto new_quantity = old_quantity + signed_fill;
  const auto old_abs = abs_quantity(old_quantity);
  const auto fill_abs = abs_quantity(signed_fill);

  cex::adapter::AdapterPrice new_average = position.average_entry_price;
  std::int64_t new_margin = position.isolated_margin;

  if (old_quantity == 0) {
    new_average = price;
    new_margin = fill_margin;
  } else if (same_direction(old_quantity, signed_fill)) {
    const auto new_abs = old_abs + fill_abs;
    new_average =
        (position.average_entry_price * old_abs + price * fill_abs) / new_abs;
    new_margin += fill_margin;
  } else if (new_quantity == 0) {
    new_average = 0;
    new_margin = 0;
  } else if (same_direction(old_quantity, new_quantity)) {
    new_margin = old_abs == 0
                     ? 0
                     : (position.isolated_margin * abs_quantity(new_quantity)) /
                           old_abs;
  } else {
    new_average = price;
    new_margin =
        fill_abs == 0 ? 0 : (fill_margin * abs_quantity(new_quantity)) / fill_abs;
  }

  position.user_id = metadata.user_id;
  position.market_id = metadata.market_id;
  position.signed_quantity = new_quantity;
  position.average_entry_price = new_average;
  if (!metadata.margin_asset.empty()) {
    position.margin_asset = metadata.margin_asset;
  }
  position.isolated_margin = new_margin;
  if (position.leverage == 0 || old_quantity == 0) {
    position.leverage = metadata.leverage;
  }
  position.updated_at_ms = timestamp_ms;
}

[[nodiscard]] cex::adapter::AdapterPrice mark_price_for(
    cex::adapter::MarketId market_id,
    cex::adapter::AdapterPrice trade_price,
    const std::unordered_map<cex::adapter::MarketId, MarkPriceState>&
        mark_prices) {
  const auto it = mark_prices.find(market_id);
  return it == mark_prices.end() ? trade_price : it->second.mark_price;
}

[[nodiscard]] IsolatedRiskState risk_from_position(
    const IsolatedPositionState& position,
    cex::adapter::AdapterPrice mark_price,
    std::int64_t timestamp_ms) {
  const auto unrealized_pnl =
      (mark_price - position.average_entry_price) * position.signed_quantity;
  const auto maintenance_margin =
      maintenance_margin_for(position.signed_quantity, mark_price);
  const auto equity = position.isolated_margin + unrealized_pnl;
  return IsolatedRiskState{
      .user_id = position.user_id,
      .market_id = position.market_id,
      .status = risk_status_for(position.signed_quantity,
                                equity,
                                maintenance_margin),
      .margin_asset = position.margin_asset,
      .signed_quantity = position.signed_quantity,
      .average_entry_price = position.average_entry_price,
      .mark_price = mark_price,
      .isolated_margin = position.isolated_margin,
      .unrealized_pnl = unrealized_pnl,
      .equity = equity,
      .maintenance_margin = maintenance_margin,
      .margin_ratio = margin_ratio_for(equity, maintenance_margin),
      .leverage = position.leverage,
      .updated_at_ms = timestamp_ms,
  };
}

struct FillAccountEffect {
  std::int64_t realized_pnl{0};
  std::int64_t released_margin{0};
};

[[nodiscard]] FillAccountEffect account_effect_for_fill(
    const IsolatedPositionState& position,
    std::int64_t signed_fill,
    cex::adapter::AdapterPrice fill_price) {
  if (position.signed_quantity == 0 ||
      same_direction(position.signed_quantity, signed_fill)) {
    return {};
  }

  const auto old_abs = abs_quantity(position.signed_quantity);
  const auto fill_abs = abs_quantity(signed_fill);
  const auto closed_quantity = std::min(old_abs, fill_abs);
  const auto released_margin =
      closed_quantity >= old_abs
          ? position.isolated_margin
          : (position.isolated_margin * closed_quantity) / old_abs;
  const auto realized_pnl =
      position.signed_quantity > 0
          ? (fill_price - position.average_entry_price) * closed_quantity
          : (position.average_entry_price - fill_price) * closed_quantity;

  return FillAccountEffect{
      .realized_pnl = realized_pnl,
      .released_margin = released_margin,
  };
}

[[nodiscard]] bool has_account_delta(const FillAccountEffect& effect) {
  return effect.realized_pnl != 0;
}

[[nodiscard]] const char* fill_reason_text(
    const EngineEventTranslationContext& context) {
  if (context.adl_execution) {
    return "ADL";
  }
  return context.command_kind == RuntimeCommandKind::LiquidatePosition
             ? "LIQUIDATION"
             : "TRADE";
}

[[nodiscard]] const char* account_delta_reason_text(
    const EngineEventTranslationContext& context) {
  if (context.adl_execution) {
    return "ADL_SETTLEMENT";
  }
  return context.command_kind == RuntimeCommandKind::LiquidatePosition
             ? "LIQUIDATION_SETTLEMENT"
             : "POSITION_CLOSE_SETTLEMENT";
}

[[nodiscard]] std::string account_delta_reference_id(
    const TradeExecuted& trade,
    const EngineEventTranslationContext& context) {
  if (context.adl_execution && !context.adl_id.empty()) {
    return context.adl_id;
  }
  if (context.command_kind == RuntimeCommandKind::LiquidatePosition &&
      !context.liquidation_id.empty()) {
    return context.liquidation_id;
  }
  return "fill_" + text(trade.tradeId);
}

[[nodiscard]] std::string account_delta_asset_for(
    const IsolatedPositionState& position,
    const cex::adapter::OrderMetadata& metadata) {
  if (!position.margin_asset.empty()) {
    return position.margin_asset;
  }
  return metadata.margin_asset.empty() ? "USDC" : metadata.margin_asset;
}

[[nodiscard]] EngineOutputRecord make_account_delta_event(
    const cex::adapter::OrderMetadata& metadata,
    const IsolatedPositionState& previous_position,
    const TradeExecuted& trade,
    const FillAccountEffect& effect,
    const EngineEventTranslationContext& context,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"account_delta_id",
       "acct_fill_" + text(trade.tradeId) + "_" + text(metadata.user_id) +
           "_" + text(metadata.order_id)},
      {"user_id", number_value(metadata.user_id)},
      {"asset", account_delta_asset_for(previous_position, metadata)},
      {"total_delta", number_value(effect.realized_pnl)},
      {"locked_delta", number_value(0)},
      {"reason", account_delta_reason_text(context)},
      {"reference_id", account_delta_reference_id(trade, context)},
  };

  return make_event("AccountDelta",
                    metadata.market_id,
                    context,
                    market_sequences,
                    clock,
                    std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_position_changed_event(
    const IsolatedPositionState& position,
    std::int64_t fill_quantity,
    cex::adapter::AdapterPrice fill_price,
    std::int64_t realized_pnl,
    cex::adapter::AdapterPrice mark_price,
    const EngineEventTranslationContext& context,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const auto unrealized_pnl =
      (mark_price - position.average_entry_price) * position.signed_quantity;
  const auto maintenance_margin =
      maintenance_margin_for(position.signed_quantity, mark_price);
  PayloadFields payload{
      {"user_id", number_value(position.user_id)},
      {"position_id", position_id_text(position.user_id, position.market_id)},
      {"side", position_side_text(position.signed_quantity)},
      {"signed_quantity", number_value(position.signed_quantity)},
      {"quantity", number_value(abs_quantity(position.signed_quantity))},
      {"average_entry_price", number_value(position.average_entry_price)},
      {"entry_price", number_value(position.average_entry_price)},
      {"mark_price", number_value(mark_price)},
      {"isolated_margin", number_value(position.isolated_margin)},
      {"realized_pnl", number_value(realized_pnl)},
      {"unrealized_pnl", number_value(unrealized_pnl)},
      {"maintenance_margin", number_value(maintenance_margin)},
      {"liquidation_price", number_value(0)},
      {"reason", fill_reason_text(context)},
      {"margin_asset", position.margin_asset},
      {"leverage", number_value(position.leverage)},
      {"last_fill_quantity", number_value(fill_quantity)},
      {"last_fill_price", number_value(fill_price)},
  };

  return make_event("PositionChanged",
                    position.market_id,
                    context,
                    market_sequences,
                    clock,
                    std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_risk_state_updated_event(
    const IsolatedRiskState& risk,
    const EngineEventTranslationContext& context,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"user_id", number_value(risk.user_id)},
      {"position_id", position_id_text(risk.user_id, risk.market_id)},
      {"status", risk.status},
      {"signed_quantity", number_value(risk.signed_quantity)},
      {"quantity", number_value(abs_quantity(risk.signed_quantity))},
      {"average_entry_price", number_value(risk.average_entry_price)},
      {"entry_price", number_value(risk.average_entry_price)},
      {"mark_price", number_value(risk.mark_price)},
      {"isolated_margin", number_value(risk.isolated_margin)},
      {"unrealized_pnl", number_value(risk.unrealized_pnl)},
      {"equity", number_value(risk.equity)},
      {"maintenance_margin", number_value(risk.maintenance_margin)},
      {"margin_ratio", number_value(risk.margin_ratio)},
      {"margin_asset", risk.margin_asset},
      {"leverage", number_value(risk.leverage)},
  };

  return make_event("RiskStateUpdated",
                    risk.market_id,
                    context,
                    market_sequences,
                    clock,
                    std::move(payload));
}

void append_position_and_risk_updates(
    EngineProcessResult& result,
    const TradeExecuted& trade,
    EngineEventTranslationContext& context,
    cex::adapter::OrderMetadataStore& metadata_store,
    IsolatedPositionMap& positions,
    IsolatedRiskMap& risk_states,
    const std::unordered_map<cex::adapter::MarketId, MarkPriceState>&
        mark_prices,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const auto timestamp_ms = clock ? clock() : 0;
  const auto fill_quantity =
      static_cast<std::int64_t>(trade.quantity.lots());
  const auto fill_price = trade.price.ticks();

  for (OrderId order_id : {trade.makerOrderId, trade.takerOrderId}) {
    auto* metadata = find_metadata_mutable(order_id, context, metadata_store);
    if (metadata == nullptr) {
      continue;
    }

    const auto signed_fill = signed_fill_quantity(*metadata, fill_quantity);
    const auto allocated_margin =
        allocate_fill_margin(*metadata, fill_quantity, fill_price);
    const auto key = PositionRiskKey{
        .user_id = metadata->user_id,
        .market_id = metadata->market_id,
    };
    auto& position = positions[key];
    const auto previous_position = position;
    const auto account_effect =
        account_effect_for_fill(previous_position, signed_fill, fill_price);
    apply_position_fill(position,
                        *metadata,
                        signed_fill,
                        fill_price,
                        allocated_margin,
                        timestamp_ms);

    const auto risk = risk_from_position(
        position,
        mark_price_for(metadata->market_id, fill_price, mark_prices),
        timestamp_ms);
    risk_states[key] = risk;

    if (has_account_delta(account_effect)) {
      result.events.push_back(make_account_delta_event(*metadata,
                                                       previous_position,
                                                       trade,
                                                       account_effect,
                                                       context,
                                                       market_sequences,
                                                       clock));
    }
    result.events.push_back(make_position_changed_event(position,
                                                        fill_quantity,
                                                        fill_price,
                                                        account_effect.realized_pnl,
                                                        risk.mark_price,
                                                        context,
                                                        market_sequences,
                                                        clock));
    result.events.push_back(make_risk_state_updated_event(risk,
                                                          context,
                                                          market_sequences,
                                                          clock));
  }
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
  std::string reply_type = "OrderAccepted";
  if (context.command_kind == RuntimeCommandKind::LiquidatePosition) {
    reply_type = "LiquidationAccepted";
    reply_payload.emplace("liquidation_id", context.liquidation_id);
  }
  if (metadata != nullptr) {
    reply_payload.emplace("reservation_id", metadata->reservation_id);
  }
  result.replies.push_back(
      make_reply(std::move(reply_type), context, std::move(reply_payload)));

  if (context.command_kind == RuntimeCommandKind::LiquidatePosition ||
      !context.can_open_resting_order || filled.contains(accepted.orderId)) {
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

[[nodiscard]] std::string settlement_asset_for(
    const cex::adapter::OrderMetadata* metadata) {
  if (metadata == nullptr || metadata->margin_asset.empty()) {
    return "USDC";
  }
  return metadata->margin_asset;
}

void append_order_rejected(EngineProcessResult& result,
                           const OrderRejected& rejected,
                           const EngineEventTranslationContext& context,
                           const cex::adapter::OrderMetadataStore& metadata_store,
                           cex::adapter::MarketSequenceGenerator& market_sequences,
                           const EngineRuntimeClock& clock) {
  PayloadFields payload{{"reason", reason_text(rejected.reason)}};

  const auto order_id = rejected.orderId.value_or(context.order_id);
  if (order_id != 0) {
    payload.emplace("order_id", text(order_id));
  }

  const auto* metadata = find_metadata(order_id, context, metadata_store);
  if (metadata != nullptr) {
    payload.emplace("reservation_id", metadata->reservation_id);
  }

  const char* reply_type = "OrderRejected";
  if (context.command_kind == RuntimeCommandKind::CancelOrder) {
    reply_type = "CancelRejected";
  } else if (context.command_kind == RuntimeCommandKind::LiquidatePosition) {
    reply_type = "LiquidationRejected";
    payload.emplace("liquidation_id", context.liquidation_id);
  }
  result.replies.push_back(make_reply(reply_type, context, std::move(payload)));

  if (context.command_kind != RuntimeCommandKind::PlaceOrder ||
      metadata == nullptr || metadata->reservation_id.empty() ||
      metadata->remaining_reserved_margin <= 0) {
    return;
  }

  PayloadFields event_payload{
      {"reservation_id", metadata->reservation_id},
      {"user_id", number_value(metadata->user_id)},
      {"asset", settlement_asset_for(metadata)},
      {"released_amount", number_value(metadata->remaining_reserved_margin)},
      {"reason", "ORDER_REJECTED_AFTER_RESERVATION"},
  };
  result.events.push_back(make_event("ReservationReleased",
                                     metadata->market_id,
                                     context,
                                     market_sequences,
                                     clock,
                                     std::move(event_payload)));
}

[[nodiscard]] std::int64_t liquidation_fee_for(
    const TradeExecuted& trade) {
  return (trade.price.ticks() *
          static_cast<std::int64_t>(trade.quantity.lots())) /
         200;
}

[[nodiscard]] std::int64_t liquidation_fee_for_context(
    const TradeExecuted& trade,
    const EngineEventTranslationContext& context) {
  return context.adl_execution ? 0 : liquidation_fee_for(trade);
}

[[nodiscard]] PayloadValue::Array liquidation_fee_deltas(
    const TradeExecuted& trade,
    const cex::adapter::OrderMetadata* taker,
    const EngineEventTranslationContext& context) {
  return PayloadValue::Array{
      PayloadValue::object(PayloadValue::Object{
          {"user_id", number_value(context.liquidated_user_id)},
          {"asset", settlement_asset_for(taker)},
          {"amount", number_value(liquidation_fee_for_context(trade,
                                                               context))},
          {"fee_type", "LIQUIDATION"},
      })};
}

void append_settlement_for(PayloadValue::Array& settlements,
                           const cex::adapter::OrderMetadata* metadata,
                           const TradeExecuted& trade) {
  if (metadata == nullptr || metadata->reservation_id.empty() ||
      metadata->reduce_only) {
    return;
  }

  const auto margin = preview_fill_margin(
      *metadata,
      static_cast<std::int64_t>(trade.quantity.lots()),
      trade.price.ticks());
  if (margin <= 0) {
    return;
  }

  const auto asset = settlement_asset_for(metadata);
  settlements.push_back(PayloadValue::object(PayloadValue::Object{
      {"reservation_id", metadata->reservation_id},
      {"debit_asset", asset},
      {"debit_amount", number_value(margin)},
      {"credit_asset", asset},
      {"credit_amount", number_value(margin)},
  }));
}

[[nodiscard]] PayloadValue::Array normal_settlements(
    const TradeExecuted& trade,
    const cex::adapter::OrderMetadata* maker,
    const cex::adapter::OrderMetadata* taker) {
  PayloadValue::Array settlements;
  append_settlement_for(settlements, maker, trade);
  append_settlement_for(settlements, taker, trade);
  return settlements;
}

[[nodiscard]] PayloadValue::Array liquidation_settlements(
    const TradeExecuted& trade,
    const cex::adapter::OrderMetadata* maker) {
  PayloadValue::Array settlements;
  if (maker == nullptr || maker->reservation_id.empty()) {
    return settlements;
  }

  const auto asset = settlement_asset_for(maker);
  const auto margin = preview_fill_margin(
      *maker,
      static_cast<std::int64_t>(trade.quantity.lots()),
      trade.price.ticks());
  settlements.push_back(PayloadValue::object(PayloadValue::Object{
      {"reservation_id", maker->reservation_id},
      {"debit_asset", asset},
      {"debit_amount", number_value(margin)},
      {"credit_asset", asset},
      {"credit_amount", number_value(margin)},
  }));
  return settlements;
}

[[nodiscard]] EngineOutputRecord make_liquidation_executed_event(
    const TradeExecuted& trade,
    const EngineEventTranslationContext& context,
    const cex::adapter::OrderMetadata* taker,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  if (context.adl_execution) {
    PayloadFields payload{
        {"adl_id", context.adl_id},
        {"liquidation_id", context.liquidation_id},
        {"liquidated_user_id", number_value(context.liquidated_user_id)},
        {"deleveraged_user_id",
         number_value(context.adl_counterparty_user_id)},
        {"position_side", adapter_side_text(context.liquidation_position_side)},
        {"quantity", number_value(trade.quantity.lots())},
        {"price", number_value(trade.price.ticks())},
        {"rank", number_value(context.adl_priority_rank)},
        {"reason", "INSURANCE_FUND_INSUFFICIENT"},
    };
    return make_event("AdlExecuted",
                      static_cast<cex::adapter::MarketId>(trade.symbolId),
                      context,
                      market_sequences,
                      clock,
                      std::move(payload));
  }

  PayloadFields payload{
      {"liquidation_id", context.liquidation_id},
      {"fill_id", number_value(trade.tradeId)},
      {"trade_id", number_value(trade.tradeId)},
      {"price", number_value(trade.price.ticks())},
      {"quantity", number_value(trade.quantity.lots())},
      {"execution_reason", "LIQUIDATION"},
      {"liquidated_user_id", number_value(context.liquidated_user_id)},
      {"user_id", number_value(context.liquidated_user_id)},
      {"position_side", adapter_side_text(context.liquidation_position_side)},
      {"liquidation_fee", number_value(liquidation_fee_for_context(trade,
                                                                    context))},
      {"fee_asset", settlement_asset_for(taker)},
  };

  return make_event("LiquidationExecuted",
                    static_cast<cex::adapter::MarketId>(trade.symbolId),
                    context,
                    market_sequences,
                    clock,
                    std::move(payload));
}

void append_trade_executed(
    EngineProcessResult& result,
    const TradeExecuted& trade,
    EngineEventTranslationContext& context,
    cex::adapter::OrderMetadataStore& metadata_store,
    IsolatedPositionMap& positions,
    IsolatedRiskMap& risk_states,
    const std::unordered_map<cex::adapter::MarketId, MarkPriceState>&
        mark_prices,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const auto* maker = find_metadata(trade.makerOrderId, context, metadata_store);
  const auto* taker = find_metadata(trade.takerOrderId, context, metadata_store);
  const bool is_liquidation =
      context.command_kind == RuntimeCommandKind::LiquidatePosition;
  const bool is_adl = context.adl_execution;

  PayloadFields payload{
      {"trade_id", text(trade.tradeId)},
      {"fill_id", text(trade.tradeId)},
      {"price", text(trade.price.ticks())},
      {"quantity", text(trade.quantity.lots())},
      {"maker_order_id", text(trade.makerOrderId)},
      {"taker_order_id", text(trade.takerOrderId)},
      {"execution_reason", is_liquidation ? "LIQUIDATION" : "TRADE"},
      {"fee_deltas",
       PayloadValue::array(is_liquidation && !is_adl
                               ? liquidation_fee_deltas(trade, taker, context)
                               : PayloadValue::Array{})},
      {"settlements",
       PayloadValue::array(is_liquidation
                               ? (!is_adl ? liquidation_settlements(trade,
                                                                    maker)
                                          : PayloadValue::Array{})
                               : normal_settlements(trade, maker, taker))},
  };
  if (maker != nullptr) {
    payload.emplace("maker_user_id", text(maker->user_id));
    payload.emplace("maker_reservation_id", maker->reservation_id);
  }
  if (taker != nullptr) {
    payload.emplace("taker_user_id", text(taker->user_id));
    payload.emplace("taker_reservation_id", taker->reservation_id);
  }
  if (is_liquidation) {
    payload.emplace("liquidation_id", context.liquidation_id);
    payload.emplace("liquidated_user_id", number_value(context.liquidated_user_id));
    payload.emplace("position_side",
                    adapter_side_text(context.liquidation_position_side));
    payload.emplace("liquidation_fee",
                    number_value(liquidation_fee_for_context(trade, context)));
  }
  if (is_adl) {
    payload.emplace("adl_id", context.adl_id);
    payload.emplace("counterparty_user_id",
                    number_value(context.adl_counterparty_user_id));
    payload.emplace("adl_priority_rank",
                    number_value(context.adl_priority_rank));
  }

  result.events.push_back(
      make_event("TradeExecuted",
                 static_cast<cex::adapter::MarketId>(trade.symbolId),
                 context,
                 market_sequences,
                 clock,
                 std::move(payload)));
  if (is_liquidation) {
    result.events.push_back(make_liquidation_executed_event(trade,
                                                            context,
                                                            taker,
                                                            market_sequences,
                                                            clock));
  }
  append_position_and_risk_updates(result,
                                   trade,
                                   context,
                                   metadata_store,
                                   positions,
                                   risk_states,
                                   mark_prices,
                                   market_sequences,
                                   clock);
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
  };
  if (metadata != nullptr) {
    payload.emplace("reservation_id", metadata->reservation_id);
    payload.emplace("user_id", text(metadata->user_id));
    payload.emplace("released_amount",
                    number_value(metadata->remaining_reserved_margin));
  } else {
    payload.emplace("released_amount", number_value(0));
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
      {"bids", PayloadValue::array(
                   delta.side == Buy
                       ? PayloadValue::Array{price_level_delta(
                             delta.price, delta.totalQuantityAtPrice)}
                       : PayloadValue::Array{})},
      {"asks", PayloadValue::array(
                   delta.side == Sell
                       ? PayloadValue::Array{price_level_delta(
                             delta.price, delta.totalQuantityAtPrice)}
                       : PayloadValue::Array{})},
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
    EngineEventTranslationContext& context,
    cex::adapter::OrderMetadataStore& metadata_store,
    IsolatedPositionMap& positions,
    IsolatedRiskMap& risk_states,
    const std::unordered_map<cex::adapter::MarketId, MarkPriceState>&
        mark_prices,
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
      append_order_rejected(result,
                            *rejected,
                            context,
                            metadata_store,
                            market_sequences,
                            clock);
      continue;
    }
    if (const auto* trade = std::get_if<TradeExecuted>(&event)) {
      append_trade_executed(result,
                            *trade,
                            context,
                            metadata_store,
                            positions,
                            risk_states,
                            mark_prices,
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
