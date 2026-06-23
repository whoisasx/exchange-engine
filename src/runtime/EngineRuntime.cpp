#include "runtime/EngineRuntime.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace cex::runtime {
namespace {

constexpr std::int64_t kMaxAutomaticLiquidationAttemptsPerTrigger = 16;

[[nodiscard]] std::int64_t system_timestamp_ms() {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

[[nodiscard]] cex::adapter::BrokerTopic broker_topic_from_string(
    const std::string& topic) {
  if (topic == EngineRepliesTopic) {
    return cex::adapter::BrokerTopic::EngineReplies;
  }
  if (topic == EngineEventsTopic) {
    return cex::adapter::BrokerTopic::EngineEvents;
  }
  return cex::adapter::BrokerTopic::EngineInput;
}

[[nodiscard]] cex::adapter::BrokerRecordContext broker_context(
    const InboundEngineRecord& record) {
  return cex::adapter::BrokerRecordContext{
      .topic = broker_topic_from_string(record.topic),
      .partition = record.partition,
      .offset = record.offset,
      .key = record.key,
  };
}

[[nodiscard]] EngineSequence received_sequence(
    const InboundEngineRecord& record) {
  if (record.offset < 0) {
    return 0;
  }
  return static_cast<EngineSequence>(record.offset);
}

[[nodiscard]] bool has_order_accepted(
    const std::vector<EngineEvent>& core_events) {
  for (const auto& event : core_events) {
    if (std::holds_alternative<OrderAccepted>(event)) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] bool has_trade_executed(
    const std::vector<EngineEvent>& core_events) {
  for (const auto& event : core_events) {
    if (std::holds_alternative<TradeExecuted>(event)) {
      return true;
    }
  }
  return false;
}

void cleanup_completed_order_metadata(
    cex::adapter::OrderMetadataStore& metadata_store,
    const std::vector<EngineEvent>& core_events) {
  for (const auto& event : core_events) {
    if (const auto* cancelled = std::get_if<OrderCancelled>(&event)) {
      (void)metadata_store.erase(cancelled->orderId);
      continue;
    }
    if (const auto* filled = std::get_if<OrderFilled>(&event)) {
      (void)metadata_store.erase(filled->orderId);
      continue;
    }
  }
}

[[nodiscard]] bool can_open_resting_order(
    const cex::adapter::PlaceOrderInput& input) {
  return input.order_type == cex::adapter::AdapterOrderType::Limit &&
         (input.time_in_force == cex::adapter::AdapterTimeInForce::Gtc ||
          input.time_in_force == cex::adapter::AdapterTimeInForce::PostOnly);
}

template <typename Value>
[[nodiscard]] std::string text(Value value) {
  return std::to_string(value);
}

[[nodiscard]] MarkPriceState mark_state_from_input(
    const cex::adapter::MarkPriceUpdatedInput& input) {
  return MarkPriceState{
      .market_id = input.market_id,
      .mark_price = input.mark_price,
      .index_price = input.index_price,
      .source_timestamp_ms = input.source_timestamp_ms,
      .published_at_ms = input.published_at_ms,
      .valid_until_ms = input.valid_until_ms,
      .source_sequence = input.source_sequence,
      .source_status = input.source_status,
  };
}

[[nodiscard]] FundingRateState funding_rate_state_from_input(
    const cex::adapter::FundingRateUpdatedInput& input) {
  return FundingRateState{
      .market_id = input.market_id,
      .funding_interval_id = input.funding_interval_id,
      .rate = input.rate,
      .rate_scale = input.rate_scale,
      .interval_start_ms = input.interval_start_ms,
      .interval_end_ms = input.interval_end_ms,
      .source_timestamp_ms = input.source_timestamp_ms,
  };
}

[[nodiscard]] EngineOutputRecord make_mark_price_updated_event(
    const InboundEngineRecord& record,
    const cex::adapter::MarkPriceUpdatedInput& input,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const EngineSequence sequence = market_sequences.next(input.market_id);
  PayloadFields payload{
      {"engine_sequence", text(sequence)},
      {"engine_event_id", "eng_" + text(input.market_id) + "_" + text(sequence)},
      {"engine_timestamp_ms", text(clock ? clock() : 0)},
      {"source_input_offset", text(record.offset)},
      {"market_id", text(input.market_id)},
      {"mark_price", text(input.mark_price)},
      {"index_price", text(input.index_price)},
      {"valid_until_ms", text(input.valid_until_ms)},
      {"source_sequence", text(input.source_sequence)},
      {"source_status", input.source_status},
  };
  if (input.input_id.has_value()) {
    payload.emplace("source_input_id", *input.input_id);
  }

  return EngineOutputRecord{
      .topic = EngineEventsTopic,
      .type = "MarkPriceUpdated",
      .key = text(input.market_id),
      .partition = std::nullopt,
      .payload = std::move(payload),
  };
}

[[nodiscard]] EngineOutputRecord make_funding_rate_updated_event(
    const InboundEngineRecord& record,
    const cex::adapter::FundingRateUpdatedInput& input,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const EngineSequence sequence = market_sequences.next(input.market_id);
  PayloadFields payload{
      {"engine_sequence", text(sequence)},
      {"engine_event_id", "eng_" + text(input.market_id) + "_" + text(sequence)},
      {"engine_timestamp_ms", text(clock ? clock() : 0)},
      {"source_input_offset", text(record.offset)},
      {"market_id", text(input.market_id)},
      {"funding_interval_id", input.funding_interval_id},
      {"rate", text(input.rate)},
      {"rate_scale", text(input.rate_scale)},
      {"interval_start_ms", text(input.interval_start_ms)},
      {"interval_end_ms", text(input.interval_end_ms)},
  };
  if (input.input_id.has_value()) {
    payload.emplace("source_input_id", *input.input_id);
  }

  return EngineOutputRecord{
      .topic = EngineEventsTopic,
      .type = "FundingRateUpdated",
      .key = text(input.market_id),
      .partition = std::nullopt,
      .payload = std::move(payload),
  };
}

[[nodiscard]] PayloadValue number_value(std::string value) {
  return PayloadValue::number_text(std::move(value));
}

template <typename Value>
[[nodiscard]] PayloadValue number_value(Value value) {
  return number_value(text(value));
}

[[nodiscard]] std::int64_t abs_quantity(std::int64_t quantity) {
  return quantity < 0 ? -quantity : quantity;
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

[[nodiscard]] bool side_matches_position(cex::adapter::AdapterSide side,
                                         std::int64_t signed_quantity) {
  return (side == cex::adapter::AdapterSide::Long && signed_quantity > 0) ||
         (side == cex::adapter::AdapterSide::Short && signed_quantity < 0);
}

[[nodiscard]] bool side_reduces_position(cex::adapter::AdapterSide side,
                                         std::int64_t signed_quantity) {
  return (side == cex::adapter::AdapterSide::Long && signed_quantity < 0) ||
         (side == cex::adapter::AdapterSide::Short && signed_quantity > 0);
}

[[nodiscard]] cex::adapter::AdapterSide liquidation_order_side(
    std::int64_t signed_quantity) {
  return signed_quantity > 0 ? cex::adapter::AdapterSide::Short
                             : cex::adapter::AdapterSide::Long;
}

[[nodiscard]] cex::adapter::AdapterQuantity liquidation_quantity(
    const cex::adapter::LiquidatePositionInput& input,
    const IsolatedPositionState& position) {
  const auto position_quantity = abs_quantity(position.signed_quantity);
  if (input.quantity <= 0) {
    return position_quantity;
  }
  return std::min(input.quantity, position_quantity);
}

[[nodiscard]] cex::adapter::AdapterPrice liquidation_price(
    const cex::adapter::LiquidatePositionInput& input,
    const IsolatedPositionState& position,
    const std::unordered_map<cex::adapter::MarketId, MarkPriceState>&
        mark_prices) {
  if (input.price > 0) {
    return input.price;
  }
  const auto mark = mark_prices.find(input.market_id);
  if (mark != mark_prices.end() && mark->second.mark_price > 0) {
    return mark->second.mark_price;
  }
  return position.average_entry_price;
}

[[nodiscard]] cex::adapter::AdapterOrderId liquidation_order_id(
    const std::string& liquidation_id) {
  const auto raw = cex::adapter::command_id_from_request_id(liquidation_id);
  constexpr auto max_order_id =
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
  auto bounded = raw & max_order_id;
  if (bounded == 0) {
    bounded = 1;
  }
  return static_cast<cex::adapter::AdapterOrderId>(bounded);
}

[[nodiscard]] bool crossing_liquidity_exists(
    const EngineCore& core,
    cex::adapter::MarketId market_id,
    cex::adapter::AdapterSide order_side,
    cex::adapter::AdapterPrice price) {
  if (price <= 0 || market_id <= 0 ||
      market_id > std::numeric_limits<SymbolId>::max()) {
    return false;
  }

  const auto* book = core.get_order_book(static_cast<SymbolId>(market_id));
  if (book == nullptr) {
    return false;
  }

  if (order_side == cex::adapter::AdapterSide::Long) {
    const auto* best_ask = book->best_ask();
    return best_ask != nullptr && !best_ask->totalQuantity.is_zero() &&
           best_ask->price.ticks() <= price;
  }

  const auto* best_bid = book->best_bid();
  return best_bid != nullptr && !best_bid->totalQuantity.is_zero() &&
         best_bid->price.ticks() >= price;
}

[[nodiscard]] std::int64_t maintenance_margin_for(
    std::int64_t signed_quantity,
    cex::adapter::AdapterPrice price) {
  return (abs_quantity(signed_quantity) * price) / 20;
}

[[nodiscard]] std::int64_t margin_ratio_for(std::int64_t equity,
                                            std::int64_t maintenance_margin) {
  if (maintenance_margin == 0) {
    return 0;
  }
  return (equity * 10'000) / maintenance_margin;
}

[[nodiscard]] std::int64_t notional_for(std::int64_t signed_quantity,
                                        cex::adapter::AdapterPrice price) {
  return abs_quantity(signed_quantity) * price;
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

[[nodiscard]] cex::adapter::AdapterSide position_adapter_side(
    std::int64_t signed_quantity) {
  return signed_quantity >= 0 ? cex::adapter::AdapterSide::Long
                              : cex::adapter::AdapterSide::Short;
}

struct AdlCandidate {
  PositionRiskKey key;
  IsolatedPositionState position;
  std::int64_t available_quantity{0};
  std::int64_t unrealized_pnl{0};
  std::int32_t leverage{0};
  std::int64_t margin_ratio{0};
};

struct AutomaticLiquidationCandidate {
  PositionRiskKey key;
  IsolatedPositionState position;
  IsolatedRiskState risk;
  std::int64_t notional{0};
};

struct ReduceOnlyCheck {
  bool allowed{true};
  std::string reason;
  cex::adapter::AdapterQuantity capped_quantity{0};
};

[[nodiscard]] ReduceOnlyCheck check_reduce_only_order(
    const cex::adapter::PlaceOrderInput& input,
    const IsolatedPositionMap& positions) {
  if (!input.reduce_only) {
    return ReduceOnlyCheck{
        .allowed = true,
        .reason = {},
        .capped_quantity = input.quantity,
    };
  }

  const PositionRiskKey key{
      .user_id = input.envelope.user_id,
      .market_id = input.market_id,
  };
  const auto position = positions.find(key);
  if (position == positions.end() || position->second.signed_quantity == 0) {
    return ReduceOnlyCheck{
        .allowed = false,
        .reason = "reduce_only_no_position",
        .capped_quantity = 0,
    };
  }
  if (!side_reduces_position(input.side, position->second.signed_quantity)) {
    return ReduceOnlyCheck{
        .allowed = false,
        .reason = "reduce_only_wrong_side",
        .capped_quantity = 0,
    };
  }

  return ReduceOnlyCheck{
      .allowed = true,
      .reason = {},
      .capped_quantity =
          std::min(input.quantity, abs_quantity(position->second.signed_quantity)),
  };
}

[[nodiscard]] bool has_opposing_exposure(std::int64_t liquidated_quantity,
                                         std::int64_t candidate_quantity) {
  return (liquidated_quantity > 0 && candidate_quantity < 0) ||
         (liquidated_quantity < 0 && candidate_quantity > 0);
}

[[nodiscard]] std::vector<AdlCandidate> adl_candidates_for(
    const cex::adapter::LiquidatePositionInput& input,
    const IsolatedPositionState& liquidated_position,
    const IsolatedPositionMap& positions,
    const IsolatedRiskMap& risk_states) {
  std::vector<AdlCandidate> candidates;
  for (const auto& [key, position] : positions) {
    if (key.market_id != input.market_id ||
        key.user_id == input.liquidated_user_id ||
        position.signed_quantity == 0 ||
        !has_opposing_exposure(liquidated_position.signed_quantity,
                               position.signed_quantity)) {
      continue;
    }

    const auto risk = risk_states.find(key);
    if (risk == risk_states.end() || risk->second.unrealized_pnl <= 0) {
      continue;
    }

    candidates.push_back(AdlCandidate{
        .key = key,
        .position = position,
        .available_quantity = abs_quantity(position.signed_quantity),
        .unrealized_pnl = risk->second.unrealized_pnl,
        .leverage = risk->second.leverage,
        .margin_ratio = risk->second.margin_ratio,
    });
  }

  // Runtime state does not maintain an exchange ADL quantile. For replayable
  // selection, rank opposing accounts by available profit/risk signals:
  // higher unrealized PnL, higher leverage, lower margin ratio, larger
  // opposing exposure, then lower user id.
  std::sort(candidates.begin(),
            candidates.end(),
            [](const AdlCandidate& left, const AdlCandidate& right) {
              if (left.unrealized_pnl != right.unrealized_pnl) {
                return left.unrealized_pnl > right.unrealized_pnl;
              }
              if (left.leverage != right.leverage) {
                return left.leverage > right.leverage;
              }
              if (left.margin_ratio != right.margin_ratio) {
                return left.margin_ratio < right.margin_ratio;
              }
              if (left.available_quantity != right.available_quantity) {
                return left.available_quantity > right.available_quantity;
              }
              return left.key.user_id < right.key.user_id;
            });
  return candidates;
}

[[nodiscard]] std::int64_t total_adl_available(
    const std::vector<AdlCandidate>& candidates,
    std::int64_t limit) {
  std::int64_t available = 0;
  for (const auto& candidate : candidates) {
    available += candidate.available_quantity;
    if (available >= limit) {
      return limit;
    }
  }
  return available;
}

[[nodiscard]] std::vector<AutomaticLiquidationCandidate>
automatic_liquidation_candidates_for(
    cex::adapter::MarketId market_id,
    const IsolatedPositionMap& positions,
    const IsolatedRiskMap& risk_states,
    const std::set<PositionRiskKey>& attempted) {
  std::vector<AutomaticLiquidationCandidate> candidates;
  for (const auto& [key, risk] : risk_states) {
    if (key.market_id != market_id || risk.status != "LIQUIDATABLE" ||
        risk.signed_quantity == 0 || attempted.contains(key)) {
      continue;
    }

    const auto position = positions.find(key);
    if (position == positions.end() ||
        position->second.signed_quantity == 0) {
      continue;
    }

    const auto price = risk.mark_price > 0 ? risk.mark_price
                                           : position->second.average_entry_price;
    candidates.push_back(AutomaticLiquidationCandidate{
        .key = key,
        .position = position->second,
        .risk = risk,
        .notional = notional_for(position->second.signed_quantity, price),
    });
  }

  std::sort(candidates.begin(),
            candidates.end(),
            [](const AutomaticLiquidationCandidate& left,
               const AutomaticLiquidationCandidate& right) {
              if (left.risk.margin_ratio != right.risk.margin_ratio) {
                return left.risk.margin_ratio < right.risk.margin_ratio;
              }
              if (left.risk.equity != right.risk.equity) {
                return left.risk.equity < right.risk.equity;
              }
              if (left.notional != right.notional) {
                return left.notional > right.notional;
              }
              return left.key.user_id < right.key.user_id;
            });
  return candidates;
}

[[nodiscard]] std::string position_id_text(
    cex::adapter::AdapterUserId user_id,
    cex::adapter::MarketId market_id) {
  return "pos_" + text(user_id) + "_" + text(market_id);
}

[[nodiscard]] IsolatedRiskState risk_from_position(
    const IsolatedPositionState& position,
    cex::adapter::AdapterPrice mark_price,
    std::int64_t updated_at_ms,
    const IsolatedRiskState* previous_risk = nullptr) {
  const auto unrealized_pnl =
      (mark_price - position.average_entry_price) * position.signed_quantity;
  const auto maintenance_margin =
      maintenance_margin_for(position.signed_quantity, mark_price);

  std::int64_t equity_adjustment = 0;
  if (previous_risk != nullptr) {
    equity_adjustment = previous_risk->equity -
                        (previous_risk->isolated_margin +
                         previous_risk->unrealized_pnl);
  }

  const auto equity =
      position.isolated_margin + unrealized_pnl + equity_adjustment;
  return IsolatedRiskState{
      .user_id = position.user_id,
      .market_id = position.market_id,
      .status = risk_status_for(position.signed_quantity,
                                equity,
                                maintenance_margin),
      .margin_asset = position.margin_asset.empty() ? "USDC"
                                                    : position.margin_asset,
      .signed_quantity = position.signed_quantity,
      .average_entry_price = position.average_entry_price,
      .mark_price = mark_price,
      .isolated_margin = position.isolated_margin,
      .unrealized_pnl = unrealized_pnl,
      .equity = equity,
      .maintenance_margin = maintenance_margin,
      .margin_ratio = margin_ratio_for(equity, maintenance_margin),
      .leverage = position.leverage,
      .updated_at_ms = updated_at_ms,
  };
}

void add_runtime_source_fields(PayloadFields& payload,
                               const InboundEngineRecord& record,
                               const std::optional<std::string>& input_id) {
  if (input_id.has_value()) {
    payload.emplace("source_input_id", *input_id);
  }
  payload.emplace("source_input_offset", number_value(record.offset));
}

void add_runtime_engine_fields(
    PayloadFields& payload,
    cex::adapter::MarketId market_id,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const EngineSequence sequence = market_sequences.next(market_id);
  payload.emplace("engine_sequence", number_value(sequence));
  payload.emplace("engine_event_id",
                  "eng_" + text(market_id) + "_" + text(sequence));
  payload.emplace("engine_timestamp_ms", number_value(clock ? clock() : 0));
}

[[nodiscard]] EngineOutputRecord make_runtime_market_event(
    std::string type,
    cex::adapter::MarketId market_id,
    const InboundEngineRecord& record,
    const std::optional<std::string>& input_id,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock,
    PayloadFields payload) {
  add_runtime_engine_fields(payload, market_id, market_sequences, clock);
  add_runtime_source_fields(payload, record, input_id);
  payload.emplace("market_id", number_value(market_id));

  return EngineOutputRecord{
      .topic = EngineEventsTopic,
      .type = std::move(type),
      .key = text(market_id),
      .partition = std::nullopt,
      .payload = std::move(payload),
  };
}

[[nodiscard]] EngineOutputRecord make_order_rejected_reply(
    const InboundEngineRecord& record,
    const cex::adapter::PlaceOrderInput& input,
    std::string reason) {
  PayloadFields payload{
      {"order_id", number_value(input.order_id)},
      {"reason", std::move(reason)},
  };
  if (!input.reservation_id.empty()) {
    payload.emplace("reservation_id", input.reservation_id);
  }
  payload.emplace("request_id", input.envelope.request_id);
  add_runtime_source_fields(payload, record, input.input_id);

  return EngineOutputRecord{
      .topic = EngineRepliesTopic,
      .type = "OrderRejected",
      .key = input.envelope.request_id,
      .partition = input.envelope.reply_partition,
      .payload = std::move(payload),
  };
}

[[nodiscard]] EngineOutputRecord make_reservation_released_event(
    const InboundEngineRecord& record,
    const cex::adapter::PlaceOrderInput& input,
    std::int64_t released_amount,
    std::string reason,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"reservation_id", input.reservation_id},
      {"user_id", number_value(input.envelope.user_id)},
      {"asset", input.margin_asset.empty() ? "USDC" : input.margin_asset},
      {"released_amount", number_value(released_amount)},
      {"reason", std::move(reason)},
  };
  return make_runtime_market_event("ReservationReleased",
                                   input.market_id,
                                   record,
                                   input.input_id,
                                   market_sequences,
                                   clock,
                                   std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_reduce_only_expired_event(
    const InboundEngineRecord& record,
    const cex::adapter::OrderMetadata& metadata,
    std::int64_t expired_quantity,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"order_id", number_value(metadata.order_id)},
      {"reservation_id", metadata.reservation_id},
      {"user_id", number_value(metadata.user_id)},
      {"expired_quantity", number_value(expired_quantity)},
      {"released_amount", number_value(metadata.remaining_reserved_margin)},
      {"reason", "REDUCE_ONLY_REMAINDER"},
  };
  return make_runtime_market_event("OrderExpired",
                                   metadata.market_id,
                                   record,
                                   metadata.place_input_id,
                                   market_sequences,
                                   clock,
                                   std::move(payload));
}

[[nodiscard]] cex::adapter::AdapterPrice settlement_mark_price_for(
    const IsolatedPositionState& position,
    const std::unordered_map<cex::adapter::MarketId, MarkPriceState>&
        mark_prices) {
  const auto mark = mark_prices.find(position.market_id);
  return mark == mark_prices.end() ? position.average_entry_price
                                   : mark->second.mark_price;
}

[[nodiscard]] std::string settlement_asset_for(
    const IsolatedPositionState& position) {
  return position.margin_asset.empty() ? "USDC" : position.margin_asset;
}

[[nodiscard]] EngineOutputRecord make_funding_payment_applied_event(
    const InboundEngineRecord& record,
    const cex::adapter::FundingSettlementTickInput& input,
    PayloadValue::Array payments,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"funding_interval_id", input.funding_interval_id},
      {"payments", PayloadValue::array(std::move(payments))},
  };
  return make_runtime_market_event("FundingPaymentApplied",
                                   input.market_id,
                                   record,
                                   input.input_id,
                                   market_sequences,
                                   clock,
                                   std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_runtime_risk_state_updated_event(
    const InboundEngineRecord& record,
    const std::optional<std::string>& input_id,
    const IsolatedRiskState& risk,
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
  return make_runtime_market_event("RiskStateUpdated",
                                   risk.market_id,
                                   record,
                                   input_id,
                                   market_sequences,
                                   clock,
                                   std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_liquidation_reply(
    std::string type,
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    PayloadFields payload) {
  payload.emplace("request_id", input.envelope.request_id);
  add_runtime_source_fields(payload, record, input.input_id);

  return EngineOutputRecord{
      .topic = EngineRepliesTopic,
      .type = std::move(type),
      .key = input.envelope.request_id,
      .partition = input.envelope.reply_partition,
      .payload = std::move(payload),
  };
}

[[nodiscard]] EngineOutputRecord make_liquidation_rejected_reply(
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    std::string reason) {
  PayloadFields payload{
      {"liquidation_id", input.liquidation_id},
      {"reason", std::move(reason)},
  };
  return make_liquidation_reply(
      "LiquidationRejected", record, input, std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_liquidation_accepted_reply(
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    const cex::adapter::OrderMetadata& metadata) {
  PayloadFields payload{
      {"liquidation_id", input.liquidation_id},
      {"order_id", text(metadata.order_id)},
  };
  if (!metadata.reservation_id.empty()) {
    payload.emplace("reservation_id", metadata.reservation_id);
  }
  return make_liquidation_reply(
      "LiquidationAccepted", record, input, std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_liquidation_started_event(
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    const IsolatedPositionState& position,
    const IsolatedRiskState& risk,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"liquidation_id", input.liquidation_id},
      {"user_id", number_value(input.liquidated_user_id)},
      {"position_id", position_id_text(input.liquidated_user_id,
                                        input.market_id)},
      {"side", position_side_text(position.signed_quantity)},
      {"quantity", number_value(abs_quantity(position.signed_quantity))},
      {"mark_price", number_value(risk.mark_price)},
      {"maintenance_margin", number_value(risk.maintenance_margin)},
      {"equity", number_value(risk.equity)},
      {"reason", "MAINTENANCE_MARGIN_BREACH"},
  };
  return make_runtime_market_event("LiquidationStarted",
                                   input.market_id,
                                   record,
                                   input.input_id,
                                   market_sequences,
                                   clock,
                                   std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_liquidation_completed_event(
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    std::int64_t remaining_quantity,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"liquidation_id", input.liquidation_id},
      {"user_id", number_value(input.liquidated_user_id)},
      {"position_id", position_id_text(input.liquidated_user_id,
                                        input.market_id)},
      {"final_status", remaining_quantity == 0 ? "FLAT" : "PARTIAL"},
      {"remaining_quantity", number_value(remaining_quantity)},
      {"insurance_fund_delta", number_value(0)},
      {"bad_debt", number_value(0)},
  };
  return make_runtime_market_event("LiquidationCompleted",
                                   input.market_id,
                                   record,
                                   input.input_id,
                                   market_sequences,
                                   clock,
                                   std::move(payload));
}

[[nodiscard]] std::string liquidation_rejection_reason(
    const cex::adapter::LiquidatePositionInput& input,
    const IsolatedPositionMap& positions,
    const IsolatedRiskMap& risk_states) {
  const PositionRiskKey key{
      .user_id = input.liquidated_user_id,
      .market_id = input.market_id,
  };
  const auto position = positions.find(key);
  if (position == positions.end() || position->second.signed_quantity == 0) {
    return "position is not liquidatable";
  }
  if (!side_matches_position(input.position_side,
                             position->second.signed_quantity)) {
    return "position side mismatch";
  }

  const auto risk = risk_states.find(key);
  if (risk == risk_states.end()) {
    return "position is not liquidatable";
  }
  if (risk->second.status != "LIQUIDATABLE" &&
      risk->second.equity > risk->second.maintenance_margin) {
    return "position is not liquidatable";
  }
  return {};
}

[[nodiscard]] cex::adapter::PlaceOrderInput synthetic_liquidation_order(
    const cex::adapter::LiquidatePositionInput& input,
    const IsolatedPositionState& position,
    cex::adapter::AdapterQuantity quantity,
    cex::adapter::AdapterPrice price) {
  auto envelope = input.envelope;
  envelope.request_id = "liquidation-core-" + input.liquidation_id;
  envelope.idempotency_key = "liquidation-core-" + input.liquidation_id;
  envelope.user_id = input.liquidated_user_id;

  return cex::adapter::PlaceOrderInput{
      .input_id = input.input_id.has_value()
                      ? std::optional<std::string>{"liquidation-core-" +
                                                   *input.input_id}
                      : std::nullopt,
      .envelope = std::move(envelope),
      .order_id = liquidation_order_id(input.liquidation_id),
      .reservation_id = "liquidation-" + input.liquidation_id,
      .market_id = input.market_id,
      .market_name = input.market_name,
      .side = liquidation_order_side(position.signed_quantity),
      .order_type = cex::adapter::AdapterOrderType::Limit,
      .time_in_force = cex::adapter::AdapterTimeInForce::Ioc,
      .price = price,
      .quantity = quantity,
      .reduce_only = true,
      .margin_asset = position.margin_asset.empty() ? "USDC"
                                                    : position.margin_asset,
      .reserved_margin_amount = 0,
      .leverage = position.leverage,
      .source = input.source,
  };
}

[[nodiscard]] std::string automatic_liquidation_id(
    const InboundEngineRecord& source,
    const std::optional<std::string>& source_input_id,
    const PositionRiskKey& key,
    std::int64_t attempt) {
  std::string id = "auto-liq-" + text(key.market_id) + "-" +
                   text(key.user_id) + "-" + text(source.partition) + "-" +
                   text(source.offset) + "-" + text(attempt);
  if (source_input_id.has_value()) {
    id += "-" + *source_input_id;
  }
  return id;
}

[[nodiscard]] cex::adapter::LiquidatePositionInput
automatic_liquidation_input(
    const InboundEngineRecord& source,
    const std::optional<std::string>& source_input_id,
    const AutomaticLiquidationCandidate& candidate,
    std::int64_t attempt) {
  const auto liquidation_id =
      automatic_liquidation_id(source, source_input_id, candidate.key, attempt);
  return cex::adapter::LiquidatePositionInput{
      .input_id = source_input_id,
      .envelope = cex::adapter::CommandEnvelope{
          .request_id = "auto-req-" + liquidation_id,
          .idempotency_key = "auto-idem-" + liquidation_id,
          .user_id = 0,
          .reply_partition = 0,
      },
      .liquidation_id = liquidation_id,
      .market_id = candidate.key.market_id,
      .market_name = "",
      .liquidated_user_id = candidate.key.user_id,
      .position_side =
          position_adapter_side(candidate.position.signed_quantity),
      .quantity = abs_quantity(candidate.position.signed_quantity),
      .price = 0,
      .request_source = std::string{"ENGINE_AUTO"},
      .source = broker_context(source),
  };
}

[[nodiscard]] std::string adl_id_for(const std::string& liquidation_id,
                                     std::int64_t priority_rank,
                                     cex::adapter::AdapterUserId user_id) {
  return liquidation_id + "-adl-" + text(priority_rank) + "-" + text(user_id);
}

[[nodiscard]] OrderId adl_counterparty_order_id(
    const std::string& adl_id,
    OrderId liquidation_order_id,
    const cex::adapter::OrderMetadataStore& metadata_store) {
  for (std::int64_t salt = 0; salt < 1'024; ++salt) {
    const auto order_id =
        cex::adapter::command_id_from_request_id("adl-order-" + adl_id +
                                                 "-" + text(salt));
    if (order_id != liquidation_order_id &&
        metadata_store.find(order_id) == nullptr) {
      return order_id;
    }
  }
  throw std::runtime_error("unable to allocate deterministic ADL order id");
}

[[nodiscard]] cex::adapter::OrderMetadata adl_counterparty_metadata(
    const cex::adapter::LiquidatePositionInput& input,
    const AdlCandidate& candidate,
    OrderId order_id,
    std::int64_t fill_quantity,
    std::int64_t priority_rank,
    const std::string& adl_id) {
  const auto stable_id = cex::adapter::command_id_from_request_id(adl_id);
  return cex::adapter::OrderMetadata{
      .order_id = order_id,
      .market_id = input.market_id,
      .user_id = candidate.key.user_id,
      .side = liquidation_order_side(candidate.position.signed_quantity),
      .original_quantity = fill_quantity,
      .remaining_quantity = fill_quantity,
      .reduce_only = true,
      .margin_asset = candidate.position.margin_asset.empty()
                          ? "USDC"
                          : candidate.position.margin_asset,
      .reserved_margin_amount = 0,
      .remaining_reserved_margin = 0,
      .leverage = candidate.position.leverage,
      .reservation_id = "adl-" + input.liquidation_id + "-" +
                        text(candidate.key.user_id) + "-" +
                        text(priority_rank),
      .place_request_id = adl_id,
      .place_idempotency_key = adl_id,
      .place_input_id = std::nullopt,
      .reply_partition = input.envelope.reply_partition,
      .core_client_order_id = stable_id,
      .core_place_command_id = stable_id,
  };
}

[[nodiscard]] TradeExecuted adl_trade_event(
    const cex::adapter::LiquidatePositionInput& input,
    const EngineEventTranslationContext& context,
    const std::string& adl_id,
    OrderId counterparty_order_id,
    std::int64_t fill_quantity,
    cex::adapter::AdapterPrice price) {
  if (!context.pending_metadata.has_value()) {
    throw std::runtime_error("ADL requires liquidation order metadata");
  }
  const auto trade_id = cex::adapter::command_id_from_request_id(
      "adl-fill-" + adl_id);
  return TradeExecuted{
      .eventId = trade_id,
      .sequence = 0,
      .tradeId = trade_id,
      .symbolId = static_cast<SymbolId>(input.market_id),
      .makerOrderId = counterparty_order_id,
      .takerOrderId = context.pending_metadata->order_id,
      .price = Price::from_ticks(price),
      .quantity = Quantity::from_lots(static_cast<std::uint64_t>(fill_quantity)),
  };
}

void append_process_result(EngineProcessResult& result,
                           const EngineProcessResult& translated) {
  result.replies.insert(result.replies.end(),
                        translated.replies.begin(),
                        translated.replies.end());
  result.events.insert(result.events.end(),
                       translated.events.begin(),
                       translated.events.end());
}

[[nodiscard]] std::int64_t append_adl_fills(
    EngineProcessResult& result,
    const cex::adapter::LiquidatePositionInput& input,
    EngineEventTranslationContext& context,
    std::int64_t remaining_quantity,
    cex::adapter::AdapterPrice price,
    const EngineEventTranslator& translator,
    cex::adapter::OrderMetadataStore& metadata_store,
    IsolatedPositionMap& positions,
    IsolatedRiskMap& risk_states,
    const std::unordered_map<cex::adapter::MarketId, MarkPriceState>&
        mark_prices,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const PositionRiskKey liquidated_key{
      .user_id = input.liquidated_user_id,
      .market_id = input.market_id,
  };
  const auto liquidated_position = positions.find(liquidated_key);
  if (liquidated_position == positions.end() ||
      liquidated_position->second.signed_quantity == 0 ||
      remaining_quantity <= 0) {
    return 0;
  }

  const auto candidates = adl_candidates_for(
      input, liquidated_position->second, positions, risk_states);
  std::int64_t executed_quantity = 0;
  std::int64_t priority_rank = 1;

  const auto previous_adl_execution = context.adl_execution;
  const auto previous_adl_id = context.adl_id;
  const auto previous_adl_counterparty_user_id =
      context.adl_counterparty_user_id;
  const auto previous_adl_priority_rank = context.adl_priority_rank;

  for (const auto& candidate : candidates) {
    if (remaining_quantity <= 0) {
      break;
    }

    const auto fill_quantity =
        std::min(remaining_quantity, candidate.available_quantity);
    if (fill_quantity <= 0) {
      ++priority_rank;
      continue;
    }

    const auto adl_id =
        adl_id_for(input.liquidation_id, priority_rank, candidate.key.user_id);
    const auto liquidation_order_id =
        context.pending_metadata.has_value()
            ? context.pending_metadata->order_id
            : OrderId{0};
    const auto counterparty_order_id =
        adl_counterparty_order_id(adl_id,
                                  liquidation_order_id,
                                  metadata_store);
    auto metadata = adl_counterparty_metadata(input,
                                              candidate,
                                              counterparty_order_id,
                                              fill_quantity,
                                              priority_rank,
                                              adl_id);
    if (!metadata_store.insert(std::move(metadata))) {
      throw std::runtime_error("ADL counterparty metadata already exists");
    }

    context.adl_execution = true;
    context.adl_id = adl_id;
    context.adl_counterparty_user_id = candidate.key.user_id;
    context.adl_priority_rank = priority_rank;

    const std::vector<EngineEvent> adl_events{
        adl_trade_event(input,
                        context,
                        adl_id,
                        counterparty_order_id,
                        fill_quantity,
                        price),
    };
    auto translated = translator.translate(adl_events,
                                           context,
                                           metadata_store,
                                           positions,
                                           risk_states,
                                           mark_prices,
                                           market_sequences,
                                           clock);
    append_process_result(result, translated);
    (void)metadata_store.erase(counterparty_order_id);

    remaining_quantity -= fill_quantity;
    executed_quantity += fill_quantity;
    ++priority_rank;
  }

  context.adl_execution = previous_adl_execution;
  context.adl_id = previous_adl_id;
  context.adl_counterparty_user_id = previous_adl_counterparty_user_id;
  context.adl_priority_rank = previous_adl_priority_rank;
  return executed_quantity;
}

[[nodiscard]] EngineProcessResult rejected_no_output() {
  EngineProcessResult result;
  result.status = EngineProcessStatus::Rejected;
  return result;
}

[[nodiscard]] EngineProcessResult reduce_only_rejected_result(
    const InboundEngineRecord& record,
    const cex::adapter::PlaceOrderInput& input,
    const std::string& reason,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  EngineProcessResult result;
  result.replies.push_back(make_order_rejected_reply(record, input, reason));
  if (!input.reservation_id.empty() && input.reserved_margin_amount > 0) {
    result.events.push_back(make_reservation_released_event(
        record,
        input,
        input.reserved_margin_amount,
        "ORDER_REJECTED_AFTER_RESERVATION",
        market_sequences,
        clock));
  }
  return result;
}

[[nodiscard]] std::int64_t reduce_only_filled_quantity(
    OrderId order_id,
    const std::vector<EngineEvent>& core_events) {
  std::int64_t filled = 0;
  for (const auto& event : core_events) {
    if (const auto* trade = std::get_if<TradeExecuted>(&event);
        trade != nullptr && trade->takerOrderId == order_id) {
      filled += static_cast<std::int64_t>(trade->quantity.lots());
    }
  }
  return filled;
}

EngineProcessResult apply_processing_mode(EngineProcessResult result,
                                          ProcessingMode mode) {
  if (mode == ProcessingMode::ReplaySilent) {
    result.replies.clear();
    result.events.clear();
  }
  return result;
}

}  // namespace

EngineRuntime::EngineRuntime(EngineRuntimeConfig config)
    : market_sequences_(config.first_public_sequence),
      config_(std::move(config)),
      clock_(config_.clock ? config_.clock : EngineRuntimeClock{system_timestamp_ms}) {
  for (const auto& symbol : config_.symbols) {
    core_.add_symbol(symbol);
  }
}

void EngineRuntime::add_symbol(const SymbolConfig& symbol_config) {
  core_.add_symbol(symbol_config);
}

std::optional<EngineProcessResult> EngineRuntime::duplicate_result_for(
    const std::optional<std::string>& input_id,
    const std::string& idempotency_key) const {
  if (input_id.has_value()) {
    const auto input_it = processed_input_ids_.find(*input_id);
    if (input_it != processed_input_ids_.end()) {
      return make_duplicate_result(
          EngineDuplicateReason::InputId, *input_id, input_it->second);
    }
  }

  const auto idempotency_it =
      processed_idempotency_keys_.find(idempotency_key);
  if (idempotency_it != processed_idempotency_keys_.end()) {
    return make_duplicate_result(EngineDuplicateReason::IdempotencyKey,
                                 idempotency_key,
                                 idempotency_it->second);
  }

  return std::nullopt;
}

std::optional<EngineProcessResult> EngineRuntime::duplicate_input_result_for(
    const std::optional<std::string>& input_id) const {
  if (!input_id.has_value()) {
    return std::nullopt;
  }

  const auto input_it = processed_input_ids_.find(*input_id);
  if (input_it == processed_input_ids_.end()) {
    return std::nullopt;
  }
  return make_duplicate_result(
      EngineDuplicateReason::InputId, *input_id, input_it->second);
}

EngineProcessResult EngineRuntime::make_duplicate_result(
    EngineDuplicateReason reason,
    const std::string& key,
    const ProcessedRuntimeRequest& original) const {
  EngineProcessResult result;
  result.status = EngineProcessStatus::Duplicate;
  result.duplicate = EngineDuplicateInfo{
      .reason = reason,
      .key = key,
      .original_topic = original.topic,
      .original_partition = original.partition,
      .original_offset = original.offset,
      .original_input_id = original.input_id,
      .original_idempotency_key = original.idempotency_key,
  };
  return result;
}

void EngineRuntime::mark_processed(
    RuntimeCommandKind command_kind,
    const InboundEngineRecord& record,
    const std::optional<std::string>& input_id,
    const std::string& idempotency_key) {
  ProcessedRuntimeRequest processed{
      .command_kind = command_kind,
      .topic = record.topic,
      .partition = record.partition,
      .offset = record.offset,
      .input_id = input_id,
      .idempotency_key = idempotency_key,
  };

  if (input_id.has_value()) {
    (void)processed_input_ids_.emplace(*input_id, processed);
  }
  (void)processed_idempotency_keys_.emplace(idempotency_key,
                                            std::move(processed));
}

void EngineRuntime::mark_input_processed(
    RuntimeCommandKind command_kind,
    const InboundEngineRecord& record,
    const std::optional<std::string>& input_id) {
  if (!input_id.has_value()) {
    return;
  }

  ProcessedRuntimeRequest processed{
      .command_kind = command_kind,
      .topic = record.topic,
      .partition = record.partition,
      .offset = record.offset,
      .input_id = input_id,
      .idempotency_key = "",
  };

  (void)processed_input_ids_.emplace(*input_id, std::move(processed));
}

EngineProcessResult EngineRuntime::execute_liquidation(
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    bool emit_replies) {
  EngineProcessResult result;
  const auto rejection_reason =
      liquidation_rejection_reason(input, positions_, risk_states_);
  if (!rejection_reason.empty()) {
    if (emit_replies) {
      result.replies.push_back(
          make_liquidation_rejected_reply(record, input, rejection_reason));
    }
    return result;
  }

  const PositionRiskKey key{
      .user_id = input.liquidated_user_id,
      .market_id = input.market_id,
  };
  const auto previous_position = positions_.at(key);
  const auto previous_risk = risk_states_.at(key);
  const auto order_quantity = liquidation_quantity(input, previous_position);
  const auto order_price =
      liquidation_price(input, previous_position, mark_prices_);
  const auto order_side =
      liquidation_order_side(previous_position.signed_quantity);
  const bool has_crossing_liquidity =
      crossing_liquidity_exists(core_,
                                input.market_id,
                                order_side,
                                order_price);
  const auto initial_adl_candidates =
      adl_candidates_for(input, previous_position, positions_, risk_states_);
  const auto initial_adl_available =
      total_adl_available(initial_adl_candidates, order_quantity);
  if (order_quantity <= 0 || order_price <= 0 ||
      (!has_crossing_liquidity && initial_adl_available <= 0)) {
    if (emit_replies) {
      result.replies.push_back(make_liquidation_rejected_reply(
          record, input, "no liquidation liquidity"));
    }
    return result;
  }

  auto synthetic =
      synthetic_liquidation_order(input,
                                  previous_position,
                                  order_quantity,
                                  order_price);
  auto mapped =
      cex::adapter::map_place_order_to_core(synthetic, received_sequence(record));
  if (!mapped.metadata_to_record.has_value()) {
    throw std::runtime_error(
        "liquidation order mapping did not produce metadata");
  }

  EngineEventTranslationContext context{
      .command_kind = RuntimeCommandKind::LiquidatePosition,
      .source = record,
      .request_id = input.envelope.request_id,
      .input_id = input.input_id,
      .reply_partition = input.envelope.reply_partition,
      .order_id = synthetic.order_id,
      .market_id = input.market_id,
      .can_open_resting_order = false,
      .pending_metadata = *mapped.metadata_to_record,
      .liquidation_id = input.liquidation_id,
      .liquidated_user_id = input.liquidated_user_id,
      .liquidation_position_side = input.position_side,
  };

  std::vector<EngineEvent> core_events;
  if (has_crossing_liquidity) {
    core_events = core_.process(mapped.command);
  }
  const bool has_core_trade = has_trade_executed(core_events);
  if (has_crossing_liquidity && !has_core_trade &&
      initial_adl_available <= 0) {
    if (emit_replies) {
      result.replies.push_back(make_liquidation_rejected_reply(
          record, input, "no liquidation liquidity"));
    }
    return result;
  }

  result.events.push_back(make_liquidation_started_event(record,
                                                         input,
                                                         previous_position,
                                                         previous_risk,
                                                         market_sequences_,
                                                         clock_));
  if (has_crossing_liquidity) {
    auto translated = translator_.translate(core_events,
                                            context,
                                            metadata_store_,
                                            positions_,
                                            risk_states_,
                                            mark_prices_,
                                            market_sequences_,
                                            clock_);
    append_process_result(result, translated);
    cleanup_completed_order_metadata(metadata_store_, core_events);
  } else if (emit_replies) {
    result.replies.push_back(make_liquidation_accepted_reply(
        record, input, *mapped.metadata_to_record));
  }

  const auto remaining_position = positions_.find(key);
  const auto pending_remaining =
      context.pending_metadata.has_value()
          ? context.pending_metadata->remaining_quantity
          : std::int64_t{0};
  const auto remaining_adl_quantity =
      remaining_position == positions_.end()
          ? 0
          : std::min(pending_remaining,
                     abs_quantity(remaining_position->second.signed_quantity));
  if (remaining_adl_quantity > 0) {
    (void)append_adl_fills(result,
                           input,
                           context,
                           remaining_adl_quantity,
                           order_price,
                           translator_,
                           metadata_store_,
                           positions_,
                           risk_states_,
                           mark_prices_,
                           market_sequences_,
                           clock_);
  }

  const auto final_position = positions_.find(key);
  const auto remaining_quantity =
      final_position == positions_.end()
          ? 0
          : abs_quantity(final_position->second.signed_quantity);
  result.events.push_back(make_liquidation_completed_event(record,
                                                           input,
                                                           remaining_quantity,
                                                           market_sequences_,
                                                           clock_));

  if (!emit_replies) {
    result.replies.clear();
  }
  return result;
}

EngineProcessResult EngineRuntime::evaluate_automatic_liquidations_for_market(
    cex::adapter::MarketId market_id,
    const InboundEngineRecord& source,
    const std::optional<std::string>& source_input_id) {
  EngineProcessResult result;
  std::set<PositionRiskKey> attempted;
  std::int64_t attempts = 0;

  while (attempts < kMaxAutomaticLiquidationAttemptsPerTrigger) {
    const auto candidates = automatic_liquidation_candidates_for(
        market_id, positions_, risk_states_, attempted);
    if (candidates.empty()) {
      break;
    }

    bool should_rescan = false;
    for (const auto& candidate : candidates) {
      if (attempts >= kMaxAutomaticLiquidationAttemptsPerTrigger) {
        break;
      }

      attempted.insert(candidate.key);
      ++attempts;
      auto input = automatic_liquidation_input(source,
                                               source_input_id,
                                               candidate,
                                               attempts);
      auto execution = execute_liquidation(source, input, false);
      const bool made_progress = !execution.events.empty();
      append_process_result(result, execution);
      if (made_progress) {
        should_rescan = true;
        break;
      }
    }

    if (!should_rescan) {
      break;
    }
  }

  return result;
}

EngineProcessResult EngineRuntime::evaluate_all_automatic_liquidations(
    const InboundEngineRecord& source,
    const std::optional<std::string>& source_input_id) {
  EngineProcessResult result;
  std::set<cex::adapter::MarketId> markets;
  for (const auto& [key, risk] : risk_states_) {
    if (risk.status == "LIQUIDATABLE" && risk.signed_quantity != 0) {
      markets.insert(key.market_id);
    }
  }

  for (const auto market_id : markets) {
    auto market_result = evaluate_automatic_liquidations_for_market(
        market_id, source, source_input_id);
    append_process_result(result, market_result);
  }
  return result;
}

EngineProcessResult EngineRuntime::evaluate_automatic_liquidations_for_market(
    cex::adapter::MarketId market_id,
    const InboundEngineRecord& source,
    ProcessingMode mode) {
  auto result = evaluate_automatic_liquidations_for_market(
      market_id, source, std::optional<std::string>{});
  return apply_processing_mode(std::move(result), mode);
}

EngineProcessResult EngineRuntime::evaluate_all_automatic_liquidations(
    const InboundEngineRecord& source,
    ProcessingMode mode) {
  auto result =
      evaluate_all_automatic_liquidations(source, std::optional<std::string>{});
  return apply_processing_mode(std::move(result), mode);
}

void EngineRuntime::append_mark_price_risk_updates(
    EngineProcessResult& result,
    const InboundEngineRecord& record,
    const std::optional<std::string>& input_id,
    cex::adapter::MarketId market_id,
    cex::adapter::AdapterPrice mark_price,
    std::int64_t updated_at_ms) {
  std::vector<PositionRiskKey> updated_keys;
  for (const auto& [key, position] : positions_) {
    if (position.market_id != market_id || position.signed_quantity == 0) {
      continue;
    }

    const auto previous_risk = risk_states_.find(key);
    const auto risk = risk_from_position(
        position,
        mark_price,
        updated_at_ms,
        previous_risk == risk_states_.end() ? nullptr : &previous_risk->second);
    risk_states_[key] = risk;
    updated_keys.push_back(key);
  }

  for (const auto& key : updated_keys) {
    result.events.push_back(make_runtime_risk_state_updated_event(
        record,
        input_id,
        risk_states_.at(key),
        market_sequences_,
        clock_));
  }
}

EngineProcessResult EngineRuntime::process(const InboundEngineRecord& record,
                                           ProcessingMode mode) {
  ParsedEngineInput parsed = parser_.parse(record.raw_json);

  if (parsed.kind == ParsedEngineInputKind::PlaceOrder) {
    auto input = std::get<cex::adapter::PlaceOrderInput>(parsed.value);
    if (auto duplicate = duplicate_result_for(
            input.input_id, input.envelope.idempotency_key);
        duplicate.has_value()) {
      // Duplicate detection runs before mode-specific output handling. During
      // replay, restored processed maps mean the original command is already
      // part of state, so duplicates stay silent and are not re-applied.
      return *duplicate;
    }

    input.source = broker_context(record);

    const auto reduce_only = check_reduce_only_order(input, positions_);
    if (!reduce_only.allowed) {
      auto result = reduce_only_rejected_result(record,
                                                input,
                                                reduce_only.reason,
                                                market_sequences_,
                                                clock_);
      mark_processed(RuntimeCommandKind::PlaceOrder,
                     record,
                     input.input_id,
                     input.envelope.idempotency_key);
      return apply_processing_mode(std::move(result), mode);
    }

    auto core_input = input;
    if (input.reduce_only) {
      core_input.quantity = reduce_only.capped_quantity;
      core_input.time_in_force = cex::adapter::AdapterTimeInForce::Ioc;
    }

    auto mapped = cex::adapter::map_place_order_to_core(
        core_input, received_sequence(record));
    if (!mapped.metadata_to_record.has_value()) {
      throw std::runtime_error("place order mapping did not produce metadata");
    }
    if (input.reduce_only) {
      mapped.metadata_to_record->original_quantity = input.quantity;
      mapped.metadata_to_record->remaining_quantity = input.quantity;
    }

    auto context =
        make_translation_context(record, core_input, *mapped.metadata_to_record);
    auto core_events = core_.process(mapped.command);
    auto result = translator_.translate(core_events,
                                        context,
                                        metadata_store_,
                                        positions_,
                                        risk_states_,
                                        mark_prices_,
                                        market_sequences_,
                                        clock_);

    if (has_order_accepted(core_events)) {
      if (!context.pending_metadata.has_value()) {
        throw std::runtime_error("accepted order metadata was not available");
      }
      if (!metadata_store_.insert(*context.pending_metadata)) {
        throw std::runtime_error("accepted order metadata already exists");
      }
    }
    cleanup_completed_order_metadata(metadata_store_, core_events);
    if (input.reduce_only && has_order_accepted(core_events) &&
        context.pending_metadata.has_value()) {
      const auto filled_quantity =
          reduce_only_filled_quantity(context.pending_metadata->order_id,
                                      core_events);
      const auto expired_quantity = input.quantity - filled_quantity;
      if (expired_quantity > 0) {
        result.events.push_back(make_reduce_only_expired_event(
            record,
            *context.pending_metadata,
            expired_quantity,
            market_sequences_,
            clock_));
      }
      (void)metadata_store_.erase(context.pending_metadata->order_id);
    }
    if (has_trade_executed(core_events)) {
      auto liquidations = evaluate_automatic_liquidations_for_market(
          input.market_id, record, input.input_id);
      append_process_result(result, liquidations);
    }
    mark_processed(RuntimeCommandKind::PlaceOrder,
                   record,
                   input.input_id,
                   input.envelope.idempotency_key);
    return apply_processing_mode(std::move(result), mode);
  }

  if (parsed.kind == ParsedEngineInputKind::LiquidatePosition) {
    auto input =
        std::get<cex::adapter::LiquidatePositionInput>(parsed.value);
    if (auto duplicate = duplicate_result_for(
            input.input_id, input.envelope.idempotency_key);
        duplicate.has_value()) {
      return *duplicate;
    }

    input.source = broker_context(record);

    auto result = execute_liquidation(record, input, true);
    mark_processed(RuntimeCommandKind::LiquidatePosition,
                   record,
                   input.input_id,
                   input.envelope.idempotency_key);
    return apply_processing_mode(std::move(result), mode);
  }

  if (parsed.kind == ParsedEngineInputKind::MarkPriceUpdated) {
    auto input = std::get<cex::adapter::MarkPriceUpdatedInput>(parsed.value);
    if (auto duplicate = duplicate_input_result_for(input.input_id);
        duplicate.has_value()) {
      return *duplicate;
    }

    input.source = broker_context(record);

    auto state = mark_state_from_input(input);
    EngineProcessResult result;
    result.events.push_back(make_mark_price_updated_event(
        record, input, market_sequences_, clock_));
    mark_prices_[input.market_id] = std::move(state);
    append_mark_price_risk_updates(result,
                                   record,
                                   input.input_id,
                                   input.market_id,
                                   input.mark_price,
                                   input.published_at_ms);
    auto liquidations = evaluate_automatic_liquidations_for_market(
        input.market_id, record, input.input_id);
    append_process_result(result, liquidations);
    mark_input_processed(RuntimeCommandKind::MarkPriceUpdated,
                         record,
                         input.input_id);
    return apply_processing_mode(std::move(result), mode);
  }

  if (parsed.kind == ParsedEngineInputKind::FundingRateUpdated) {
    auto input =
        std::get<cex::adapter::FundingRateUpdatedInput>(parsed.value);
    if (auto duplicate = duplicate_input_result_for(input.input_id);
        duplicate.has_value()) {
      return *duplicate;
    }

    input.source = broker_context(record);

    auto state = funding_rate_state_from_input(input);
    EngineProcessResult result;
    result.events.push_back(make_funding_rate_updated_event(
        record, input, market_sequences_, clock_));
    funding_rates_[input.market_id] = std::move(state);
    mark_input_processed(RuntimeCommandKind::FundingRateUpdated,
                         record,
                         input.input_id);
    return apply_processing_mode(std::move(result), mode);
  }

  if (parsed.kind == ParsedEngineInputKind::FundingSettlementTick) {
    auto input =
        std::get<cex::adapter::FundingSettlementTickInput>(parsed.value);
    if (auto duplicate = duplicate_input_result_for(input.input_id);
        duplicate.has_value()) {
      return *duplicate;
    }

    input.source = broker_context(record);

    const FundingSettlementKey settlement_key{
        .market_id = input.market_id,
        .funding_interval_id = input.funding_interval_id,
    };
    if (settled_funding_intervals_.contains(settlement_key)) {
      mark_input_processed(RuntimeCommandKind::FundingSettlementTick,
                           record,
                           input.input_id);
      return EngineProcessResult{};
    }

    const auto funding = funding_rates_.find(input.market_id);
    if (funding == funding_rates_.end() ||
        funding->second.funding_interval_id != input.funding_interval_id) {
      mark_input_processed(RuntimeCommandKind::FundingSettlementTick,
                           record,
                           input.input_id);
      return rejected_no_output();
    }

    EngineProcessResult result;
    PayloadValue::Array payments;
    std::vector<PositionRiskKey> paid_keys;

    for (const auto& [key, position] : positions_) {
      if (position.market_id != input.market_id ||
          position.signed_quantity == 0) {
        continue;
      }

      const auto mark_price =
          settlement_mark_price_for(position, mark_prices_);
      const auto amount =
          -((position.signed_quantity * mark_price * funding->second.rate) /
            funding->second.rate_scale);
      if (amount == 0) {
        continue;
      }

      payments.push_back(PayloadValue::object(PayloadValue::Object{
          {"user_id", number_value(position.user_id)},
          {"position_id", position_id_text(position.user_id,
                                           position.market_id)},
          {"side", position_side_text(position.signed_quantity)},
          {"asset", settlement_asset_for(position)},
          {"amount", number_value(amount)},
      }));

      const auto previous_risk = risk_states_.find(key);
      auto risk = risk_from_position(
          position,
          mark_price,
          input.settle_at_ms,
          previous_risk == risk_states_.end() ? nullptr : &previous_risk->second);
      risk.equity += amount;
      risk.margin_ratio =
          margin_ratio_for(risk.equity, risk.maintenance_margin);
      if (risk.signed_quantity == 0) {
        risk.status = "FLAT";
      } else if (risk.equity <= risk.maintenance_margin) {
        risk.status = "LIQUIDATABLE";
      } else {
        risk.status = "HEALTHY";
      }
      risk_states_[key] = risk;
      paid_keys.push_back(key);
    }

    result.events.push_back(make_funding_payment_applied_event(
        record, input, std::move(payments), market_sequences_, clock_));
    for (const auto& key : paid_keys) {
      result.events.push_back(make_runtime_risk_state_updated_event(
          record,
          input.input_id,
          risk_states_.at(key),
          market_sequences_,
          clock_));
    }

    settled_funding_intervals_.insert(settlement_key);
    auto liquidations = evaluate_automatic_liquidations_for_market(
        input.market_id, record, input.input_id);
    append_process_result(result, liquidations);
    mark_input_processed(RuntimeCommandKind::FundingSettlementTick,
                         record,
                         input.input_id);
    return apply_processing_mode(std::move(result), mode);
  }

  auto input = std::get<cex::adapter::CancelOrderInput>(parsed.value);
  if (auto duplicate = duplicate_result_for(input.input_id,
                                            input.envelope.idempotency_key);
      duplicate.has_value()) {
    // See the place-order duplicate branch for replay behavior.
    return *duplicate;
  }

  input.source = broker_context(record);

  auto mapped =
      cex::adapter::map_cancel_order_to_core(input, received_sequence(record));
  auto context = make_translation_context(record, input);
  auto core_events = core_.process(mapped.command);
  auto result = translator_.translate(core_events,
                                      context,
                                      metadata_store_,
                                      positions_,
                                      risk_states_,
                                      mark_prices_,
                                      market_sequences_,
                                      clock_);

  cleanup_completed_order_metadata(metadata_store_, core_events);
  mark_processed(RuntimeCommandKind::CancelOrder,
                 record,
                 input.input_id,
                 input.envelope.idempotency_key);
  return apply_processing_mode(std::move(result), mode);
}

EngineProcessResult EngineRuntime::process_replay(
    const InboundEngineRecord& record) {
  return process(record, ProcessingMode::ReplaySilent);
}

const EngineCore& EngineRuntime::core() const noexcept {
  return core_;
}

const cex::adapter::OrderMetadataStore& EngineRuntime::metadata_store()
    const noexcept {
  return metadata_store_;
}

const cex::adapter::MarketSequenceGenerator& EngineRuntime::market_sequences()
    const noexcept {
  return market_sequences_;
}

const std::unordered_map<cex::adapter::MarketId, MarkPriceState>&
EngineRuntime::mark_prices() const noexcept {
  return mark_prices_;
}

const std::unordered_map<cex::adapter::MarketId, FundingRateState>&
EngineRuntime::funding_rates() const noexcept {
  return funding_rates_;
}

const FundingSettlementSet& EngineRuntime::settled_funding_intervals()
    const noexcept {
  return settled_funding_intervals_;
}

const IsolatedPositionMap& EngineRuntime::positions() const noexcept {
  return positions_;
}

const IsolatedRiskMap& EngineRuntime::risk_states() const noexcept {
  return risk_states_;
}

EngineRuntimeStateSnapshot EngineRuntime::snapshot_state() const {
  EngineRuntimeStateSnapshot snapshot{
      .core_snapshot = core_.snapshot(),
      .metadata_store = metadata_store_,
      .public_sequences = market_sequences_.snapshot(),
      .mark_prices = mark_prices_,
      .funding_rates = funding_rates_,
      .settled_funding_intervals = settled_funding_intervals_,
      .positions = positions_,
      .risk_states = risk_states_,
  };

  for (const auto& [key, processed] : processed_input_ids_) {
    snapshot.processed_input_ids.emplace(
        key,
        EngineRuntimeProcessedRequestSnapshot{
            .command_kind = processed.command_kind,
            .topic = processed.topic,
            .partition = processed.partition,
            .offset = processed.offset,
            .input_id = processed.input_id,
            .idempotency_key = processed.idempotency_key,
        });
  }

  for (const auto& [key, processed] : processed_idempotency_keys_) {
    snapshot.processed_idempotency_keys.emplace(
        key,
        EngineRuntimeProcessedRequestSnapshot{
            .command_kind = processed.command_kind,
            .topic = processed.topic,
            .partition = processed.partition,
            .offset = processed.offset,
            .input_id = processed.input_id,
            .idempotency_key = processed.idempotency_key,
        });
  }

  return snapshot;
}

void EngineRuntime::restore_state(
    const EngineRuntimeStateSnapshot& snapshot) {
  core_.restore(snapshot.core_snapshot);
  metadata_store_ = snapshot.metadata_store;

  market_sequences_ =
      cex::adapter::MarketSequenceGenerator(config_.first_public_sequence);
  for (const auto& [market_id, next_sequence] : snapshot.public_sequences) {
    market_sequences_.restore(market_id, next_sequence);
  }

  mark_prices_ = snapshot.mark_prices;
  funding_rates_ = snapshot.funding_rates;
  settled_funding_intervals_ = snapshot.settled_funding_intervals;
  positions_ = snapshot.positions;
  risk_states_ = snapshot.risk_states;

  processed_input_ids_.clear();
  for (const auto& [key, processed] : snapshot.processed_input_ids) {
    processed_input_ids_.emplace(
        key,
        ProcessedRuntimeRequest{
            .command_kind = processed.command_kind,
            .topic = processed.topic,
            .partition = processed.partition,
            .offset = processed.offset,
            .input_id = processed.input_id,
            .idempotency_key = processed.idempotency_key,
        });
  }

  processed_idempotency_keys_.clear();
  for (const auto& [key, processed] : snapshot.processed_idempotency_keys) {
    processed_idempotency_keys_.emplace(
        key,
        ProcessedRuntimeRequest{
            .command_kind = processed.command_kind,
            .topic = processed.topic,
            .partition = processed.partition,
            .offset = processed.offset,
            .input_id = processed.input_id,
            .idempotency_key = processed.idempotency_key,
        });
  }
}

EngineEventTranslationContext EngineRuntime::make_translation_context(
    const InboundEngineRecord& record,
    const cex::adapter::PlaceOrderInput& input,
    const cex::adapter::OrderMetadata& pending_metadata) const {
  return EngineEventTranslationContext{
      .command_kind = RuntimeCommandKind::PlaceOrder,
      .source = record,
      .request_id = input.envelope.request_id,
      .input_id = input.input_id,
      .reply_partition = input.envelope.reply_partition,
      .order_id = input.order_id,
      .market_id = input.market_id,
      .can_open_resting_order = can_open_resting_order(input),
      .pending_metadata = pending_metadata,
  };
}

EngineEventTranslationContext EngineRuntime::make_translation_context(
    const InboundEngineRecord& record,
    const cex::adapter::CancelOrderInput& input) const {
  return EngineEventTranslationContext{
      .command_kind = RuntimeCommandKind::CancelOrder,
      .source = record,
      .request_id = input.envelope.request_id,
      .input_id = input.input_id,
      .reply_partition = input.envelope.reply_partition,
      .order_id = input.order_id,
      .market_id = input.market_id,
      .can_open_resting_order = false,
      .pending_metadata = std::nullopt,
  };
}

}  // namespace cex::runtime
