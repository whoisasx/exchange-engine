#include "runtime/EngineRuntime.hpp"

#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

namespace cex::runtime {
namespace {

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

[[nodiscard]] std::string position_id_text(
    cex::adapter::AdapterUserId user_id,
    cex::adapter::MarketId market_id) {
  return "pos_" + text(user_id) + "_" + text(market_id);
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

[[nodiscard]] EngineOutputRecord make_liquidation_accepted_reply(
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    OrderId liquidation_order_id) {
  PayloadFields payload{
      {"liquidation_id", input.liquidation_id},
      {"order_id", number_value(liquidation_order_id)},
  };
  return make_liquidation_reply(
      "LiquidationAccepted", record, input, std::move(payload));
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

[[nodiscard]] EngineOutputRecord make_liquidation_executed_event(
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    const IsolatedPositionState& position,
    EngineSequence fill_id,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  const auto price = input.price > 0 ? input.price : position.average_entry_price;
  const auto quantity = input.quantity > 0
                            ? input.quantity
                            : abs_quantity(position.signed_quantity);
  PayloadFields payload{
      {"liquidation_id", input.liquidation_id},
      {"user_id", number_value(input.liquidated_user_id)},
      {"position_id", position_id_text(input.liquidated_user_id,
                                        input.market_id)},
      {"fill_id", number_value(fill_id)},
      {"price", number_value(price)},
      {"quantity", number_value(quantity)},
      {"execution_reason", "LIQUIDATION"},
  };
  return make_runtime_market_event("LiquidationExecuted",
                                   input.market_id,
                                   record,
                                   input.input_id,
                                   market_sequences,
                                   clock,
                                   std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_liquidation_position_changed_event(
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    const IsolatedPositionState& flat_position,
    const IsolatedRiskState& previous_risk,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"user_id", number_value(flat_position.user_id)},
      {"position_id", position_id_text(flat_position.user_id,
                                        flat_position.market_id)},
      {"side", position_side_text(flat_position.signed_quantity)},
      {"signed_quantity", number_value(flat_position.signed_quantity)},
      {"quantity", number_value(0)},
      {"average_entry_price", number_value(flat_position.average_entry_price)},
      {"entry_price", number_value(flat_position.average_entry_price)},
      {"mark_price", number_value(previous_risk.mark_price)},
      {"isolated_margin", number_value(flat_position.isolated_margin)},
      {"realized_pnl", number_value(previous_risk.unrealized_pnl)},
      {"unrealized_pnl", number_value(0)},
      {"maintenance_margin", number_value(0)},
      {"liquidation_price", number_value(0)},
      {"reason", "LIQUIDATION"},
      {"margin_asset", flat_position.margin_asset},
      {"leverage", number_value(flat_position.leverage)},
      {"last_fill_quantity", number_value(input.quantity)},
      {"last_fill_price", number_value(input.price)},
  };
  return make_runtime_market_event("PositionChanged",
                                   flat_position.market_id,
                                   record,
                                   input.input_id,
                                   market_sequences,
                                   clock,
                                   std::move(payload));
}

[[nodiscard]] EngineOutputRecord make_liquidation_completed_event(
    const InboundEngineRecord& record,
    const cex::adapter::LiquidatePositionInput& input,
    cex::adapter::MarketSequenceGenerator& market_sequences,
    const EngineRuntimeClock& clock) {
  PayloadFields payload{
      {"liquidation_id", input.liquidation_id},
      {"user_id", number_value(input.liquidated_user_id)},
      {"position_id", position_id_text(input.liquidated_user_id,
                                        input.market_id)},
      {"final_status", "FLAT"},
      {"remaining_quantity", number_value(0)},
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

[[nodiscard]] EngineProcessResult rejected_no_output() {
  EngineProcessResult result;
  result.status = EngineProcessStatus::Rejected;
  return result;
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

    auto mapped =
        cex::adapter::map_place_order_to_core(input, received_sequence(record));
    if (!mapped.metadata_to_record.has_value()) {
      throw std::runtime_error("place order mapping did not produce metadata");
    }

    auto context =
        make_translation_context(record, input, *mapped.metadata_to_record);
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

    EngineProcessResult result;
    const auto rejection_reason =
        liquidation_rejection_reason(input, positions_, risk_states_);
    if (!rejection_reason.empty()) {
      result.replies.push_back(
          make_liquidation_rejected_reply(record, input, rejection_reason));
      mark_processed(RuntimeCommandKind::LiquidatePosition,
                     record,
                     input.input_id,
                     input.envelope.idempotency_key);
      return apply_processing_mode(std::move(result), mode);
    }

    const PositionRiskKey key{
        .user_id = input.liquidated_user_id,
        .market_id = input.market_id,
    };
    const auto previous_position = positions_.at(key);
    const auto previous_risk = risk_states_.at(key);
    const auto fill_id = market_sequences_.peek(input.market_id) + 1;
    const auto liquidation_order_id =
        static_cast<OrderId>(
            cex::adapter::command_id_from_request_id(input.liquidation_id));

    result.replies.push_back(make_liquidation_accepted_reply(
        record, input, liquidation_order_id));
    result.events.push_back(make_liquidation_started_event(record,
                                                           input,
                                                           previous_position,
                                                           previous_risk,
                                                           market_sequences_,
                                                           clock_));
    result.events.push_back(make_liquidation_executed_event(record,
                                                            input,
                                                            previous_position,
                                                            fill_id,
                                                            market_sequences_,
                                                            clock_));

    auto flat_position = previous_position;
    flat_position.signed_quantity = 0;
    flat_position.average_entry_price = 0;
    flat_position.isolated_margin = 0;
    flat_position.updated_at_ms = clock_ ? clock_() : 0;
    positions_[key] = flat_position;

    IsolatedRiskState flat_risk{
        .user_id = input.liquidated_user_id,
        .market_id = input.market_id,
        .status = "FLAT",
        .margin_asset = previous_risk.margin_asset,
        .signed_quantity = 0,
        .average_entry_price = 0,
        .mark_price = previous_risk.mark_price,
        .isolated_margin = 0,
        .unrealized_pnl = 0,
        .equity = 0,
        .maintenance_margin = 0,
        .margin_ratio = 0,
        .leverage = previous_risk.leverage,
        .updated_at_ms = flat_position.updated_at_ms,
    };
    risk_states_[key] = flat_risk;

    result.events.push_back(make_liquidation_position_changed_event(
        record,
        input,
        flat_position,
        previous_risk,
        market_sequences_,
        clock_));
    result.events.push_back(make_runtime_risk_state_updated_event(record,
                                                                  input.input_id,
                                                                  flat_risk,
                                                                  market_sequences_,
                                                                  clock_));
    result.events.push_back(make_liquidation_completed_event(record,
                                                             input,
                                                             market_sequences_,
                                                             clock_));

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

      auto& risk = risk_states_[key];
      if (risk.user_id == 0 && risk.market_id == 0) {
        risk.user_id = position.user_id;
        risk.market_id = position.market_id;
        risk.equity = position.isolated_margin;
      }
      risk.user_id = position.user_id;
      risk.market_id = position.market_id;
      risk.margin_asset = settlement_asset_for(position);
      risk.signed_quantity = position.signed_quantity;
      risk.average_entry_price = position.average_entry_price;
      risk.mark_price = mark_price;
      risk.isolated_margin = position.isolated_margin;
      risk.unrealized_pnl =
          (mark_price - position.average_entry_price) *
          position.signed_quantity;
      risk.equity += amount;
      risk.maintenance_margin =
          maintenance_margin_for(position.signed_quantity, mark_price);
      risk.margin_ratio =
          margin_ratio_for(risk.equity, risk.maintenance_margin);
      risk.leverage = position.leverage;
      risk.updated_at_ms = input.settle_at_ms;
      if (position.signed_quantity == 0) {
        risk.status = "FLAT";
      } else if (risk.equity <= risk.maintenance_margin) {
        risk.status = "LIQUIDATABLE";
      } else {
        risk.status = "HEALTHY";
      }
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
