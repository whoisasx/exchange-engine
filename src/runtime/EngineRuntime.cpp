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
