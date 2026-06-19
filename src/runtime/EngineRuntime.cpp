#include "runtime/EngineRuntime.hpp"

#include <chrono>
#include <stdexcept>
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

EngineProcessResult EngineRuntime::process(const InboundEngineRecord& record) {
  ParsedEngineInput parsed = parser_.parse(record.raw_json);

  if (parsed.kind == ParsedEngineInputKind::PlaceOrder) {
    auto input = std::get<cex::adapter::PlaceOrderInput>(parsed.value);
    input.source = broker_context(record);

    auto mapped =
        cex::adapter::map_place_order_to_core(input, received_sequence(record));
    if (!mapped.metadata_to_record.has_value()) {
      throw std::runtime_error("place order mapping did not produce metadata");
    }

    const auto context =
        make_translation_context(record, input, *mapped.metadata_to_record);
    auto core_events = core_.process(mapped.command);
    auto result = translator_.translate(
        core_events, context, metadata_store_, market_sequences_, clock_);

    if (has_order_accepted(core_events)) {
      if (!metadata_store_.insert(*mapped.metadata_to_record)) {
        throw std::runtime_error("accepted order metadata already exists");
      }
    }
    cleanup_completed_order_metadata(metadata_store_, core_events);
    return result;
  }

  auto input = std::get<cex::adapter::CancelOrderInput>(parsed.value);
  input.source = broker_context(record);

  auto mapped =
      cex::adapter::map_cancel_order_to_core(input, received_sequence(record));
  const auto context = make_translation_context(record, input);
  auto core_events = core_.process(mapped.command);
  auto result = translator_.translate(
      core_events, context, metadata_store_, market_sequences_, clock_);

  cleanup_completed_order_metadata(metadata_store_, core_events);
  return result;
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
