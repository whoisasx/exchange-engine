#include "services/OrderbookProjector.hpp"

namespace services {

OrderbookProjector::OrderbookProjector(IOrderbookStateStore& store)
    : store_(store) {}

OrderbookProjectorResult OrderbookProjector::consume(
    const ProtocolMessage& message) {
  if (message.topic != EngineEventsTopic || message.type != "OrderBookDelta") {
    return OrderbookProjectorResult{OrderbookProjectorStatus::Ignored,
                                    std::nullopt,
                                    {}};
  }

  const auto delta = parse_orderbook_delta(message);
  if (!delta.has_value()) {
    return OrderbookProjectorResult{
        OrderbookProjectorStatus::Invalid,
        std::nullopt,
        "OrderBookDelta requires market_id, engine_sequence, and "
        "engine_timestamp_ms"};
  }

  return project(*delta);
}

OrderbookProjectorResult OrderbookProjector::project(
    const OrderBookDeltaDto& delta) {
  if (store_.has_applied_delta(delta.key)) {
    return OrderbookProjectorResult{OrderbookProjectorStatus::Duplicate,
                                    delta.key,
                                    {}};
  }

  store_.apply_delta(delta);
  store_.commit_offset(delta.source_position);

  return OrderbookProjectorResult{OrderbookProjectorStatus::Applied,
                                  delta.key,
                                  {}};
}

std::optional<OrderBookDeltaDto> OrderbookProjector::parse_orderbook_delta(
    const ProtocolMessage& message) {
  if (message.topic != EngineEventsTopic || message.type != "OrderBookDelta") {
    return std::nullopt;
  }

  const auto market_id = payload_u64(message, "market_id");
  const auto engine_sequence = payload_u64(message, "engine_sequence");
  const auto engine_timestamp_ms = payload_i64(message, "engine_timestamp_ms");
  if (!market_id.has_value() || !engine_sequence.has_value() ||
      !engine_timestamp_ms.has_value()) {
    return std::nullopt;
  }

  return OrderBookDeltaDto{
      MarketSequenceKey{*market_id, *engine_sequence},
      *engine_timestamp_ms,
      {},
      {},
      message.position(),
  };
}

}  // namespace services
