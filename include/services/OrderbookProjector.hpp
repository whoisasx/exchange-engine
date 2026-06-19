#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "services/ProtocolMessage.hpp"

namespace services {

struct PriceLevelDeltaDto {
  std::int64_t price{0};
  std::int64_t quantity{0};
};

struct OrderBookDeltaDto {
  MarketSequenceKey key;
  std::int64_t engine_timestamp_ms{0};
  std::vector<PriceLevelDeltaDto> bids;
  std::vector<PriceLevelDeltaDto> asks;
  StreamPosition source_position;
};

enum class OrderbookProjectorStatus {
  Ignored,
  Duplicate,
  Applied,
  Invalid,
};

struct OrderbookProjectorResult {
  OrderbookProjectorStatus status{OrderbookProjectorStatus::Ignored};
  std::optional<MarketSequenceKey> idempotency_key;
  std::string reason;
};

class IOrderbookStateStore {
public:
  virtual ~IOrderbookStateStore() = default;

  [[nodiscard]] virtual bool has_applied_delta(
      const MarketSequenceKey& key) const = 0;

  // Real adapters should update orderbook_state and orderbook_levels. A
  // quantity of zero removes the level.
  virtual void apply_delta(const OrderBookDeltaDto& delta) = 0;

  virtual void commit_offset(const StreamPosition& position) = 0;
};

class OrderbookProjector {
public:
  explicit OrderbookProjector(IOrderbookStateStore& store);

  // Contract: consume engine.events OrderBookDelta only. Apply deltas in
  // (market_id, engine_sequence) order; engine_sequence is per market.
  [[nodiscard]] OrderbookProjectorResult consume(const ProtocolMessage& message);
  [[nodiscard]] OrderbookProjectorResult project(const OrderBookDeltaDto& delta);

  // The generic protocol envelope carries routing/idempotency fields. Concrete
  // Redpanda/JSON adapters should decode bids and asks into OrderBookDeltaDto
  // before calling project().
  [[nodiscard]] static std::optional<OrderBookDeltaDto> parse_orderbook_delta(
      const ProtocolMessage& message);

private:
  IOrderbookStateStore& store_;
};

}  // namespace services
