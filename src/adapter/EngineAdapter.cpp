#include "adapter/EngineAdapter.hpp"

#include <limits>
#include <stdexcept>

#include "core/FixedPoint.hpp"

namespace cex::adapter {
namespace {

constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

[[nodiscard]] std::uint64_t stable_string_id(std::string_view value) {
  std::uint64_t hash = kFnvOffsetBasis;
  for (const unsigned char c : value) {
    hash ^= c;
    hash *= kFnvPrime;
  }
  return hash == 0 ? 1 : hash;
}

void require_non_empty(std::string_view value, const char* field_name) {
  if (value.empty()) {
    throw std::invalid_argument(std::string(field_name) + " must not be empty");
  }
}

[[nodiscard]] SymbolId to_core_symbol_id(MarketId market_id) {
  if (market_id <= 0 || market_id > std::numeric_limits<SymbolId>::max()) {
    throw std::invalid_argument("market_id is outside the core SymbolId range");
  }
  return static_cast<SymbolId>(market_id);
}

[[nodiscard]] UserId to_core_user_id(AdapterUserId user_id) {
  if (user_id < 0 || user_id > std::numeric_limits<UserId>::max()) {
    throw std::invalid_argument("user_id is outside the core UserId range");
  }
  return static_cast<UserId>(user_id);
}

[[nodiscard]] OrderId to_core_order_id(AdapterOrderId order_id) {
  if (order_id <= 0) {
    throw std::invalid_argument("order_id must be positive");
  }
  return static_cast<OrderId>(order_id);
}

[[nodiscard]] Price to_core_price(AdapterPrice price, AdapterOrderType order_type) {
  if (price < 0) {
    throw std::invalid_argument("price must not be negative");
  }
  if (order_type == AdapterOrderType::Limit && price == 0) {
    throw std::invalid_argument("limit order price must be positive");
  }
  return Price::from_ticks(price);
}

[[nodiscard]] Quantity to_core_quantity(AdapterQuantity quantity) {
  if (quantity <= 0) {
    throw std::invalid_argument("quantity must be positive");
  }
  return Quantity::from_lots(static_cast<std::uint64_t>(quantity));
}

void validate_envelope(const CommandEnvelope& envelope) {
  require_non_empty(envelope.request_id, "request_id");
  require_non_empty(envelope.idempotency_key, "idempotency_key");
  (void)to_core_user_id(envelope.user_id);
}

} // namespace

bool OrderMetadataStore::insert(OrderMetadata metadata) {
  return by_order_id_.emplace(metadata.order_id, std::move(metadata)).second;
}

const OrderMetadata* OrderMetadataStore::find(OrderId order_id) const {
  const auto it = by_order_id_.find(order_id);
  return it == by_order_id_.end() ? nullptr : &it->second;
}

OrderMetadata* OrderMetadataStore::find(OrderId order_id) {
  const auto it = by_order_id_.find(order_id);
  return it == by_order_id_.end() ? nullptr : &it->second;
}

bool OrderMetadataStore::erase(OrderId order_id) {
  return by_order_id_.erase(order_id) != 0;
}

std::size_t OrderMetadataStore::size() const {
  return by_order_id_.size();
}

bool OrderMetadataStore::empty() const {
  return by_order_id_.empty();
}

MarketSequenceGenerator::MarketSequenceGenerator(EngineSequence first_sequence)
    : first_sequence_(first_sequence == 0 ? 1 : first_sequence) {}

EngineSequence MarketSequenceGenerator::next(MarketId market_id) {
  if (market_id <= 0) {
    throw std::invalid_argument("market_id must be positive");
  }

  auto [it, inserted] = next_by_market_.emplace(market_id, first_sequence_);
  (void)inserted;
  const EngineSequence sequence = it->second;
  ++it->second;
  return sequence;
}

EngineSequence MarketSequenceGenerator::peek(MarketId market_id) const {
  if (market_id <= 0) {
    throw std::invalid_argument("market_id must be positive");
  }

  const auto it = next_by_market_.find(market_id);
  return it == next_by_market_.end() ? first_sequence_ : it->second;
}

void MarketSequenceGenerator::restore(MarketId market_id, EngineSequence next_sequence) {
  if (market_id <= 0) {
    throw std::invalid_argument("market_id must be positive");
  }
  if (next_sequence == 0) {
    throw std::invalid_argument("next_sequence must be positive");
  }
  next_by_market_[market_id] = next_sequence;
}

std::unordered_map<MarketId, EngineSequence> MarketSequenceGenerator::snapshot() const {
  return next_by_market_;
}

CommandId command_id_from_request_id(std::string_view request_id) {
  require_non_empty(request_id, "request_id");
  return stable_string_id(request_id);
}

ClientOrderId client_order_id_from_idempotency_key(std::string_view idempotency_key) {
  require_non_empty(idempotency_key, "idempotency_key");
  return stable_string_id(idempotency_key);
}

MappedCommand map_place_order_to_core(
    const PlaceOrderInput& input,
    EngineSequence received_sequence
) {
  validate_envelope(input.envelope);
  require_non_empty(input.reservation_id, "reservation_id");

  const CommandId command_id = command_id_from_request_id(input.envelope.request_id);
  const ClientOrderId client_order_id =
      client_order_id_from_idempotency_key(input.envelope.idempotency_key);
  const OrderId order_id = to_core_order_id(input.order_id);

  PlaceOrderCommand place_command{
      .commandId = command_id,
      .clientOrderId = client_order_id,
      .orderId = order_id,
      .userId = to_core_user_id(input.envelope.user_id),
      .symbolId = to_core_symbol_id(input.market_id),
      .side = to_core_side(input.side),
      .orderType = to_core_order_type(input.order_type),
      .timeInForce = to_core_time_in_force(input.time_in_force),
      .price = to_core_price(input.price, input.order_type),
      .quantity = to_core_quantity(input.quantity),
      .receivedSequence = received_sequence,
  };

  EngineCommand command{};
  command.kind = EngineCommandKind::PlaceOrder;
  command.placeOrder = place_command;
  command.cancelOrder = std::nullopt;

  OrderMetadata metadata{
      .order_id = order_id,
      .market_id = input.market_id,
      .user_id = input.envelope.user_id,
      .reservation_id = input.reservation_id,
      .place_request_id = input.envelope.request_id,
      .place_idempotency_key = input.envelope.idempotency_key,
      .place_input_id = input.input_id,
      .reply_partition = input.envelope.reply_partition,
      .core_client_order_id = client_order_id,
      .core_place_command_id = command_id,
  };

  return MappedCommand{
      .command = command,
      .metadata_to_record = metadata,
  };
}

MappedCommand map_cancel_order_to_core(
    const CancelOrderInput& input,
    EngineSequence received_sequence
) {
  validate_envelope(input.envelope);

  CancelOrderCommand cancel_command{
      .commandId = command_id_from_request_id(input.envelope.request_id),
      .userId = to_core_user_id(input.envelope.user_id),
      .symbolId = to_core_symbol_id(input.market_id),
      .orderId = to_core_order_id(input.order_id),
      .clientOrderId = client_order_id_from_idempotency_key(input.envelope.idempotency_key),
      .receivedSequence = received_sequence,
  };

  EngineCommand command{};
  command.kind = EngineCommandKind::CancelOrder;
  command.placeOrder = std::nullopt;
  command.cancelOrder = cancel_command;

  return MappedCommand{
      .command = command,
      .metadata_to_record = std::nullopt,
  };
}

Side to_core_side(AdapterSide side) {
  switch (side) {
    case AdapterSide::Long:
      return Buy;
    case AdapterSide::Short:
      return Sell;
  }
  throw std::invalid_argument("unknown side");
}

OrderType to_core_order_type(AdapterOrderType order_type) {
  switch (order_type) {
    case AdapterOrderType::Limit:
      return Limit;
    case AdapterOrderType::Market:
      return Market;
  }
  throw std::invalid_argument("unknown order_type");
}

TimeInForce to_core_time_in_force(AdapterTimeInForce time_in_force) {
  switch (time_in_force) {
    case AdapterTimeInForce::Gtc:
      return GTC;
    case AdapterTimeInForce::Ioc:
      return IOC;
    case AdapterTimeInForce::Fok:
      return FOK;
    case AdapterTimeInForce::PostOnly:
      return PO;
  }
  throw std::invalid_argument("unknown time_in_force");
}

} // namespace cex::adapter
