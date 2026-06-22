#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "core/Command.hpp"
#include "core/Types.hpp"

namespace cex::adapter {

using MarketId = std::int64_t;
using AdapterUserId = std::int64_t;
using AdapterOrderId = std::int64_t;
using AdapterPrice = std::int64_t;
using AdapterQuantity = std::int64_t;

enum class BrokerTopic {
  EngineInput,
  EngineReplies,
  EngineEvents
};

struct BrokerRecordContext {
  BrokerTopic topic{BrokerTopic::EngineInput};
  std::int32_t partition{0};
  std::int64_t offset{0};
  std::optional<std::string> key;
};

struct OutboundRecordContext {
  BrokerTopic topic{BrokerTopic::EngineEvents};
  std::string key;
  std::optional<std::int32_t> partition;
};

enum class AdapterSide {
  Long,
  Short
};

enum class AdapterOrderType {
  Limit,
  Market
};

enum class AdapterTimeInForce {
  Gtc,
  Ioc,
  Fok,
  PostOnly
};

struct CommandEnvelope {
  std::string request_id;
  std::string idempotency_key;
  AdapterUserId user_id{0};
  std::int32_t reply_partition{0};
};

struct PlaceOrderInput {
  std::optional<std::string> input_id;
  CommandEnvelope envelope;
  AdapterOrderId order_id{0};
  std::string reservation_id;
  MarketId market_id{0};
  std::string market_name;
  AdapterSide side{AdapterSide::Long};
  AdapterOrderType order_type{AdapterOrderType::Limit};
  AdapterTimeInForce time_in_force{AdapterTimeInForce::Gtc};
  AdapterPrice price{0};
  AdapterQuantity quantity{0};
  bool reduce_only{false};
  std::string margin_asset;
  std::int64_t reserved_margin_amount{0};
  std::int32_t leverage{0};
  std::optional<BrokerRecordContext> source;
};

struct CancelOrderInput {
  std::optional<std::string> input_id;
  CommandEnvelope envelope;
  MarketId market_id{0};
  AdapterOrderId order_id{0};
  std::optional<BrokerRecordContext> source;
};

struct MarkPriceUpdatedInput {
  std::optional<std::string> input_id;
  MarketId market_id{0};
  AdapterPrice mark_price{0};
  AdapterPrice index_price{0};
  std::int64_t source_timestamp_ms{0};
  std::int64_t published_at_ms{0};
  std::int64_t valid_until_ms{0};
  std::int64_t source_sequence{0};
  std::string source_status;
  std::optional<BrokerRecordContext> source;
};

struct FundingRateUpdatedInput {
  std::optional<std::string> input_id;
  MarketId market_id{0};
  std::string funding_interval_id;
  std::int64_t rate{0};
  std::int64_t rate_scale{0};
  std::int64_t interval_start_ms{0};
  std::int64_t interval_end_ms{0};
  std::int64_t source_timestamp_ms{0};
  std::optional<BrokerRecordContext> source;
};

struct OrderMetadata {
  OrderId order_id{0};
  MarketId market_id{0};
  AdapterUserId user_id{0};
  AdapterSide side{AdapterSide::Long};
  AdapterQuantity original_quantity{0};
  AdapterQuantity remaining_quantity{0};
  bool reduce_only{false};
  std::string margin_asset;
  std::int64_t reserved_margin_amount{0};
  std::int64_t remaining_reserved_margin{0};
  std::int32_t leverage{0};
  std::string reservation_id;
  std::string place_request_id;
  std::string place_idempotency_key;
  std::optional<std::string> place_input_id;
  std::int32_t reply_partition{0};
  ClientOrderId core_client_order_id{0};
  CommandId core_place_command_id{0};
};

class OrderMetadataStore {
public:
  [[nodiscard]] bool insert(OrderMetadata metadata);
  [[nodiscard]] const OrderMetadata* find(OrderId order_id) const;
  [[nodiscard]] OrderMetadata* find(OrderId order_id);
  [[nodiscard]] bool erase(OrderId order_id);
  [[nodiscard]] std::size_t size() const;
  [[nodiscard]] bool empty() const;

private:
  std::unordered_map<OrderId, OrderMetadata> by_order_id_;
};

class MarketSequenceGenerator {
public:
  explicit MarketSequenceGenerator(EngineSequence first_sequence = 1);

  [[nodiscard]] EngineSequence next(MarketId market_id);
  [[nodiscard]] EngineSequence peek(MarketId market_id) const;
  void restore(MarketId market_id, EngineSequence next_sequence);
  [[nodiscard]] std::unordered_map<MarketId, EngineSequence> snapshot() const;

private:
  EngineSequence first_sequence_;
  std::unordered_map<MarketId, EngineSequence> next_by_market_;
};

struct MappedCommand {
  EngineCommand command;
  std::optional<OrderMetadata> metadata_to_record;
};

[[nodiscard]] CommandId command_id_from_request_id(std::string_view request_id);
[[nodiscard]] ClientOrderId client_order_id_from_idempotency_key(std::string_view idempotency_key);

[[nodiscard]] MappedCommand map_place_order_to_core(
    const PlaceOrderInput& input,
    EngineSequence received_sequence = 0
);

[[nodiscard]] MappedCommand map_cancel_order_to_core(
    const CancelOrderInput& input,
    EngineSequence received_sequence = 0
);

[[nodiscard]] Side to_core_side(AdapterSide side);
[[nodiscard]] OrderType to_core_order_type(AdapterOrderType order_type);
[[nodiscard]] TimeInForce to_core_time_in_force(AdapterTimeInForce time_in_force);

} // namespace cex::adapter
