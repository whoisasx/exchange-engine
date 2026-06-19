#pragma once

#include <charconv>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_map>

namespace services {

inline constexpr const char* EngineEventsTopic = "engine.events";
inline constexpr const char* WalletEventsTopic = "wallet.events";
inline constexpr const char* EngineInputTopic = "engine.input";

using PayloadFields = std::unordered_map<std::string, std::string>;

struct StreamPosition {
  std::string topic;
  std::int32_t partition{0};
  std::int64_t offset{0};

  bool operator==(const StreamPosition&) const = default;
};

struct ProtocolMessage {
  std::string topic;
  std::string type;
  PayloadFields payload;
  std::optional<std::string> key;
  std::int32_t partition{0};
  std::int64_t offset{0};

  [[nodiscard]] StreamPosition position() const {
    return StreamPosition{topic, partition, offset};
  }
};

struct StreamPositionHash {
  [[nodiscard]] std::size_t operator()(const StreamPosition& position) const {
    const auto topic_hash = std::hash<std::string>{}(position.topic);
    const auto partition_hash = std::hash<std::int32_t>{}(position.partition);
    const auto offset_hash = std::hash<std::int64_t>{}(position.offset);
    return topic_hash ^ (partition_hash << 1U) ^ (offset_hash << 2U);
  }
};

struct MarketSequenceKey {
  std::uint64_t market_id{0};
  std::uint64_t engine_sequence{0};

  bool operator==(const MarketSequenceKey&) const = default;
};

struct MarketSequenceKeyHash {
  [[nodiscard]] std::size_t operator()(const MarketSequenceKey& key) const {
    const auto market_hash = std::hash<std::uint64_t>{}(key.market_id);
    const auto sequence_hash = std::hash<std::uint64_t>{}(key.engine_sequence);
    return market_hash ^ (sequence_hash << 1U);
  }
};

[[nodiscard]] inline std::optional<std::string> payload_string(
    const ProtocolMessage& message,
    const std::string& field) {
  const auto iter = message.payload.find(field);
  if (iter == message.payload.end()) {
    return std::nullopt;
  }
  return iter->second;
}

[[nodiscard]] inline std::optional<std::uint64_t> payload_u64(
    const ProtocolMessage& message,
    const std::string& field) {
  const auto value = payload_string(message, field);
  if (!value.has_value()) {
    return std::nullopt;
  }

  std::uint64_t parsed{0};
  const auto* begin = value->data();
  const auto* end = begin + value->size();
  const auto result = std::from_chars(begin, end, parsed);
  if (result.ec != std::errc{} || result.ptr != end) {
    return std::nullopt;
  }
  return parsed;
}

[[nodiscard]] inline std::optional<std::int64_t> payload_i64(
    const ProtocolMessage& message,
    const std::string& field) {
  const auto value = payload_string(message, field);
  if (!value.has_value()) {
    return std::nullopt;
  }

  std::int64_t parsed{0};
  const auto* begin = value->data();
  const auto* end = begin + value->size();
  const auto result = std::from_chars(begin, end, parsed);
  if (result.ec != std::errc{} || result.ptr != end) {
    return std::nullopt;
  }
  return parsed;
}

}  // namespace services
