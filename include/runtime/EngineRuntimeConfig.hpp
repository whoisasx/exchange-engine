#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/SymbolConfig.hpp"

namespace cex::runtime {

inline constexpr const char* EngineInputTopic = "engine.input";
inline constexpr const char* EngineRepliesTopic = "engine.replies";
inline constexpr const char* EngineEventsTopic = "engine.events";

using PayloadFields = std::unordered_map<std::string, std::string>;
using EngineRuntimeClock = std::function<std::int64_t()>;

struct InboundEngineRecord {
  std::string topic{EngineInputTopic};
  std::int32_t partition{0};
  std::int64_t offset{0};
  std::optional<std::string> key;
  std::string raw_json;
};

struct EngineOutputRecord {
  std::string topic;
  std::string type;
  std::string key;
  std::optional<std::int32_t> partition;
  PayloadFields payload;
};

struct EngineProcessResult {
  std::vector<EngineOutputRecord> replies;
  std::vector<EngineOutputRecord> events;

  [[nodiscard]] bool empty() const noexcept {
    return replies.empty() && events.empty();
  }
};

struct EngineRuntimeConfig {
  std::vector<SymbolConfig> symbols;
  EngineSequence first_public_sequence{1};
  EngineRuntimeClock clock;
};

}  // namespace cex::runtime
