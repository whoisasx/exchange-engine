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

enum class EngineProcessStatus {
  Processed,
  Duplicate,
};

enum class ProcessingMode {
  Live,
  ReplaySilent,
};

enum class EngineDuplicateReason {
  None,
  InputId,
  IdempotencyKey,
};

struct EngineDuplicateInfo {
  EngineDuplicateReason reason{EngineDuplicateReason::None};
  std::string key;
  std::string original_topic{EngineInputTopic};
  std::int32_t original_partition{0};
  std::int64_t original_offset{0};
  std::optional<std::string> original_input_id;
  std::string original_idempotency_key;
};

struct EngineProcessResult {
  EngineProcessStatus status{EngineProcessStatus::Processed};
  std::optional<EngineDuplicateInfo> duplicate;
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
