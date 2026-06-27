#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/SymbolConfig.hpp"
#include "runtime/EnginePayloadValue.hpp"

namespace cex::runtime {

inline constexpr const char* EngineInputTopic = "engine.input";
inline constexpr const char* EngineRepliesTopic = "engine.replies";
inline constexpr const char* EngineEventsTopic = "engine.events";

using PayloadFields = std::unordered_map<std::string, PayloadValue>;
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

struct EngineTraceSummary {
  std::optional<std::string> request_id;
  std::optional<std::string> source_input_id;
  std::size_t reply_count{0};
  std::size_t event_count{0};
  bool duplicate{false};
  bool no_output{false};
};

enum class EngineProcessStatus {
  Processed,
  Duplicate,
  Rejected,
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

  [[nodiscard]] EngineTraceSummary trace_summary() const {
    EngineTraceSummary summary{
        .reply_count = replies.size(),
        .event_count = events.size(),
        .duplicate = status == EngineProcessStatus::Duplicate,
        .no_output = empty(),
    };

    const auto capture = [&summary](const EngineOutputRecord& record) {
      if (!summary.request_id.has_value()) {
        if (const auto found = record.payload.find("request_id");
            found != record.payload.end()) {
          if (const auto* value = found->second.as_string();
              value != nullptr && !value->empty()) {
            summary.request_id = *value;
          }
        }
      }
      if (!summary.source_input_id.has_value()) {
        if (const auto found = record.payload.find("source_input_id");
            found != record.payload.end()) {
          if (const auto* value = found->second.as_string();
              value != nullptr && !value->empty()) {
            summary.source_input_id = *value;
          }
        }
      }
    };

    for (const auto& reply : replies) {
      capture(reply);
    }
    for (const auto& event : events) {
      capture(event);
    }
    return summary;
  }
};

struct EngineRuntimeConfig {
  std::vector<SymbolConfig> symbols;
  EngineSequence first_public_sequence{1};
  EngineRuntimeClock clock;
};

}  // namespace cex::runtime
