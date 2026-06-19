#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "services/ProtocolMessage.hpp"

namespace services {

struct TopicConfig {
  std::string name;
  std::int32_t partitions{0};
  std::optional<std::int64_t> retention_ms;
};

struct SmokeValidationIssue {
  std::string field;
  std::string message;
};

struct FakeEngineSmokeContract {
  static constexpr std::int32_t RequiredEngineInputPartitions = 1;
  static constexpr std::int64_t RequiredEngineInputRetentionMs = 1'800'000;

  // Contract: the fake-engine smoke harness requires engine.input to be a
  // single-partition ordered log with 30-minute retention.
  [[nodiscard]] static std::vector<SmokeValidationIssue> validate_engine_input(
      const TopicConfig& topic);

  [[nodiscard]] static bool is_valid_engine_input(const TopicConfig& topic);
};

class ITopicAdmin {
public:
  virtual ~ITopicAdmin() = default;

  [[nodiscard]] virtual std::optional<TopicConfig> describe_topic(
      const std::string& name) const = 0;

  virtual void ensure_topic(const TopicConfig& topic) = 0;
};

struct FakeEngineSmokeEnsureResult {
  bool valid{false};
  std::vector<SmokeValidationIssue> issues;
};

class FakeEngineSmoke {
public:
  explicit FakeEngineSmoke(ITopicAdmin& topic_admin);

  // Real topic-admin implementations can create or alter topics before this
  // check. This scaffold keeps the Redpanda dependency behind ITopicAdmin.
  [[nodiscard]] FakeEngineSmokeEnsureResult validate_engine_input_topic() const;

private:
  ITopicAdmin& topic_admin_;
};

}  // namespace services
