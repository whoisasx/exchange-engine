#include "services/FakeEngineSmoke.hpp"

#include <utility>

namespace services {

std::vector<SmokeValidationIssue> FakeEngineSmokeContract::validate_engine_input(
    const TopicConfig& topic) {
  std::vector<SmokeValidationIssue> issues;

  if (topic.name != EngineInputTopic) {
    issues.push_back(SmokeValidationIssue{
        "name",
        "fake-engine smoke must validate the engine.input topic"});
  }

  if (topic.partitions != RequiredEngineInputPartitions) {
    issues.push_back(SmokeValidationIssue{
        "partitions",
        "engine.input must have exactly one partition for ordered input"});
  }

  if (!topic.retention_ms.has_value() ||
      *topic.retention_ms != RequiredEngineInputRetentionMs) {
    issues.push_back(SmokeValidationIssue{
        "retention.ms",
        "engine.input retention.ms must be 1800000"});
  }

  return issues;
}

bool FakeEngineSmokeContract::is_valid_engine_input(const TopicConfig& topic) {
  return validate_engine_input(topic).empty();
}

FakeEngineSmoke::FakeEngineSmoke(ITopicAdmin& topic_admin)
    : topic_admin_(topic_admin) {}

FakeEngineSmokeEnsureResult FakeEngineSmoke::validate_engine_input_topic()
    const {
  const auto topic = topic_admin_.describe_topic(EngineInputTopic);
  if (!topic.has_value()) {
    return FakeEngineSmokeEnsureResult{
        false,
        {SmokeValidationIssue{"name", "engine.input topic is missing"}},
    };
  }

  auto issues = FakeEngineSmokeContract::validate_engine_input(*topic);
  return FakeEngineSmokeEnsureResult{issues.empty(), std::move(issues)};
}

}  // namespace services
