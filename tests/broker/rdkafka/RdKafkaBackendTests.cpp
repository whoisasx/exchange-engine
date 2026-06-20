#include "broker/rdkafka/RdKafkaEngineBroker.hpp"

#include <cassert>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <type_traits>

using namespace cex::broker;

namespace {

void assert_contains(const std::string& value, const std::string& token) {
  assert(value.find(token) != std::string::npos);
}

void assert_config_error_contains(const std::optional<std::string>& error,
                                  const std::string& token) {
  assert(error.has_value());
  assert_contains(*error, token);
}

void test_valid_configs() {
  assert(!validate_config(RdKafkaConsumerConfig{}).has_value());
  assert(!validate_config(RdKafkaProducerConfig{}).has_value());
  assert(!validate_config(RdKafkaOffsetCommitterConfig{}).has_value());
}

void test_consumer_config_validation() {
  RdKafkaConsumerConfig config;

  config.bootstrap_servers = " ";
  assert_config_error_contains(validate_config(config), "bootstrap_servers");

  config = RdKafkaConsumerConfig{};
  config.group_id.clear();
  assert_config_error_contains(validate_config(config), "group_id");

  config = RdKafkaConsumerConfig{};
  config.topics.clear();
  assert_config_error_contains(validate_config(config), "topics");

  config = RdKafkaConsumerConfig{};
  config.topics = {EngineInputTopic, ""};
  assert_config_error_contains(validate_config(config), "topic names");

  config = RdKafkaConsumerConfig{};
  config.poll_timeout = std::chrono::milliseconds{-1};
  assert_config_error_contains(validate_config(config), "poll_timeout");

  config = RdKafkaConsumerConfig{};
  config.seek_timeout = std::chrono::milliseconds{-1};
  assert_config_error_contains(validate_config(config), "seek_timeout");

  config = RdKafkaConsumerConfig{};
  config.watermark_timeout = std::chrono::milliseconds{-1};
  assert_config_error_contains(validate_config(config), "watermark_timeout");
}

void test_producer_config_validation() {
  RdKafkaProducerConfig config;

  config.bootstrap_servers.clear();
  assert_config_error_contains(validate_config(config), "bootstrap_servers");

  config = RdKafkaProducerConfig{};
  config.flush_timeout = std::chrono::milliseconds{-1};
  assert_config_error_contains(validate_config(config), "flush_timeout");

  config = RdKafkaProducerConfig{};
  config.properties.push_back({"", "value"});
  assert_config_error_contains(validate_config(config), "property name");
}

void test_committer_config_validation() {
  RdKafkaOffsetCommitterConfig config;

  config.bootstrap_servers.clear();
  assert_config_error_contains(validate_config(config), "bootstrap_servers");

  config = RdKafkaOffsetCommitterConfig{};
  config.group_id.clear();
  assert_config_error_contains(validate_config(config), "group_id");
}

void test_invalid_construction_errors_are_explicit() {
  try {
    RdKafkaProducerConfig config;
    config.bootstrap_servers.clear();
    RdKafkaEngineRecordProducer producer(config);
    assert(false);
  } catch (const RdKafkaConfigError& error) {
    assert_contains(error.what(), "producer config");
    assert_contains(error.what(), "bootstrap_servers");
  }

  try {
    RdKafkaConsumerConfig config;
    config.group_id.clear();
    RdKafkaEngineInputConsumer consumer(config);
    assert(false);
  } catch (const RdKafkaConfigError& error) {
    assert_contains(error.what(), "consumer config");
    assert_contains(error.what(), "group_id");
  }

  try {
    RdKafkaOffsetCommitterConfig config;
    config.group_id.clear();
    RdKafkaOffsetCommitter committer(config);
    assert(false);
  } catch (const RdKafkaConfigError& error) {
    assert_contains(error.what(), "offset committer config");
    assert_contains(error.what(), "group_id");
  }
}

void test_seek_offset_validation_reports_unavailable_offsets() {
  const BrokerWatermarkOffsets watermark{.low = 20, .high = 30};

  assert(!validate_seek_offset(EngineInputTopic, 0, 20, watermark)
              .has_value());
  assert(!validate_seek_offset(EngineInputTopic, 0, 30, watermark)
              .has_value());

  const auto below_low =
      validate_seek_offset(EngineInputTopic, 0, 19, watermark);
  assert_config_error_contains(below_low, "below low watermark");
  assert_config_error_contains(below_low, "checkpoint");

  const auto above_high =
      validate_seek_offset(EngineInputTopic, 0, 31, watermark);
  assert_config_error_contains(above_high, "above high watermark");

  const auto invalid = validate_seek_offset(
      EngineInputTopic, 0, 20, BrokerWatermarkOffsets{.low = 30, .high = 20});
  assert_config_error_contains(invalid, "invalid broker watermark");
}

void test_compile_link_to_librdkafka() {
  static_assert(!std::is_abstract_v<RdKafkaEngineInputConsumer>);
  const std::string version = rdkafka_runtime_version();
  assert(!version.empty());
}

}  // namespace

int main() {
  try {
    test_valid_configs();
    test_consumer_config_validation();
    test_producer_config_validation();
    test_committer_config_validation();
    test_invalid_construction_errors_are_explicit();
    test_seek_offset_validation_reports_unavailable_offsets();
    test_compile_link_to_librdkafka();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
