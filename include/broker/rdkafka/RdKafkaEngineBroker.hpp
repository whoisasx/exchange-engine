#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "broker/RedpandaEngineApp.hpp"

namespace cex::broker {

using RdKafkaProperty = std::pair<std::string, std::string>;

struct RdKafkaConsumerConfig {
  std::string bootstrap_servers{"127.0.0.1:9092"};
  std::string group_id{"engine"};
  std::vector<std::string> topics{EngineInputTopic};
  std::chrono::milliseconds poll_timeout{100};
  std::chrono::milliseconds seek_timeout{5000};
  std::chrono::milliseconds watermark_timeout{5000};
  std::string client_id{"engine-input-consumer"};
  std::vector<RdKafkaProperty> properties;
};

struct RdKafkaProducerConfig {
  std::string bootstrap_servers{"127.0.0.1:9092"};
  std::string client_id{"engine-record-producer"};
  std::chrono::milliseconds flush_timeout{5000};
  std::vector<RdKafkaProperty> properties;
};

struct RdKafkaOffsetCommitterConfig {
  std::string bootstrap_servers{"127.0.0.1:9092"};
  std::string group_id{"engine"};
  std::string client_id{"engine-offset-committer"};
  std::vector<RdKafkaProperty> properties;
};

class RdKafkaError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class RdKafkaConfigError final : public RdKafkaError {
 public:
  using RdKafkaError::RdKafkaError;
};

class RdKafkaConstructionError final : public RdKafkaError {
 public:
  using RdKafkaError::RdKafkaError;
};

class RdKafkaOperationError final : public RdKafkaError {
 public:
  using RdKafkaError::RdKafkaError;
};

[[nodiscard]] std::optional<std::string> validate_config(
    const RdKafkaConsumerConfig& config);
[[nodiscard]] std::optional<std::string> validate_config(
    const RdKafkaProducerConfig& config);
[[nodiscard]] std::optional<std::string> validate_config(
    const RdKafkaOffsetCommitterConfig& config);

[[nodiscard]] std::string rdkafka_runtime_version();

class RdKafkaEngineInputConsumer final : public IEngineInputConsumer {
 public:
  explicit RdKafkaEngineInputConsumer(const RdKafkaConsumerConfig& config);
  ~RdKafkaEngineInputConsumer() override;

  RdKafkaEngineInputConsumer(const RdKafkaEngineInputConsumer&) = delete;
  RdKafkaEngineInputConsumer& operator=(const RdKafkaEngineInputConsumer&) =
      delete;

  [[nodiscard]] std::optional<ConsumedRecord> poll() override;
  [[nodiscard]] std::optional<std::string> seek(
      const std::string& topic,
      std::int32_t partition,
      std::int64_t offset) override;
  [[nodiscard]] BrokerWatermarkResult get_watermark(
      const std::string& topic,
      std::int32_t partition) override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class RdKafkaEngineRecordProducer final : public IEngineRecordProducer {
 public:
  explicit RdKafkaEngineRecordProducer(const RdKafkaProducerConfig& config);
  ~RdKafkaEngineRecordProducer() override;

  RdKafkaEngineRecordProducer(const RdKafkaEngineRecordProducer&) = delete;
  RdKafkaEngineRecordProducer& operator=(const RdKafkaEngineRecordProducer&) =
      delete;

  [[nodiscard]] std::optional<std::string> produce(
      const ProduceRequest& request) override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class RdKafkaOffsetCommitter final : public IEngineOffsetCommitter {
 public:
  explicit RdKafkaOffsetCommitter(const RdKafkaOffsetCommitterConfig& config);
  ~RdKafkaOffsetCommitter() override;

  RdKafkaOffsetCommitter(const RdKafkaOffsetCommitter&) = delete;
  RdKafkaOffsetCommitter& operator=(const RdKafkaOffsetCommitter&) = delete;

  [[nodiscard]] std::optional<std::string> commit(
      const OffsetCommitRequest& request) override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cex::broker
