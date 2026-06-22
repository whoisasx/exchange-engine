#include "broker/rdkafka/RdKafkaEngineBroker.hpp"

#include <librdkafka/rdkafkacpp.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace cex::broker {
namespace {

template <typename T>
struct DeleteRdKafka {
  void operator()(T* value) const {
    delete value;
  }
};

using ConfPtr = std::unique_ptr<RdKafka::Conf, DeleteRdKafka<RdKafka::Conf>>;
using ConsumerPtr =
    std::unique_ptr<RdKafka::KafkaConsumer,
                    DeleteRdKafka<RdKafka::KafkaConsumer>>;
using ProducerPtr =
    std::unique_ptr<RdKafka::Producer, DeleteRdKafka<RdKafka::Producer>>;
using MessagePtr =
    std::unique_ptr<RdKafka::Message, DeleteRdKafka<RdKafka::Message>>;
using TopicPartitionPtr =
    std::unique_ptr<RdKafka::TopicPartition,
                    DeleteRdKafka<RdKafka::TopicPartition>>;

[[nodiscard]] bool blank(std::string_view value) {
  return std::ranges::all_of(value, [](unsigned char ch) {
    return std::isspace(ch) != 0;
  });
}

[[nodiscard]] std::optional<std::string> validate_properties(
    const std::vector<RdKafkaProperty>& properties) {
  for (const auto& [name, value] : properties) {
    (void)value;
    if (blank(name)) {
      return "librdkafka property name must not be empty";
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::string> validate_bootstrap(
    std::string_view bootstrap_servers) {
  if (blank(bootstrap_servers)) {
    return "bootstrap_servers must not be empty";
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::string> validate_group_id(
    std::string_view group_id) {
  if (blank(group_id)) {
    return "group_id must not be empty";
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::string> validate_timeout(
    std::chrono::milliseconds timeout,
    std::string_view name) {
  if (timeout.count() < 0) {
    return std::string(name) + " must not be negative";
  }
  if (timeout.count() > std::numeric_limits<int>::max()) {
    return std::string(name) + " must fit in a 32-bit millisecond value";
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::string> validate_common(
    std::string_view bootstrap_servers,
    const std::vector<RdKafkaProperty>& properties) {
  if (auto error = validate_bootstrap(bootstrap_servers); error.has_value()) {
    return error;
  }
  return validate_properties(properties);
}

[[nodiscard]] std::string describe_partition(std::string_view topic,
                                             std::int32_t partition) {
  return std::string(topic) + "[" + std::to_string(partition) + "]";
}

[[nodiscard]] std::optional<std::string> validate_partition_request(
    std::string_view topic,
    std::int32_t partition) {
  if (blank(topic)) {
    return "topic must not be empty";
  }
  if (partition < 0) {
    return "partition must not be negative";
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::string> validate_seek_request(
    std::string_view topic,
    std::int32_t partition,
    std::int64_t offset) {
  if (auto error = validate_partition_request(topic, partition);
      error.has_value()) {
    return error;
  }
  if (offset < 0) {
    return "offset must not be negative";
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::string> validate_watermark_offsets(
    std::string_view topic,
    std::int32_t partition,
    const BrokerWatermarkOffsets& watermark) {
  if (watermark.low < 0 || watermark.high < 0 ||
      watermark.low > watermark.high) {
    return "invalid broker watermark for " +
           describe_partition(topic, partition) + ": low=" +
           std::to_string(watermark.low) + ", high=" +
           std::to_string(watermark.high);
  }
  return std::nullopt;
}

[[nodiscard]] int to_timeout_ms(std::chrono::milliseconds timeout) {
  return static_cast<int>(timeout.count());
}

void set_conf(RdKafka::Conf& conf,
              const std::string& name,
              const std::string& value) {
  std::string errstr;
  const auto result = conf.set(name, value, errstr);
  if (result != RdKafka::Conf::CONF_OK) {
    throw RdKafkaConfigError("invalid librdkafka config '" + name +
                             "': " + errstr);
  }
}

void set_conf(RdKafka::Conf& conf,
              const std::string& name,
              RdKafka::DeliveryReportCb* callback) {
  std::string errstr;
  const auto result = conf.set(name, callback, errstr);
  if (result != RdKafka::Conf::CONF_OK) {
    throw RdKafkaConfigError("invalid librdkafka callback config '" + name +
                             "': " + errstr);
  }
}

void apply_properties(RdKafka::Conf& conf,
                      const std::vector<RdKafkaProperty>& properties) {
  for (const auto& [name, value] : properties) {
    set_conf(conf, name, value);
  }
}

[[nodiscard]] ConfPtr create_global_conf() {
  return ConfPtr(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
}

[[nodiscard]] std::string describe_error(RdKafka::ErrorCode code) {
  return RdKafka::err2str(code);
}

[[nodiscard]] std::string describe_message_error(
    const RdKafka::Message& message) {
  const std::string details = message.errstr();
  if (!details.empty()) {
    return details;
  }
  return describe_error(message.err());
}

[[nodiscard]] std::string join_topics(
    const std::vector<std::string>& topics) {
  std::ostringstream output;
  for (std::size_t index = 0; index < topics.size(); ++index) {
    if (index != 0) {
      output << ',';
    }
    output << topics[index];
  }
  return output.str();
}

[[nodiscard]] ConfPtr create_consumer_conf(
    const RdKafkaConsumerConfig& config) {
  auto conf = create_global_conf();
  set_conf(*conf, "bootstrap.servers", config.bootstrap_servers);
  set_conf(*conf, "group.id", config.group_id);
  set_conf(*conf, "client.id", config.client_id);
  set_conf(*conf, "enable.auto.commit", "false");
  set_conf(*conf, "auto.offset.reset", "earliest");
  apply_properties(*conf, config.properties);
  return conf;
}

[[nodiscard]] ConfPtr create_committer_conf(
    const RdKafkaOffsetCommitterConfig& config) {
  auto conf = create_global_conf();
  set_conf(*conf, "bootstrap.servers", config.bootstrap_servers);
  set_conf(*conf, "group.id", config.group_id);
  set_conf(*conf, "client.id", config.client_id);
  set_conf(*conf, "enable.auto.commit", "false");
  apply_properties(*conf, config.properties);
  return conf;
}

class DeliveryReporter final : public RdKafka::DeliveryReportCb {
 public:
  void reset() {
    last_error_.reset();
  }

  [[nodiscard]] std::optional<std::string> last_error() const {
    return last_error_;
  }

  void dr_cb(RdKafka::Message& message) override {
    if (message.err() != RdKafka::ERR_NO_ERROR) {
      last_error_ = "rdkafka delivery failed: " +
                    describe_message_error(message);
    }
  }

 private:
  std::optional<std::string> last_error_;
};

[[nodiscard]] ConfPtr create_producer_conf(
    const RdKafkaProducerConfig& config,
    DeliveryReporter& delivery_reporter) {
  auto conf = create_global_conf();
  set_conf(*conf, "bootstrap.servers", config.bootstrap_servers);
  set_conf(*conf, "client.id", config.client_id);
  set_conf(*conf, "dr_cb", &delivery_reporter);
  apply_properties(*conf, config.properties);
  return conf;
}

[[nodiscard]] std::optional<std::string> validate_produce_request(
    const ProduceRequest& request) {
  if (blank(request.topic)) {
    return "topic must not be empty";
  }
  if (request.partition.has_value() && *request.partition < 0) {
    return "partition must not be negative";
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<std::string> validate_commit_request(
    const OffsetCommitRequest& request) {
  if (blank(request.topic)) {
    return "topic must not be empty";
  }
  if (request.partition < 0) {
    return "partition must not be negative";
  }
  if (request.offset < 0) {
    return "offset must not be negative";
  }
  if (request.offset == std::numeric_limits<std::int64_t>::max()) {
    return "offset is too large to commit";
  }
  return std::nullopt;
}

}  // namespace

std::optional<std::string> validate_config(
    const RdKafkaConsumerConfig& config) {
  if (auto error = validate_common(config.bootstrap_servers,
                                   config.properties);
      error.has_value()) {
    return error;
  }
  if (auto error = validate_group_id(config.group_id); error.has_value()) {
    return error;
  }
  if (config.topics.empty()) {
    return "topics must not be empty";
  }
  for (const auto& topic : config.topics) {
    if (blank(topic)) {
      return "topic names must not be empty";
    }
  }
  if (auto error = validate_timeout(config.poll_timeout, "poll_timeout");
      error.has_value()) {
    return error;
  }
  if (auto error = validate_timeout(config.seek_timeout, "seek_timeout");
      error.has_value()) {
    return error;
  }
  return validate_timeout(config.watermark_timeout, "watermark_timeout");
}

std::optional<std::string> validate_config(
    const RdKafkaProducerConfig& config) {
  if (auto error = validate_common(config.bootstrap_servers,
                                   config.properties);
      error.has_value()) {
    return error;
  }
  return validate_timeout(config.flush_timeout, "flush_timeout");
}

std::optional<std::string> validate_config(
    const RdKafkaOffsetCommitterConfig& config) {
  if (auto error = validate_common(config.bootstrap_servers,
                                   config.properties);
      error.has_value()) {
    return error;
  }
  return validate_group_id(config.group_id);
}

std::string rdkafka_runtime_version() {
  return RdKafka::version_str();
}

struct RdKafkaEngineInputConsumer::Impl {
  Impl(ConsumerPtr consumer,
       std::chrono::milliseconds poll_timeout,
       std::chrono::milliseconds seek_timeout,
       std::chrono::milliseconds watermark_timeout)
      : consumer(std::move(consumer)),
        poll_timeout(poll_timeout),
        seek_timeout(seek_timeout),
        watermark_timeout(watermark_timeout) {}

  ~Impl() {
    if (consumer) {
      consumer->close();
    }
  }

  ConsumerPtr consumer;
  std::chrono::milliseconds poll_timeout;
  std::chrono::milliseconds seek_timeout;
  std::chrono::milliseconds watermark_timeout;
};

RdKafkaEngineInputConsumer::RdKafkaEngineInputConsumer(
    const RdKafkaConsumerConfig& config) {
  if (auto error = validate_config(config); error.has_value()) {
    throw RdKafkaConfigError("invalid rdkafka consumer config: " + *error);
  }

  auto conf = create_consumer_conf(config);
  std::string errstr;
  ConsumerPtr consumer(RdKafka::KafkaConsumer::create(conf.get(), errstr));
  if (!consumer) {
    throw RdKafkaConstructionError("failed to create rdkafka consumer: " +
                                   errstr);
  }

  const RdKafka::ErrorCode subscribe_result =
      consumer->subscribe(config.topics);
  if (subscribe_result != RdKafka::ERR_NO_ERROR) {
    consumer->close();
    throw RdKafkaConstructionError("failed to subscribe rdkafka consumer to " +
                                   join_topics(config.topics) + ": " +
                                   describe_error(subscribe_result));
  }

  impl_ = std::make_unique<Impl>(std::move(consumer), config.poll_timeout,
                                 config.seek_timeout,
                                 config.watermark_timeout);
}

RdKafkaEngineInputConsumer::~RdKafkaEngineInputConsumer() = default;

std::optional<ConsumedRecord> RdKafkaEngineInputConsumer::poll() {
  MessagePtr message(
      impl_->consumer->consume(to_timeout_ms(impl_->poll_timeout)));
  if (!message) {
    return std::nullopt;
  }

  if (message->err() == RdKafka::ERR__TIMED_OUT) {
    return std::nullopt;
  }
  if (message->err() != RdKafka::ERR_NO_ERROR) {
    throw RdKafkaOperationError("rdkafka consumer error: " +
                                describe_message_error(*message));
  }

  ConsumedRecord record{
      .topic = message->topic_name(),
      .partition = message->partition(),
      .offset = message->offset(),
      .key = std::nullopt,
      .value = {},
  };

  if (const std::string* key = message->key(); key != nullptr) {
    record.key = *key;
  }

  const auto* payload = static_cast<const char*>(message->payload());
  if (payload != nullptr && message->len() > 0) {
    record.value.assign(payload, message->len());
  }

  return record;
}

std::optional<std::string> RdKafkaEngineInputConsumer::seek(
    const std::string& topic,
    std::int32_t partition,
    std::int64_t offset) {
  if (auto error = validate_seek_request(topic, partition, offset);
      error.has_value()) {
    return error;
  }

  const BrokerWatermarkResult watermark = get_watermark(topic, partition);
  if (!watermark.ok()) {
    if (watermark.error.has_value()) {
      return watermark.error;
    }
    return "rdkafka watermark query failed for " +
           describe_partition(topic, partition);
  }

  if (auto error =
          validate_seek_offset(topic, partition, offset, *watermark.offsets);
      error.has_value()) {
    return error;
  }

  TopicPartitionPtr target(
      RdKafka::TopicPartition::create(topic, partition, offset));
  if (!target) {
    return "failed to create rdkafka topic partition for seek";
  }

  const RdKafka::ErrorCode unsubscribe_result = impl_->consumer->unsubscribe();
  if (unsubscribe_result != RdKafka::ERR_NO_ERROR) {
    return "rdkafka unsubscribe failed before seek for " +
           describe_partition(topic, partition) + ": " +
           describe_error(unsubscribe_result);
  }

  std::vector<RdKafka::TopicPartition*> assignment{target.get()};
  const RdKafka::ErrorCode assign_result = impl_->consumer->assign(assignment);
  if (assign_result != RdKafka::ERR_NO_ERROR) {
    return "rdkafka assign failed for " + describe_partition(topic, partition) +
           " at offset " + std::to_string(offset) + ": " +
           describe_error(assign_result);
  }

  return std::nullopt;
}

BrokerWatermarkResult RdKafkaEngineInputConsumer::get_watermark(
    const std::string& topic,
    std::int32_t partition) {
  if (auto error = validate_partition_request(topic, partition);
      error.has_value()) {
    return BrokerWatermarkResult{.offsets = std::nullopt, .error = error};
  }

  std::int64_t low = 0;
  std::int64_t high = 0;
  const RdKafka::ErrorCode query_result =
      impl_->consumer->query_watermark_offsets(
          topic, partition, &low, &high,
          to_timeout_ms(impl_->watermark_timeout));
  if (query_result != RdKafka::ERR_NO_ERROR) {
    return BrokerWatermarkResult{
        .offsets = std::nullopt,
        .error = "rdkafka watermark query failed for " +
                 describe_partition(topic, partition) + ": " +
                 describe_error(query_result),
    };
  }

  BrokerWatermarkOffsets offsets{.low = low, .high = high};
  if (auto error = validate_watermark_offsets(topic, partition, offsets);
      error.has_value()) {
    return BrokerWatermarkResult{.offsets = std::nullopt, .error = error};
  }

  return BrokerWatermarkResult{.offsets = offsets, .error = std::nullopt};
}

std::optional<std::string> RdKafkaEngineInputConsumer::commit(
    const OffsetCommitRequest& request) {
  if (auto error = validate_commit_request(request); error.has_value()) {
    return error;
  }

  const std::int64_t kafka_resume_offset = request.offset + 1;
  TopicPartitionPtr partition(RdKafka::TopicPartition::create(
      request.topic, request.partition, kafka_resume_offset));
  if (!partition) {
    return "failed to create rdkafka topic partition for commit";
  }

  std::vector<RdKafka::TopicPartition*> offsets{partition.get()};
  const RdKafka::ErrorCode commit_result = impl_->consumer->commitSync(offsets);
  if (commit_result != RdKafka::ERR_NO_ERROR) {
    return "rdkafka offset commit failed: " + describe_error(commit_result);
  }

  if (partition->err() != RdKafka::ERR_NO_ERROR) {
    return "rdkafka offset commit failed for " + request.topic + "[" +
           std::to_string(request.partition) + "]: " +
           describe_error(partition->err());
  }

  return std::nullopt;
}

struct RdKafkaEngineRecordProducer::Impl {
  Impl(ProducerPtr producer,
       std::chrono::milliseconds flush_timeout)
      : producer(std::move(producer)), flush_timeout(flush_timeout) {}

  ~Impl() {
    if (producer) {
      producer->flush(to_timeout_ms(flush_timeout));
    }
  }

  DeliveryReporter delivery_reporter;
  ProducerPtr producer;
  std::chrono::milliseconds flush_timeout;
  std::mutex produce_mutex;
};

RdKafkaEngineRecordProducer::RdKafkaEngineRecordProducer(
    const RdKafkaProducerConfig& config) {
  if (auto error = validate_config(config); error.has_value()) {
    throw RdKafkaConfigError("invalid rdkafka producer config: " + *error);
  }

  auto impl = std::make_unique<Impl>(nullptr, config.flush_timeout);
  auto conf = create_producer_conf(config, impl->delivery_reporter);
  std::string errstr;
  impl->producer.reset(RdKafka::Producer::create(conf.get(), errstr));
  if (!impl->producer) {
    throw RdKafkaConstructionError("failed to create rdkafka producer: " +
                                   errstr);
  }

  impl_ = std::move(impl);
}

RdKafkaEngineRecordProducer::~RdKafkaEngineRecordProducer() = default;

std::optional<std::string> RdKafkaEngineRecordProducer::produce(
    const ProduceRequest& request) {
  if (auto error = validate_produce_request(request); error.has_value()) {
    return error;
  }

  std::lock_guard<std::mutex> lock(impl_->produce_mutex);
  impl_->delivery_reporter.reset();

  const std::int32_t partition =
      request.partition.value_or(RdKafka::Topic::PARTITION_UA);
  void* payload = request.value.empty()
                      ? nullptr
                      : const_cast<char*>(request.value.data());
  const void* key = request.key.empty()
                        ? nullptr
                        : static_cast<const void*>(request.key.data());

  const RdKafka::ErrorCode produce_result = impl_->producer->produce(
      request.topic, partition, RdKafka::Producer::RK_MSG_COPY, payload,
      request.value.size(), key, request.key.size(), 0, nullptr);
  if (produce_result != RdKafka::ERR_NO_ERROR) {
    return "rdkafka produce failed: " + describe_error(produce_result);
  }

  const RdKafka::ErrorCode flush_result =
      impl_->producer->flush(to_timeout_ms(impl_->flush_timeout));
  if (flush_result != RdKafka::ERR_NO_ERROR) {
    return "rdkafka producer flush failed: " + describe_error(flush_result);
  }

  return impl_->delivery_reporter.last_error();
}

struct RdKafkaOffsetCommitter::Impl {
  explicit Impl(ConsumerPtr consumer) : consumer(std::move(consumer)) {}

  ~Impl() {
    if (consumer) {
      consumer->close();
    }
  }

  ConsumerPtr consumer;
};

RdKafkaOffsetCommitter::RdKafkaOffsetCommitter(
    const RdKafkaOffsetCommitterConfig& config) {
  if (auto error = validate_config(config); error.has_value()) {
    throw RdKafkaConfigError("invalid rdkafka offset committer config: " +
                             *error);
  }

  auto conf = create_committer_conf(config);
  std::string errstr;
  ConsumerPtr consumer(RdKafka::KafkaConsumer::create(conf.get(), errstr));
  if (!consumer) {
    throw RdKafkaConstructionError("failed to create rdkafka offset committer: " +
                                   errstr);
  }

  impl_ = std::make_unique<Impl>(std::move(consumer));
}

RdKafkaOffsetCommitter::~RdKafkaOffsetCommitter() = default;

std::optional<std::string> RdKafkaOffsetCommitter::commit(
    const OffsetCommitRequest& request) {
  if (auto error = validate_commit_request(request); error.has_value()) {
    return error;
  }

  const std::int64_t kafka_resume_offset = request.offset + 1;
  TopicPartitionPtr partition(RdKafka::TopicPartition::create(
      request.topic, request.partition, kafka_resume_offset));
  if (!partition) {
    return "failed to create rdkafka topic partition for commit";
  }

  std::vector<RdKafka::TopicPartition*> offsets{partition.get()};
  const RdKafka::ErrorCode commit_result = impl_->consumer->commitSync(offsets);
  if (commit_result != RdKafka::ERR_NO_ERROR) {
    return "rdkafka offset commit failed: " + describe_error(commit_result);
  }

  if (partition->err() != RdKafka::ERR_NO_ERROR) {
    return "rdkafka offset commit failed for " + request.topic + "[" +
           std::to_string(request.partition) + "]: " +
           describe_error(partition->err());
  }

  return std::nullopt;
}

}  // namespace cex::broker
