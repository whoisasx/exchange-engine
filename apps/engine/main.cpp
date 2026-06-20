#include "apps/engine/EngineAppConfig.hpp"
#include "broker/RedpandaEngineApp.hpp"
#include "broker/rdkafka/RdKafkaEngineBroker.hpp"
#include "checkpoint/EngineCheckpointManager.hpp"
#include "checkpoint/file/FileCheckpointStore.hpp"
#include "runtime/EngineRuntime.hpp"

#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

[[nodiscard]] const char* status_name(
    cex::broker::EngineBrokerAppStatus status) {
  switch (status) {
    case cex::broker::EngineBrokerAppStatus::NoRecord:
      return "NoRecord";
    case cex::broker::EngineBrokerAppStatus::Processed:
      return "Processed";
    case cex::broker::EngineBrokerAppStatus::RejectedInputTopic:
      return "RejectedInputTopic";
    case cex::broker::EngineBrokerAppStatus::PublishFailed:
      return "PublishFailed";
    case cex::broker::EngineBrokerAppStatus::CommitFailed:
      return "CommitFailed";
    case cex::broker::EngineBrokerAppStatus::ProcessingFailed:
      return "ProcessingFailed";
    case cex::broker::EngineBrokerAppStatus::UnsafeDuplicate:
      return "UnsafeDuplicate";
  }
  return "Unknown";
}

[[nodiscard]] std::vector<std::string> args_from_argv(int argc, char* argv[]) {
  std::vector<std::string> args;
  for (int index = 1; index < argc; ++index) {
    args.emplace_back(argv[index]);
  }
  return args;
}

[[nodiscard]] std::string safe_checkpoint_topic(std::string_view topic) {
  std::string result;
  result.reserve(topic.size());
  for (const unsigned char ch : topic) {
    const bool safe = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                      (ch >= '0' && ch <= '9') || ch == '.' || ch == '-' ||
                      ch == '_';
    result.push_back(safe ? static_cast<char>(ch) : '_');
  }
  return result.empty() ? "topic" : result;
}

[[nodiscard]] std::int64_t next_offset(const cex::broker::ConsumedRecord& source) {
  if (source.offset == std::numeric_limits<std::int64_t>::max()) {
    throw std::runtime_error("cannot checkpoint source offset without overflow");
  }
  return source.offset + 1;
}

[[nodiscard]] std::string checkpoint_id_for(
    const cex::broker::ConsumedRecord& source) {
  std::ostringstream output;
  output << "source-" << safe_checkpoint_topic(source.topic) << "-p"
         << std::setw(10) << std::setfill('0') << source.partition << "-o"
         << std::setw(20) << std::setfill('0') << next_offset(source);
  return output.str();
}

void save_checkpoint_for_source(
    const cex::broker::ConsumedRecord& source,
    const cex::runtime::EngineRuntime& runtime,
    cex::checkpoint::EngineCheckpointManager& checkpoint_manager,
    cex::checkpoint::FileCheckpointStore& checkpoint_store) {
  const cex::checkpoint::CheckpointSourcePosition source_position{
      .topic = source.topic,
      .partition = source.partition,
      .next_offset = next_offset(source),
  };

  auto checkpoint = checkpoint_manager.create_checkpoint(
      runtime, source_position, checkpoint_id_for(source));
  const auto checkpoint_id = checkpoint.checkpoint_id;
  checkpoint_store.save(std::move(checkpoint));

  std::cerr << "saved checkpoint " << checkpoint_id << " at "
            << source_position.topic << '[' << source_position.partition
            << "] next_offset=" << source_position.next_offset << '\n';
}

void restore_latest_checkpoint(
    cex::checkpoint::FileCheckpointStore& checkpoint_store,
    cex::checkpoint::EngineCheckpointManager& checkpoint_manager,
    cex::runtime::EngineRuntime& runtime) {
  auto checkpoint = checkpoint_store.load_latest();
  if (!checkpoint.has_value()) {
    std::cerr << "no checkpoint found in "
              << checkpoint_store.directory().string() << '\n';
    return;
  }

  checkpoint_manager.restore_runtime(*checkpoint, runtime);
  const auto& position = checkpoint->source_position;
  std::cerr << "loaded checkpoint " << checkpoint->checkpoint_id << " from "
            << position.topic << '[' << position.partition
            << "] next_offset=" << position.next_offset << '\n';
}

}  // namespace

int main(int argc, char* argv[]) {
  try {
    const auto parse_result = cex::apps::engine::parse_engine_app_config(
        args_from_argv(argc, argv),
        cex::apps::engine::engine_app_environment_from_process());

    if (parse_result.help_requested) {
      std::cout << cex::apps::engine::engine_app_usage(
          argc > 0 && argv[0] != nullptr ? argv[0] : "engine_app");
      return EXIT_SUCCESS;
    }

    const auto& config = parse_result.config;
    std::cerr << "starting engine_app bootstrap_servers="
              << config.bootstrap_servers << " group_id="
              << config.consumer_group_id << " input_topic="
              << config.input_topic << " replies_topic="
              << config.replies_topic << " events_topic="
              << config.events_topic << " checkpoint_dir="
              << config.checkpoint_directory.string() << '\n';

    auto symbols = cex::apps::engine::symbol_configs_for_runtime(config);
    cex::runtime::EngineRuntime runtime(cex::runtime::EngineRuntimeConfig{
        .symbols = std::move(symbols),
        .first_public_sequence = 1,
    });

    cex::checkpoint::FileCheckpointStore checkpoint_store(
        config.checkpoint_directory);
    cex::checkpoint::EngineCheckpointManager checkpoint_manager;
    restore_latest_checkpoint(checkpoint_store, checkpoint_manager, runtime);

    cex::broker::RdKafkaConsumerConfig consumer_config{
        .bootstrap_servers = config.bootstrap_servers,
        .group_id = config.consumer_group_id,
        .topics = {config.input_topic},
        .client_id = "engine-app-input-consumer",
    };
    cex::broker::RdKafkaProducerConfig producer_config{
        .bootstrap_servers = config.bootstrap_servers,
        .client_id = "engine-app-record-producer",
    };
    cex::broker::RdKafkaOffsetCommitterConfig committer_config{
        .bootstrap_servers = config.bootstrap_servers,
        .group_id = config.consumer_group_id,
        .client_id = "engine-app-offset-committer",
    };

    cex::broker::RdKafkaEngineInputConsumer consumer(consumer_config);
    cex::broker::RdKafkaEngineRecordProducer producer(producer_config);
    cex::broker::RdKafkaOffsetCommitter committer(committer_config);
    cex::broker::RedpandaEngineApp app(
        consumer, producer, committer, runtime);

    std::uint64_t poll_count{0};
    while (!config.poll_loop_limit.has_value() ||
           poll_count < *config.poll_loop_limit) {
      ++poll_count;
      const auto result = app.poll_once();
      if (!result.ok()) {
        std::cerr << "engine_app poll failed status="
                  << status_name(result.status);
        if (!result.error.empty()) {
          std::cerr << " error=" << result.error;
        }
        std::cerr << '\n';
        return EXIT_FAILURE;
      }

      if (result.status == cex::broker::EngineBrokerAppStatus::NoRecord) {
        if (config.poll_loop_limit.has_value()) {
          std::cerr << "poll " << poll_count << ": no record\n";
        }
        continue;
      }

      if (result.source.has_value() && result.committed) {
        save_checkpoint_for_source(
            *result.source, runtime, checkpoint_manager, checkpoint_store);
      }

      std::cerr << "poll " << poll_count << ": processed";
      if (result.source.has_value()) {
        std::cerr << " " << result.source->topic << '['
                  << result.source->partition << "] offset="
                  << result.source->offset;
      }
      std::cerr << '\n';
    }

    return EXIT_SUCCESS;
  } catch (const std::exception& error) {
    std::cerr << "engine_app failed: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
