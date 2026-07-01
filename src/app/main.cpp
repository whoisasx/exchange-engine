#include "EngineAppConfig.hpp"
#include "broker/RedpandaEngineApp.hpp"
#include "broker/rdkafka/RdKafkaEngineBroker.hpp"
#include "checkpoint/EngineCheckpointManager.hpp"
#include "checkpoint/ICheckpointStore.hpp"
#include "checkpoint/file/FileCheckpointStore.hpp"
#include "checkpoint/s3/S3CheckpointStore.hpp"
#include "recovery/RecoveryCoordinator.hpp"
#include "runtime/EngineRuntime.hpp"

#include <cstdint>
#include <cstdlib>
#include <atomic>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
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
    case cex::broker::EngineBrokerAppStatus::RejectedInputPartition:
      return "RejectedInputPartition";
    case cex::broker::EngineBrokerAppStatus::RejectedInputMarket:
      return "RejectedInputMarket";
    case cex::broker::EngineBrokerAppStatus::PublishFailed:
      return "PublishFailed";
    case cex::broker::EngineBrokerAppStatus::CheckpointFailed:
      return "CheckpointFailed";
    case cex::broker::EngineBrokerAppStatus::CommitFailed:
      return "CommitFailed";
    case cex::broker::EngineBrokerAppStatus::ProcessingFailed:
      return "ProcessingFailed";
    case cex::broker::EngineBrokerAppStatus::UnsafeDuplicate:
      return "UnsafeDuplicate";
  }
  return "Unknown";
}

[[nodiscard]] const char* status_name(cex::recovery::RecoveryStatus status) {
  switch (status) {
    case cex::recovery::RecoveryStatus::NoCheckpoint:
      return "NoCheckpoint";
    case cex::recovery::RecoveryStatus::Recovered:
      return "Recovered";
    case cex::recovery::RecoveryStatus::Replayed:
      return "Replayed";
    case cex::recovery::RecoveryStatus::SourceMismatch:
      return "SourceMismatch";
    case cex::recovery::RecoveryStatus::CheckpointLoadFailed:
      return "CheckpointLoadFailed";
    case cex::recovery::RecoveryStatus::WatermarkUnavailable:
      return "WatermarkUnavailable";
    case cex::recovery::RecoveryStatus::InvalidWatermark:
      return "InvalidWatermark";
    case cex::recovery::RecoveryStatus::OffsetBelowLowWatermark:
      return "OffsetBelowLowWatermark";
    case cex::recovery::RecoveryStatus::OffsetAboveHighWatermark:
      return "OffsetAboveHighWatermark";
    case cex::recovery::RecoveryStatus::RestoreFailed:
      return "RestoreFailed";
    case cex::recovery::RecoveryStatus::SeekFailed:
      return "SeekFailed";
    case cex::recovery::RecoveryStatus::ReplayFailed:
      return "ReplayFailed";
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

[[nodiscard]] const char* bool_name(bool value) noexcept {
  return value ? "true" : "false";
}

[[nodiscard]] const char* checkpoint_store_name(
    cex::engine_app::EngineCheckpointStoreKind kind) {
  switch (kind) {
    case cex::engine_app::EngineCheckpointStoreKind::File:
      return "file";
    case cex::engine_app::EngineCheckpointStoreKind::S3:
      return "s3";
  }
  return "unknown";
}

[[nodiscard]] cex::checkpoint::S3CheckpointStoreConfig s3_checkpoint_config(
    const cex::engine_app::EngineS3CheckpointConfig& config) {
  return cex::checkpoint::S3CheckpointStoreConfig{
      .endpoint = config.endpoint,
      .bucket = config.bucket,
      .access_key = config.access_key,
      .secret_key = config.secret_key,
      .region = config.region,
      .prefix = config.prefix,
  };
}

[[nodiscard]] std::string checkpoint_scope_for(std::string_view topic,
                                               std::int32_t partition) {
  std::ostringstream output;
  output << safe_checkpoint_topic(topic) << "-p" << std::setw(10)
         << std::setfill('0') << partition;
  return output.str();
}

[[nodiscard]] std::string append_s3_prefix(std::string base,
                                           std::string scope) {
  if (base.empty()) {
    return scope;
  }
  if (base.back() == '/') {
    return base + scope;
  }
  return base + "/" + scope;
}

[[nodiscard]] std::unique_ptr<cex::checkpoint::ICheckpointStore>
make_checkpoint_store(const cex::engine_app::EngineAppConfig& config,
                      std::string_view topic,
                      std::int32_t partition) {
  const auto scope = checkpoint_scope_for(topic, partition);
  switch (config.checkpoint_store) {
    case cex::engine_app::EngineCheckpointStoreKind::File: {
      return std::make_unique<cex::checkpoint::FileCheckpointStore>(
          config.checkpoint_directory / scope);
    }
    case cex::engine_app::EngineCheckpointStoreKind::S3: {
      auto s3_config = s3_checkpoint_config(config.s3_checkpoint);
      s3_config.prefix = append_s3_prefix(std::move(s3_config.prefix), scope);
      return std::make_unique<cex::checkpoint::S3CheckpointStore>(
          std::move(s3_config));
    }
  }
  throw std::invalid_argument("unknown checkpoint store kind");
}

[[nodiscard]] std::string checkpoint_store_summary(
    const cex::engine_app::EngineAppConfig& config) {
  std::ostringstream summary;
  summary << "checkpoint_store=" << checkpoint_store_name(config.checkpoint_store);
  if (config.checkpoint_store ==
      cex::engine_app::EngineCheckpointStoreKind::File) {
    summary << " checkpoint_dir=" << config.checkpoint_directory.string();
  } else {
    summary << " checkpoint_s3_endpoint=" << config.s3_checkpoint.endpoint
            << " checkpoint_s3_bucket=" << config.s3_checkpoint.bucket
            << " checkpoint_s3_region=" << config.s3_checkpoint.region
            << " checkpoint_s3_prefix="
            << (config.s3_checkpoint.prefix.empty()
                    ? std::string{"<none>"}
                    : config.s3_checkpoint.prefix);
  }
  return summary.str();
}

void log_trace_summary(
    const std::optional<cex::runtime::EngineTraceSummary>& trace) {
  if (!trace.has_value()) {
    std::cerr << " request_id=<none> source_input_id=<none>"
              << " reply_count=0 event_count=0"
              << " duplicate=<none> no_output=<none>";
    return;
  }

  std::cerr << " request_id="
            << (trace->request_id.has_value() ? *trace->request_id : "<none>")
            << " source_input_id="
            << (trace->source_input_id.has_value() ? *trace->source_input_id
                                                   : "<none>")
            << " reply_count=" << trace->reply_count
            << " event_count=" << trace->event_count
            << " duplicate=" << bool_name(trace->duplicate)
            << " no_output=" << bool_name(trace->no_output);
}

[[nodiscard]] std::int64_t next_offset(const cex::broker::ConsumedRecord& source) {
  if (source.offset == std::numeric_limits<std::int64_t>::max()) {
    throw std::runtime_error("cannot checkpoint source offset without overflow");
  }
  return source.offset + 1;
}

[[nodiscard]] std::string checkpoint_id_for(
    const cex::checkpoint::CheckpointSourcePosition& source) {
  std::ostringstream output;
  output << "source-" << safe_checkpoint_topic(source.topic) << "-p"
         << std::setw(10) << std::setfill('0') << source.partition << "-o"
         << std::setw(20) << std::setfill('0') << source.next_offset;
  return output.str();
}

[[nodiscard]] std::string checkpoint_id_for(
    const cex::broker::ConsumedRecord& source) {
  return checkpoint_id_for(cex::checkpoint::CheckpointSourcePosition{
      .topic = source.topic,
      .partition = source.partition,
      .next_offset = next_offset(source),
  });
}

std::optional<std::string> save_checkpoint_for_source(
    const cex::checkpoint::CheckpointSourcePosition& source_position,
    const cex::runtime::EngineRuntime& runtime,
    cex::checkpoint::EngineCheckpointManager& checkpoint_manager,
    cex::checkpoint::ICheckpointStore& checkpoint_store) {
  auto checkpoint = checkpoint_manager.create_checkpoint(
      runtime, source_position, checkpoint_id_for(source_position));
  const auto checkpoint_id = checkpoint.checkpoint_id;
  checkpoint_store.save(std::move(checkpoint));

  std::cerr << "saved checkpoint " << checkpoint_id << " at "
            << source_position.topic << '[' << source_position.partition
            << "] next_offset=" << source_position.next_offset << '\n';
  return std::nullopt;
}

std::optional<std::string> save_checkpoint_for_source(
    const cex::broker::ConsumedRecord& source,
    const cex::runtime::EngineRuntime& runtime,
    cex::checkpoint::EngineCheckpointManager& checkpoint_manager,
    cex::checkpoint::ICheckpointStore& checkpoint_store) {
  return save_checkpoint_for_source(
      cex::checkpoint::CheckpointSourcePosition{
          .topic = source.topic,
          .partition = source.partition,
          .next_offset = next_offset(source),
      },
      runtime,
      checkpoint_manager,
      checkpoint_store);
}

void log_recovery_result(
    const cex::recovery::RecoveryResult& result,
    const cex::engine_app::EngineAppConfig& config) {
  std::cerr << "engine_app recovery status=" << status_name(result.status);
  if (result.checkpoint_id.empty()) {
    std::cerr << " checkpoint_id=<none>";
  } else {
    std::cerr << " checkpoint_id=" << result.checkpoint_id;
  }

  std::cerr << ' ' << checkpoint_store_summary(config);
  if (result.source_position.has_value()) {
    const auto& position = *result.source_position;
    std::cerr << " source=" << position.topic << '[' << position.partition
              << "] checkpoint_next_offset=" << position.next_offset;
  } else {
    std::cerr << " source=<none> checkpoint_next_offset=<none>";
  }

  if (result.watermark.has_value()) {
    std::cerr << " watermark_low=" << result.watermark->low
              << " watermark_high=" << result.watermark->high;
  } else {
    std::cerr << " watermark_low=<none> watermark_high=<none>";
  }

  std::cerr << " replayed_records=" << result.replayed_records
            << " replay_output_records=" << result.replay_output_records
            << " next_offset=" << result.next_offset;
  if (result.last_replayed_offset.has_value()) {
    std::cerr << " last_replayed_offset=" << *result.last_replayed_offset;
  } else {
    std::cerr << " last_replayed_offset=<none>";
  }
  std::cerr << " caught_up=" << bool_name(result.caught_up);
  if (!result.error.empty()) {
    std::cerr << " error=" << result.error;
  }
  std::cerr << '\n';
}

void fail_if_recovery_incomplete(
    const cex::recovery::RecoveryResult& result) {
  if (!result.ok()) {
    std::string error = "startup recovery failed with status ";
    error += status_name(result.status);
    if (!result.error.empty()) {
      error += ": ";
      error += result.error;
    }
    throw std::runtime_error(std::move(error));
  }

  if (result.source_position.has_value() && !result.caught_up) {
    throw std::runtime_error(
        "startup recovery did not catch up before live polling");
  }
}

[[nodiscard]] cex::runtime::InboundEngineRecord startup_liquidation_source() {
  return cex::runtime::InboundEngineRecord{
      .topic = "engine.startup",
      .partition = -1,
      .offset = -1,
      .key = std::string{"startup-automatic-liquidation"},
      .raw_json = "",
  };
}

[[nodiscard]] cex::checkpoint::CheckpointSourcePosition
startup_checkpoint_position(const cex::recovery::RecoveryResult& result,
                            std::string fallback_topic,
                            std::int32_t fallback_partition) {
  if (result.source_position.has_value()) {
    return cex::checkpoint::CheckpointSourcePosition{
        .topic = result.source_position->topic,
        .partition = result.source_position->partition,
        .next_offset = result.next_offset,
    };
  }

  return cex::checkpoint::CheckpointSourcePosition{
      .topic = std::move(fallback_topic),
      .partition = fallback_partition,
      .next_offset = result.next_offset,
  };
}

void run_startup_automatic_liquidation_scan(
    cex::runtime::EngineRuntime& runtime,
    cex::broker::RedpandaEngineApp& app,
    const cex::recovery::RecoveryResult& recovery_result,
    const std::string& fallback_input_topic,
    std::int32_t fallback_input_partition,
    cex::checkpoint::EngineCheckpointManager& checkpoint_manager,
    cex::checkpoint::ICheckpointStore& checkpoint_store) {
  auto process_result =
      runtime.evaluate_all_automatic_liquidations(startup_liquidation_source());
  if (process_result.empty()) {
    std::cerr << "startup automatic liquidation scan emitted no records\n";
    return;
  }

  const auto publish_result = app.publish(process_result);
  if (!publish_result.ok()) {
    throw std::runtime_error("startup automatic liquidation publish failed: " +
                             publish_result.failures.front().error);
  }

  const auto checkpoint_position =
      startup_checkpoint_position(
          recovery_result, fallback_input_topic, fallback_input_partition);
  if (auto error = save_checkpoint_for_source(checkpoint_position,
                                              runtime,
                                              checkpoint_manager,
                                              checkpoint_store);
      error.has_value()) {
    throw std::runtime_error(
        "startup automatic liquidation checkpoint failed: " + *error);
  }

  std::cerr << "startup automatic liquidation scan published "
            << publish_result.published << " records\n";
}

[[nodiscard]] TradeId first_trade_id_for_market(
    cex::adapter::MarketId market_id) {
  constexpr TradeId TradeIdNamespaceSize = 1'000'000'000'000ULL;
  if (market_id <= 0) {
    throw std::invalid_argument("market_id must be positive for trade id seed");
  }
  const auto market = static_cast<TradeId>(market_id);
  if (market >
      (std::numeric_limits<TradeId>::max() - 1) / TradeIdNamespaceSize) {
    throw std::invalid_argument("market_id is too large for trade id seed");
  }
  return market * TradeIdNamespaceSize + 1;
}

void run_engine_partition_worker(
    const cex::engine_app::EngineAppConfig& config,
    cex::engine_app::EngineMarketConfig market,
    std::atomic_bool& stop_requested) {
  std::cerr << "starting engine worker market_id=" << market.market_id
            << " market_name=" << market.market_name
            << " input_partition=" << market.input_partition << '\n';

  cex::runtime::EngineRuntime runtime(cex::runtime::EngineRuntimeConfig{
      .symbols = {market.symbol_config},
      .first_public_sequence = 1,
      .first_trade_id = first_trade_id_for_market(market.market_id),
  });

  auto checkpoint_store =
      make_checkpoint_store(config, config.input_topic, market.input_partition);
  cex::checkpoint::EngineCheckpointManager checkpoint_manager;

  cex::broker::RdKafkaConsumerConfig consumer_config{
      .bootstrap_servers = config.bootstrap_servers,
      .group_id = config.consumer_group_id,
      .topics = {},
      .assigned_partitions =
          {cex::broker::RdKafkaAssignedPartition{
              .topic = config.input_topic,
              .partition = market.input_partition,
              .offset = 0,
          }},
      .client_id = "engine-app-input-consumer-p" +
                   std::to_string(market.input_partition),
  };

  cex::broker::RdKafkaProducerConfig producer_config{
      .bootstrap_servers = config.bootstrap_servers,
      .client_id = "engine-app-record-producer-p" +
                   std::to_string(market.input_partition),
  };

  cex::broker::RdKafkaEngineInputConsumer consumer(consumer_config);
  cex::recovery::RecoveryCoordinator recovery_coordinator(
      *checkpoint_store,
      consumer,
      runtime,
      checkpoint_manager,
      cex::recovery::RecoverySource{
          .topic = config.input_topic,
          .partition = market.input_partition,
      });
  const auto recovery_result = recovery_coordinator.recover_and_replay();
  log_recovery_result(recovery_result, config);
  fail_if_recovery_incomplete(recovery_result);

  cex::broker::RdKafkaEngineRecordProducer producer(producer_config);
  cex::broker::RedpandaEngineApp app(
      consumer,
      producer,
      consumer,
      runtime,
      cex::broker::EngineInputGuardConfig{
          .topic = config.input_topic,
          .partition = market.input_partition,
          .market_ids = {market.market_id},
      },
      [&](const cex::broker::ConsumedRecord& source,
          const cex::runtime::EngineRuntime& checkpoint_runtime) {
        return save_checkpoint_for_source(source,
                                          checkpoint_runtime,
                                          checkpoint_manager,
                                          *checkpoint_store);
      });

  run_startup_automatic_liquidation_scan(runtime,
                                         app,
                                         recovery_result,
                                         config.input_topic,
                                         market.input_partition,
                                         checkpoint_manager,
                                         *checkpoint_store);

  std::uint64_t poll_count{0};
  while (!stop_requested.load() &&
         (!config.poll_loop_limit.has_value() ||
          poll_count < *config.poll_loop_limit)) {
    ++poll_count;
    const auto result = app.poll_once();
    if (!result.ok()) {
      std::ostringstream error;
      error << "engine worker market_id=" << market.market_id
            << " input_partition=" << market.input_partition
            << " poll failed status=" << status_name(result.status);
      if (!result.error.empty()) {
        error << " error=" << result.error;
      }
      throw std::runtime_error(error.str());
    }

    if (result.status == cex::broker::EngineBrokerAppStatus::NoRecord) {
      if (config.poll_loop_limit.has_value()) {
        std::cerr << "worker market_id=" << market.market_id
                  << " partition=" << market.input_partition << " poll "
                  << poll_count << ": no record\n";
      }
      continue;
    }

    std::cerr << "worker market_id=" << market.market_id
              << " partition=" << market.input_partition << " poll "
              << poll_count << ": processed";
    if (result.source.has_value()) {
      std::cerr << " " << result.source->topic << '['
                << result.source->partition << "] offset="
                << result.source->offset;
    }
    log_trace_summary(result.trace);
    std::cerr << '\n';
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  try {
    const auto parse_result = cex::engine_app::parse_engine_app_config(
        args_from_argv(argc, argv),
        cex::engine_app::engine_app_environment_from_process());

    if (parse_result.help_requested) {
      std::cout << cex::engine_app::engine_app_usage(
          argc > 0 && argv[0] != nullptr ? argv[0] : "engine_app");
      return EXIT_SUCCESS;
    }

    const auto& config = parse_result.config;
    std::cerr << "starting engine_app bootstrap_servers="
              << config.bootstrap_servers << " group_id="
              << config.consumer_group_id << " input_topic="
              << config.input_topic << " replies_topic="
              << config.replies_topic << " events_topic="
              << config.events_topic << ' '
              << checkpoint_store_summary(config) << '\n';

    std::atomic_bool stop_requested{false};
    std::mutex errors_mutex;
    std::vector<std::string> worker_errors;
    std::vector<std::jthread> workers;
    workers.reserve(config.markets.size());

    for (const auto& market : config.markets) {
      workers.emplace_back([&config,
                            &stop_requested,
                            &errors_mutex,
                            &worker_errors,
                            market] {
        try {
          run_engine_partition_worker(config, market, stop_requested);
        } catch (const std::exception& error) {
          {
            std::lock_guard<std::mutex> lock(errors_mutex);
            worker_errors.push_back(error.what());
          }
          stop_requested.store(true);
        }
      });
    }

    for (auto& worker : workers) {
      if (worker.joinable()) {
        worker.join();
      }
    }

    if (!worker_errors.empty()) {
      for (const auto& error : worker_errors) {
        std::cerr << "engine_app worker failed: " << error << '\n';
      }
      return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
  } catch (const std::exception& error) {
    std::cerr << "engine_app failed: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
