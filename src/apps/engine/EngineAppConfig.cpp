#include "apps/engine/EngineAppConfig.hpp"

#include <charconv>
#include <cstdlib>
#include <cctype>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace cex::apps::engine {
namespace {

[[nodiscard]] bool blank(std::string_view value) {
  for (const unsigned char ch : value) {
    if (std::isspace(ch) == 0) {
      return false;
    }
  }
  return true;
}

void require_non_blank(std::string_view value, std::string_view name) {
  if (blank(value)) {
    throw std::invalid_argument(std::string(name) + " must not be empty");
  }
}

[[nodiscard]] bool starts_with(std::string_view value,
                               std::string_view prefix) {
  return value.size() >= prefix.size() &&
         value.substr(0, prefix.size()) == prefix;
}

[[nodiscard]] std::string take_option_value(
    const std::vector<std::string>& args,
    std::size_t& index,
    std::string_view option) {
  const std::string& arg = args[index];
  const std::string prefix = std::string(option) + "=";
  if (starts_with(arg, prefix)) {
    return arg.substr(prefix.size());
  }

  if (arg != option) {
    throw std::invalid_argument("unknown engine_app option: " + arg);
  }
  if (index + 1 >= args.size()) {
    throw std::invalid_argument(std::string(option) + " requires a value");
  }

  ++index;
  return args[index];
}

[[nodiscard]] std::uint64_t parse_positive_u64(std::string_view value,
                                               std::string_view name) {
  std::uint64_t parsed{0};
  const char* begin = value.data();
  const char* end = value.data() + value.size();
  const auto [ptr, error] = std::from_chars(begin, end, parsed);
  if (error != std::errc{} || ptr != end || parsed == 0) {
    throw std::invalid_argument(std::string(name) +
                                " must be a positive integer");
  }
  return parsed;
}

void apply_environment(EngineAppConfig& config,
                       const EngineAppEnvironment& environment) {
  if (environment.bootstrap_servers.has_value()) {
    require_non_blank(*environment.bootstrap_servers, BootstrapServersEnv);
    config.bootstrap_servers = *environment.bootstrap_servers;
  }
  if (environment.consumer_group_id.has_value()) {
    require_non_blank(*environment.consumer_group_id, ConsumerGroupIdEnv);
    config.consumer_group_id = *environment.consumer_group_id;
  }
  if (environment.checkpoint_directory.has_value()) {
    require_non_blank(*environment.checkpoint_directory, CheckpointDirectoryEnv);
    config.checkpoint_directory = *environment.checkpoint_directory;
  }
  if (environment.poll_loop_limit.has_value()) {
    config.poll_loop_limit =
        parse_positive_u64(*environment.poll_loop_limit, PollLoopLimitEnv);
  }
}

[[nodiscard]] std::optional<std::string> read_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return std::nullopt;
  }
  return std::string(value);
}

}  // namespace

EngineMarketConfig default_sol_perp_market_config() {
  return EngineMarketConfig{
      .market_id = DefaultMarketId,
      .market_name = DefaultMarketName,
      .symbol_config =
          SymbolConfig{
              .symbolId = 1,
              .baseAssetId = 1,
              .quoteAssetId = 2,
              .tickSize = Price::from_ticks(1),
              .lotSize = Quantity::from_lots(1),
              .minQuantity = Quantity::from_lots(1),
              .maxQuantity = Quantity::from_lots(1'000'000),
              .minPrice = Price::from_ticks(1),
              .maxPrice = Price::from_ticks(1'000'000),
              .ringCapacityTicks = 1'000,
              .thresholdPercentage = 10,
              .initialBaseTick = 0,
              .priceScale = 0,
              .quantityScale = 0,
              .makerFeeRate = 0,
              .takerFeeRate = 0,
              .tradingEnabled = true,
          },
  };
}

EngineAppConfig default_engine_app_config() {
  return EngineAppConfig{
      .bootstrap_servers = DefaultBootstrapServers,
      .consumer_group_id = DefaultConsumerGroupId,
      .input_topic = cex::runtime::EngineInputTopic,
      .replies_topic = cex::runtime::EngineRepliesTopic,
      .events_topic = cex::runtime::EngineEventsTopic,
      .checkpoint_directory = DefaultCheckpointDirectory,
      .markets = {default_sol_perp_market_config()},
      .poll_loop_limit = std::nullopt,
      .once = false,
  };
}

EngineAppEnvironment engine_app_environment_from_process() {
  return EngineAppEnvironment{
      .bootstrap_servers = read_env(BootstrapServersEnv),
      .consumer_group_id = read_env(ConsumerGroupIdEnv),
      .checkpoint_directory = read_env(CheckpointDirectoryEnv),
      .poll_loop_limit = read_env(PollLoopLimitEnv),
  };
}

EngineAppConfigParseResult parse_engine_app_config(
    const std::vector<std::string>& args,
    const EngineAppEnvironment& environment) {
  EngineAppConfigParseResult result{
      .config = default_engine_app_config(),
      .help_requested = false,
  };
  apply_environment(result.config, environment);

  for (std::size_t index = 0; index < args.size(); ++index) {
    const std::string& arg = args[index];

    if (arg == "--help" || arg == "-h") {
      result.help_requested = true;
      continue;
    }
    if (arg == "--once") {
      result.config.once = true;
      result.config.poll_loop_limit = 1;
      continue;
    }
    if (arg == "--bootstrap-servers" ||
        starts_with(arg, "--bootstrap-servers=")) {
      auto value = take_option_value(args, index, "--bootstrap-servers");
      require_non_blank(value, "--bootstrap-servers");
      result.config.bootstrap_servers = std::move(value);
      continue;
    }
    if (arg == "--group-id" || starts_with(arg, "--group-id=")) {
      auto value = take_option_value(args, index, "--group-id");
      require_non_blank(value, "--group-id");
      result.config.consumer_group_id = std::move(value);
      continue;
    }
    if (arg == "--checkpoint-dir" || starts_with(arg, "--checkpoint-dir=")) {
      auto value = take_option_value(args, index, "--checkpoint-dir");
      require_non_blank(value, "--checkpoint-dir");
      result.config.checkpoint_directory = std::move(value);
      continue;
    }
    if (arg == "--poll-limit" || starts_with(arg, "--poll-limit=")) {
      const auto value = take_option_value(args, index, "--poll-limit");
      result.config.poll_loop_limit =
          parse_positive_u64(value, "--poll-limit");
      result.config.once = result.config.poll_loop_limit == 1;
      continue;
    }

    throw std::invalid_argument("unknown engine_app option: " + arg);
  }

  return result;
}

std::vector<SymbolConfig> symbol_configs_for_runtime(
    const EngineAppConfig& config) {
  std::vector<SymbolConfig> symbols;
  symbols.reserve(config.markets.size());
  for (const auto& market : config.markets) {
    symbols.push_back(market.symbol_config);
  }
  return symbols;
}

std::string engine_app_usage(std::string_view executable_name) {
  std::string name(executable_name);
  if (name.empty()) {
    name = "engine_app";
  }

  return "Usage: " + name + " [options]\n"
         "\n"
         "Options:\n"
         "  --bootstrap-servers <servers>  Kafka bootstrap servers "
         "(default 127.0.0.1:9092)\n"
         "  --group-id <group>             Consumer group id "
         "(default cex-engine)\n"
         "  --checkpoint-dir <path>        File checkpoint directory "
         "(default .data/engine/checkpoints)\n"
         "  --poll-limit <count>           Stop after count poll attempts\n"
         "  --once                         Stop after one poll attempt\n"
         "  -h, --help                     Show this help\n"
         "\n"
         "Environment:\n"
         "  CEX_ENGINE_BOOTSTRAP_SERVERS\n"
         "  CEX_ENGINE_GROUP_ID\n"
         "  CEX_ENGINE_CHECKPOINT_DIR\n"
         "  CEX_ENGINE_POLL_LIMIT\n";
}

}  // namespace cex::apps::engine
