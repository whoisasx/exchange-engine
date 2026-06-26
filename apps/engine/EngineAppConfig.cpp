#include "EngineAppConfig.hpp"

#include <charconv>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <unordered_set>
#include <utility>

namespace cex::apps::engine {
namespace {

struct MarketConfigBuilder {
  explicit MarketConfigBuilder(std::size_t start) : start_line(start) {}

  std::size_t start_line{0};
  std::optional<cex::adapter::MarketId> market_id;
  std::optional<std::string> market_name;
  std::optional<std::int64_t> tick_size;
  std::optional<std::uint64_t> lot_size;
  std::optional<std::uint64_t> min_quantity;
  std::optional<std::uint64_t> max_quantity;
  std::optional<std::int64_t> min_price;
  std::optional<std::int64_t> max_price;
  std::optional<std::uint64_t> ring_capacity_ticks;
  std::optional<std::uint64_t> threshold_percentage;
  std::optional<std::int64_t> initial_base_tick;
  std::optional<std::uint8_t> price_scale;
  std::optional<std::uint8_t> quantity_scale;
  std::optional<FeeRate> maker_fee_rate;
  std::optional<FeeRate> taker_fee_rate;
  std::optional<bool> trading_enabled;
  std::unordered_set<std::string> keys;
};

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

[[nodiscard]] std::string_view trim(std::string_view value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.remove_prefix(1);
  }
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.back())) != 0) {
    value.remove_suffix(1);
  }
  return value;
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

[[nodiscard]] std::string config_error_prefix(
    const std::filesystem::path& path,
    std::size_t line_number) {
  return "invalid markets config '" + path.string() + "' at line " +
         std::to_string(line_number) + ": ";
}

template <typename Integer>
[[nodiscard]] Integer parse_integer_field(std::string_view value,
                                          std::string_view key,
                                          const std::filesystem::path& path,
                                          std::size_t line_number) {
  Integer parsed{};
  const char* begin = value.data();
  const char* end = value.data() + value.size();
  const auto [ptr, error] = std::from_chars(begin, end, parsed);
  if (error != std::errc{} || ptr != end) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                std::string(key) + " must be an integer");
  }
  return parsed;
}

[[nodiscard]] std::uint8_t parse_u8_field(std::string_view value,
                                          std::string_view key,
                                          const std::filesystem::path& path,
                                          std::size_t line_number) {
  const auto parsed =
      parse_integer_field<unsigned int>(value, key, path, line_number);
  if (parsed > std::numeric_limits<std::uint8_t>::max()) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                std::string(key) +
                                " must fit in an unsigned 8-bit integer");
  }
  return static_cast<std::uint8_t>(parsed);
}

[[nodiscard]] bool parse_bool_field(std::string_view value,
                                    std::string_view key,
                                    const std::filesystem::path& path,
                                    std::size_t line_number) {
  if (value == "true" || value == "1") {
    return true;
  }
  if (value == "false" || value == "0") {
    return false;
  }
  throw std::invalid_argument(config_error_prefix(path, line_number) +
                              std::string(key) +
                              " must be true, false, 1, or 0");
}

template <typename T>
[[nodiscard]] T required_market_field(const std::optional<T>& value,
                                      std::string_view key,
                                      const std::filesystem::path& path,
                                      std::size_t line_number) {
  if (!value.has_value()) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "missing required key " + std::string(key));
  }
  return *value;
}

[[nodiscard]] SymbolId symbol_id_from_market_id(
    cex::adapter::MarketId market_id,
    const std::filesystem::path& path,
    std::size_t line_number) {
  if (market_id <= 0 ||
      market_id > static_cast<cex::adapter::MarketId>(
                      std::numeric_limits<SymbolId>::max())) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "market_id must be positive and fit in "
                                "SymbolId");
  }
  return static_cast<SymbolId>(market_id);
}

void validate_loaded_market(const EngineMarketConfig& market,
                            const std::filesystem::path& path,
                            std::size_t line_number) {
  const auto& symbol = market.symbol_config;
  if (market.market_id <= 0) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "market_id must be positive");
  }
  if (blank(market.market_name)) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "market_name must not be empty");
  }
  if (symbol.tickSize.ticks() <= 0) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "tick_size must be positive");
  }
  if (!symbol.lotSize.is_valid()) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "lot_size must be positive");
  }
  if (!symbol.minQuantity.is_valid()) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "min_quantity must be positive");
  }
  if (symbol.maxQuantity < symbol.minQuantity) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "max_quantity must be >= min_quantity");
  }
  if (!symbol.minPrice.is_valid()) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "min_price must be positive");
  }
  if (symbol.maxPrice < symbol.minPrice) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "max_price must be >= min_price");
  }
  if (symbol.ringCapacityTicks == 0) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "ring_capacity_ticks must be positive");
  }
}

void assign_market_key(MarketConfigBuilder& builder,
                       std::string_view key,
                       std::string_view value,
                       const std::filesystem::path& path,
                       std::size_t line_number) {
  if (blank(key)) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "key must not be empty");
  }
  if (blank(value)) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                std::string(key) + " must not be empty");
  }

  const std::string key_text(key);
  if (!builder.keys.insert(key_text).second) {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "duplicate key " + key_text);
  }

  if (key == "market_id") {
    builder.market_id = parse_integer_field<cex::adapter::MarketId>(
        value, key, path, line_number);
  } else if (key == "market_name") {
    builder.market_name = std::string(value);
  } else if (key == "tick_size") {
    builder.tick_size =
        parse_integer_field<std::int64_t>(value, key, path, line_number);
  } else if (key == "lot_size") {
    builder.lot_size =
        parse_integer_field<std::uint64_t>(value, key, path, line_number);
  } else if (key == "min_quantity") {
    builder.min_quantity =
        parse_integer_field<std::uint64_t>(value, key, path, line_number);
  } else if (key == "max_quantity") {
    builder.max_quantity =
        parse_integer_field<std::uint64_t>(value, key, path, line_number);
  } else if (key == "min_price") {
    builder.min_price =
        parse_integer_field<std::int64_t>(value, key, path, line_number);
  } else if (key == "max_price") {
    builder.max_price =
        parse_integer_field<std::int64_t>(value, key, path, line_number);
  } else if (key == "ring_capacity_ticks") {
    builder.ring_capacity_ticks =
        parse_integer_field<std::uint64_t>(value, key, path, line_number);
  } else if (key == "threshold_percentage") {
    builder.threshold_percentage =
        parse_integer_field<std::uint64_t>(value, key, path, line_number);
  } else if (key == "initial_base_tick") {
    builder.initial_base_tick =
        parse_integer_field<std::int64_t>(value, key, path, line_number);
  } else if (key == "price_scale") {
    builder.price_scale = parse_u8_field(value, key, path, line_number);
  } else if (key == "quantity_scale") {
    builder.quantity_scale = parse_u8_field(value, key, path, line_number);
  } else if (key == "maker_fee_rate") {
    builder.maker_fee_rate =
        parse_integer_field<FeeRate>(value, key, path, line_number);
  } else if (key == "taker_fee_rate") {
    builder.taker_fee_rate =
        parse_integer_field<FeeRate>(value, key, path, line_number);
  } else if (key == "trading_enabled") {
    builder.trading_enabled = parse_bool_field(value, key, path, line_number);
  } else {
    throw std::invalid_argument(config_error_prefix(path, line_number) +
                                "unknown key " + key_text);
  }
}

[[nodiscard]] EngineMarketConfig build_market_config(
    const MarketConfigBuilder& builder,
    const std::filesystem::path& path) {
  const auto line_number = builder.start_line;
  const auto market_id =
      required_market_field(builder.market_id, "market_id", path, line_number);

  EngineMarketConfig market{
      .market_id = market_id,
      .market_name = required_market_field(
          builder.market_name, "market_name", path, line_number),
      .symbol_config =
          SymbolConfig{
              .symbolId = symbol_id_from_market_id(market_id, path, line_number),
              .baseAssetId = 1,
              .quoteAssetId = 2,
              .tickSize = Price::from_ticks(required_market_field(
                  builder.tick_size, "tick_size", path, line_number)),
              .lotSize = Quantity::from_lots(required_market_field(
                  builder.lot_size, "lot_size", path, line_number)),
              .minQuantity = Quantity::from_lots(required_market_field(
                  builder.min_quantity, "min_quantity", path, line_number)),
              .maxQuantity = Quantity::from_lots(required_market_field(
                  builder.max_quantity, "max_quantity", path, line_number)),
              .minPrice = Price::from_ticks(required_market_field(
                  builder.min_price, "min_price", path, line_number)),
              .maxPrice = Price::from_ticks(required_market_field(
                  builder.max_price, "max_price", path, line_number)),
              .ringCapacityTicks = required_market_field(
                  builder.ring_capacity_ticks,
                  "ring_capacity_ticks",
                  path,
                  line_number),
              .thresholdPercentage = required_market_field(
                  builder.threshold_percentage,
                  "threshold_percentage",
                  path,
                  line_number),
              .initialBaseTick = required_market_field(
                  builder.initial_base_tick,
                  "initial_base_tick",
                  path,
                  line_number),
              .priceScale = required_market_field(
                  builder.price_scale, "price_scale", path, line_number),
              .quantityScale = required_market_field(
                  builder.quantity_scale, "quantity_scale", path, line_number),
              .makerFeeRate = required_market_field(
                  builder.maker_fee_rate, "maker_fee_rate", path, line_number),
              .takerFeeRate = required_market_field(
                  builder.taker_fee_rate, "taker_fee_rate", path, line_number),
              .tradingEnabled = required_market_field(
                  builder.trading_enabled,
                  "trading_enabled",
                  path,
                  line_number),
          },
  };
  validate_loaded_market(market, path, line_number);
  return market;
}

void append_market_config(
    const MarketConfigBuilder& builder,
    const std::filesystem::path& path,
    std::vector<EngineMarketConfig>& markets,
    std::unordered_set<cex::adapter::MarketId>& market_ids,
    std::unordered_set<std::string>& market_names) {
  auto market = build_market_config(builder, path);
  if (!market_ids.insert(market.market_id).second) {
    throw std::invalid_argument(config_error_prefix(path, builder.start_line) +
                                "duplicate market_id " +
                                std::to_string(market.market_id));
  }
  if (!market_names.insert(market.market_name).second) {
    throw std::invalid_argument(config_error_prefix(path, builder.start_line) +
                                "duplicate market_name " +
                                market.market_name);
  }
  markets.push_back(std::move(market));
}

[[nodiscard]] std::vector<EngineMarketConfig> load_markets_config_file(
    const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::invalid_argument("unable to open markets config '" +
                                path.string() + "'");
  }

  std::vector<EngineMarketConfig> markets;
  std::unordered_set<cex::adapter::MarketId> market_ids;
  std::unordered_set<std::string> market_names;
  std::optional<MarketConfigBuilder> current_market;
  std::string line;
  std::size_t line_number{0};

  while (std::getline(input, line)) {
    ++line_number;
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }

    const auto text = trim(line);
    if (text.empty() || starts_with(text, "#")) {
      continue;
    }

    if (text == "[[market]]") {
      if (current_market.has_value()) {
        append_market_config(
            *current_market, path, markets, market_ids, market_names);
      }
      current_market.emplace(line_number);
      continue;
    }

    if (!current_market.has_value()) {
      throw std::invalid_argument(config_error_prefix(path, line_number) +
                                  "expected [[market]] before key/value");
    }

    const auto delimiter = text.find('=');
    if (delimiter == std::string_view::npos) {
      throw std::invalid_argument(config_error_prefix(path, line_number) +
                                  "expected key=value");
    }

    const auto key = trim(text.substr(0, delimiter));
    const auto value = trim(text.substr(delimiter + 1));
    assign_market_key(*current_market, key, value, path, line_number);
  }

  if (current_market.has_value()) {
    append_market_config(*current_market, path, markets, market_ids, market_names);
  }
  if (markets.empty()) {
    throw std::invalid_argument("markets config '" + path.string() +
                                "' must contain at least one [[market]]");
  }
  return markets;
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
  if (environment.markets_config.has_value()) {
    require_non_blank(*environment.markets_config, MarketsConfigEnv);
    config.markets = load_markets_config_file(*environment.markets_config);
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
      .markets_config = read_env(MarketsConfigEnv),
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
    if (arg == "--markets-config" || starts_with(arg, "--markets-config=")) {
      auto value = take_option_value(args, index, "--markets-config");
      require_non_blank(value, "--markets-config");
      result.config.markets = load_markets_config_file(value);
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
         "  --markets-config <path>        Market config file; repeat "
         "[[market]] sections with key=value fields\n"
         "  --poll-limit <count>           Stop after count poll attempts\n"
         "  --once                         Stop after one poll attempt\n"
         "  -h, --help                     Show this help\n"
         "\n"
         "Environment:\n"
         "  CEX_ENGINE_BOOTSTRAP_SERVERS\n"
         "  CEX_ENGINE_GROUP_ID\n"
         "  CEX_ENGINE_CHECKPOINT_DIR\n"
         "  CEX_ENGINE_POLL_LIMIT\n"
         "  CEX_ENGINE_MARKETS_CONFIG\n";
}

}  // namespace cex::apps::engine
