#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "adapter/EngineAdapter.hpp"
#include "core/SymbolConfig.hpp"
#include "runtime/EngineRuntimeConfig.hpp"

namespace cex::apps::engine {

inline constexpr const char* DefaultBootstrapServers = "127.0.0.1:9092";
inline constexpr const char* DefaultConsumerGroupId = "cex-engine";
inline constexpr const char* DefaultCheckpointDirectory =
    ".data/engine/checkpoints";
inline constexpr const char* DefaultMarketName = "SOL-PERP";
inline constexpr cex::adapter::MarketId DefaultMarketId = 1;

inline constexpr const char* BootstrapServersEnv =
    "CEX_ENGINE_BOOTSTRAP_SERVERS";
inline constexpr const char* ConsumerGroupIdEnv = "CEX_ENGINE_GROUP_ID";
inline constexpr const char* CheckpointDirectoryEnv =
    "CEX_ENGINE_CHECKPOINT_DIR";
inline constexpr const char* PollLoopLimitEnv = "CEX_ENGINE_POLL_LIMIT";

struct EngineMarketConfig {
  cex::adapter::MarketId market_id{DefaultMarketId};
  std::string market_name{DefaultMarketName};
  SymbolConfig symbol_config;
};

struct EngineAppConfig {
  std::string bootstrap_servers;
  std::string consumer_group_id;
  std::string input_topic;
  std::string replies_topic;
  std::string events_topic;
  std::filesystem::path checkpoint_directory;
  std::vector<EngineMarketConfig> markets;
  std::optional<std::uint64_t> poll_loop_limit;
  bool once{false};
};

struct EngineAppEnvironment {
  std::optional<std::string> bootstrap_servers;
  std::optional<std::string> consumer_group_id;
  std::optional<std::string> checkpoint_directory;
  std::optional<std::string> poll_loop_limit;
};

struct EngineAppConfigParseResult {
  EngineAppConfig config;
  bool help_requested{false};
};

[[nodiscard]] EngineMarketConfig default_sol_perp_market_config();
[[nodiscard]] EngineAppConfig default_engine_app_config();
[[nodiscard]] EngineAppEnvironment engine_app_environment_from_process();

[[nodiscard]] EngineAppConfigParseResult parse_engine_app_config(
    const std::vector<std::string>& args,
    const EngineAppEnvironment& environment = {});

[[nodiscard]] std::vector<SymbolConfig> symbol_configs_for_runtime(
    const EngineAppConfig& config);

[[nodiscard]] std::string engine_app_usage(std::string_view executable_name);

}  // namespace cex::apps::engine
