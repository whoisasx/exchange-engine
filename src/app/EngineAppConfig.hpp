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

namespace cex::engine_app {

inline constexpr const char* DefaultBootstrapServers = "127.0.0.1:9092";
inline constexpr const char* DefaultConsumerGroupId = "cex-engine";
inline constexpr const char* DefaultCheckpointDirectory =
    ".data/engine/checkpoints";
inline constexpr const char* DefaultCheckpointS3Endpoint =
    "http://127.0.0.1:59000";
inline constexpr const char* DefaultCheckpointS3Bucket =
    "exchange-checkpoints";
inline constexpr const char* DefaultCheckpointS3AccessKey = "minioadmin";
inline constexpr const char* DefaultCheckpointS3SecretKey = "minioadmin";
inline constexpr const char* DefaultCheckpointS3Region = "us-east-1";
inline constexpr const char* DefaultMarketName = "SOL-PERP";
inline constexpr cex::adapter::MarketId DefaultMarketId = 1;

inline constexpr const char* BootstrapServersEnv =
    "CEX_ENGINE_BOOTSTRAP_SERVERS";
inline constexpr const char* ConsumerGroupIdEnv = "CEX_ENGINE_GROUP_ID";
inline constexpr const char* CheckpointDirectoryEnv =
    "CEX_ENGINE_CHECKPOINT_DIR";
inline constexpr const char* CheckpointStoreEnv =
    "CEX_ENGINE_CHECKPOINT_STORE";
inline constexpr const char* CheckpointS3EndpointEnv =
    "CEX_ENGINE_CHECKPOINT_S3_ENDPOINT";
inline constexpr const char* CheckpointS3BucketEnv =
    "CEX_ENGINE_CHECKPOINT_S3_BUCKET";
inline constexpr const char* CheckpointS3AccessKeyEnv =
    "CEX_ENGINE_CHECKPOINT_S3_ACCESS_KEY";
inline constexpr const char* CheckpointS3SecretKeyEnv =
    "CEX_ENGINE_CHECKPOINT_S3_SECRET_KEY";
inline constexpr const char* CheckpointS3RegionEnv =
    "CEX_ENGINE_CHECKPOINT_S3_REGION";
inline constexpr const char* CheckpointS3PrefixEnv =
    "CEX_ENGINE_CHECKPOINT_S3_PREFIX";
inline constexpr const char* PollLoopLimitEnv = "CEX_ENGINE_POLL_LIMIT";
inline constexpr const char* MarketsConfigEnv = "CEX_ENGINE_MARKETS_CONFIG";

enum class EngineCheckpointStoreKind {
  // Local dev/test fallback. S3 is the production-like engine_app default.
  File,
  S3,
};

struct EngineMarketConfig {
  cex::adapter::MarketId market_id{DefaultMarketId};
  std::string market_name{DefaultMarketName};
  SymbolConfig symbol_config;
};

struct EngineS3CheckpointConfig {
  std::string endpoint{DefaultCheckpointS3Endpoint};
  std::string bucket{DefaultCheckpointS3Bucket};
  std::string access_key{DefaultCheckpointS3AccessKey};
  std::string secret_key{DefaultCheckpointS3SecretKey};
  std::string region{DefaultCheckpointS3Region};
  std::string prefix;
};

struct EngineAppConfig {
  std::string bootstrap_servers;
  std::string consumer_group_id;
  std::string input_topic;
  std::string replies_topic;
  std::string events_topic;
  EngineCheckpointStoreKind checkpoint_store{EngineCheckpointStoreKind::S3};
  std::filesystem::path checkpoint_directory;
  EngineS3CheckpointConfig s3_checkpoint;
  std::vector<EngineMarketConfig> markets;
  std::optional<std::uint64_t> poll_loop_limit;
  bool once{false};
};

struct EngineAppEnvironment {
  std::optional<std::string> bootstrap_servers;
  std::optional<std::string> consumer_group_id;
  std::optional<std::string> checkpoint_store;
  std::optional<std::string> checkpoint_directory;
  std::optional<std::string> checkpoint_s3_endpoint;
  std::optional<std::string> checkpoint_s3_bucket;
  std::optional<std::string> checkpoint_s3_access_key;
  std::optional<std::string> checkpoint_s3_secret_key;
  std::optional<std::string> checkpoint_s3_region;
  std::optional<std::string> checkpoint_s3_prefix;
  std::optional<std::string> poll_loop_limit;
  std::optional<std::string> markets_config;
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

}  // namespace cex::engine_app
