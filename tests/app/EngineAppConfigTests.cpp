#include "EngineAppConfig.hpp"

#include <cassert>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

using namespace cex::engine_app;

namespace {

void assert_contains(const std::string& value, const std::string& token) {
  assert(value.find(token) != std::string::npos);
}

std::filesystem::path write_config_file(std::string_view name,
                                        std::string_view content) {
  const auto directory =
      std::filesystem::current_path() / "EngineAppConfigTests.data";
  std::filesystem::create_directories(directory);

  const auto path = directory / std::string(name);
  std::ofstream output(path);
  output << content;
  assert(output);
  output.close();
  assert(output);
  return path;
}

std::string single_market_config_text(int market_id,
                                      std::string_view market_name,
                                      bool trading_enabled) {
  return "[[market]]\n"
         "market_id = " +
         std::to_string(market_id) +
         "\n"
         "market_name = " +
         std::string(market_name) +
         "\n"
         "tick_size = 5\n"
         "lot_size = 2\n"
         "min_quantity = 2\n"
         "max_quantity = 2000\n"
         "min_price = 10\n"
         "max_price = 200000\n"
         "ring_capacity_ticks = 2048\n"
         "threshold_percentage = 15\n"
         "initial_base_tick = -20\n"
         "price_scale = 2\n"
         "quantity_scale = 3\n"
         "maker_fee_rate = 4\n"
         "taker_fee_rate = 6\n"
         "trading_enabled = " +
         (trading_enabled ? std::string("true") : std::string("false")) +
         "\n";
}

void assert_custom_market(const EngineMarketConfig& market,
                          cex::adapter::MarketId market_id,
                          const std::string& market_name,
                          bool trading_enabled) {
  assert(market.market_id == market_id);
  assert(market.market_name == market_name);

  const auto& symbol = market.symbol_config;
  assert(symbol.symbolId == static_cast<SymbolId>(market_id));
  assert(symbol.tickSize.ticks() == 5);
  assert(symbol.lotSize.lots() == 2);
  assert(symbol.minQuantity.lots() == 2);
  assert(symbol.maxQuantity.lots() == 2000);
  assert(symbol.minPrice.ticks() == 10);
  assert(symbol.maxPrice.ticks() == 200000);
  assert(symbol.ringCapacityTicks == 2048);
  assert(symbol.thresholdPercentage == 15);
  assert(symbol.initialBaseTick == -20);
  assert(symbol.priceScale == 2);
  assert(symbol.quantityScale == 3);
  assert(symbol.makerFeeRate == 4);
  assert(symbol.takerFeeRate == 6);
  assert(symbol.tradingEnabled == trading_enabled);
}

void test_defaults_are_minio_friendly() {
  const auto parsed = parse_engine_app_config({});
  const auto& config = parsed.config;

  assert(!parsed.help_requested);
  assert(config.bootstrap_servers == "127.0.0.1:9092");
  assert(config.consumer_group_id == "cex-engine");
  assert(config.input_topic == cex::runtime::EngineInputTopic);
  assert(config.replies_topic == cex::runtime::EngineRepliesTopic);
  assert(config.events_topic == cex::runtime::EngineEventsTopic);
  assert(config.checkpoint_store == EngineCheckpointStoreKind::S3);
  assert(config.checkpoint_directory == ".data/engine/checkpoints");
  assert(config.s3_checkpoint.endpoint == "http://127.0.0.1:59000");
  assert(config.s3_checkpoint.bucket == "exchange-checkpoints");
  assert(config.s3_checkpoint.access_key == "minioadmin");
  assert(config.s3_checkpoint.secret_key == "minioadmin");
  assert(config.s3_checkpoint.region == "us-east-1");
  assert(config.s3_checkpoint.prefix.empty());
  assert(!config.poll_loop_limit.has_value());
  assert(!config.once);

  assert(config.markets.size() == 1);
  const auto& market = config.markets.front();
  assert(market.market_id == 1);
  assert(market.market_name == "SOL-PERP");
  assert(market.symbol_config.symbolId == 1);
  assert(market.symbol_config.tickSize.ticks() == 1);
  assert(market.symbol_config.lotSize.lots() == 1);
  assert(market.symbol_config.minQuantity.lots() == 1);
  assert(market.symbol_config.maxQuantity.lots() == 1'000'000);
  assert(market.symbol_config.minPrice.ticks() == 1);
  assert(market.symbol_config.maxPrice.ticks() == 1'000'000);
  assert(market.symbol_config.tradingEnabled);
}

void test_file_checkpoint_store_is_opt_in_fallback() {
  const auto config =
      parse_engine_app_config({"--checkpoint-store=file",
                               "--checkpoint-dir=dev/checkpoints"})
          .config;

  assert(config.checkpoint_store == EngineCheckpointStoreKind::File);
  assert(config.checkpoint_directory == "dev/checkpoints");
}

void test_environment_overrides_defaults() {
  const EngineAppEnvironment environment{
      .bootstrap_servers = "redpanda:9092",
      .consumer_group_id = "local-engine",
      .checkpoint_store = "s3",
      .checkpoint_directory = "tmp/checkpoints",
      .checkpoint_s3_endpoint = "http://minio:9000",
      .checkpoint_s3_bucket = "engine-checkpoints",
      .checkpoint_s3_access_key = "access",
      .checkpoint_s3_secret_key = "secret",
      .checkpoint_s3_region = "local",
      .checkpoint_s3_prefix = "blue",
      .poll_loop_limit = "7",
  };

  const auto config = parse_engine_app_config({}, environment).config;

  assert(config.bootstrap_servers == "redpanda:9092");
  assert(config.consumer_group_id == "local-engine");
  assert(config.checkpoint_store == EngineCheckpointStoreKind::S3);
  assert(config.checkpoint_directory == "tmp/checkpoints");
  assert(config.s3_checkpoint.endpoint == "http://minio:9000");
  assert(config.s3_checkpoint.bucket == "engine-checkpoints");
  assert(config.s3_checkpoint.access_key == "access");
  assert(config.s3_checkpoint.secret_key == "secret");
  assert(config.s3_checkpoint.region == "local");
  assert(config.s3_checkpoint.prefix == "blue");
  assert(config.poll_loop_limit == 7);
  assert(!config.once);
}

void test_environment_markets_config_overrides_default_market() {
  const auto path = write_config_file(
      "env.markets.conf", single_market_config_text(7, "ETH-PERP", false));
  EngineAppEnvironment environment{};
  environment.markets_config = path.string();

  const auto config = parse_engine_app_config({}, environment).config;

  assert(config.markets.size() == 1);
  assert_custom_market(config.markets.front(), 7, "ETH-PERP", false);
}

void test_command_line_overrides_environment() {
  const EngineAppEnvironment environment{
      .bootstrap_servers = "env:9092",
      .consumer_group_id = "env-group",
      .checkpoint_store = "file",
      .checkpoint_directory = "env/checkpoints",
      .checkpoint_s3_endpoint = "http://env-minio:9000",
      .checkpoint_s3_bucket = "env-bucket",
      .checkpoint_s3_access_key = "env-access",
      .checkpoint_s3_secret_key = "env-secret",
      .checkpoint_s3_region = "env-region",
      .checkpoint_s3_prefix = "env",
      .poll_loop_limit = "9",
  };
  const std::vector<std::string> args{
      "--bootstrap-servers=cli:9092",
      "--group-id",
      "cli-group",
      "--checkpoint-store=s3",
      "--checkpoint-dir=cli/checkpoints",
      "--checkpoint-s3-endpoint=http://cli-minio:9000",
      "--checkpoint-s3-bucket",
      "cli-bucket",
      "--checkpoint-s3-access-key=cli-access",
      "--checkpoint-s3-secret-key",
      "cli-secret",
      "--checkpoint-s3-region=cli-region",
      "--checkpoint-s3-prefix=cli",
      "--once",
  };

  const auto config = parse_engine_app_config(args, environment).config;

  assert(config.bootstrap_servers == "cli:9092");
  assert(config.consumer_group_id == "cli-group");
  assert(config.checkpoint_store == EngineCheckpointStoreKind::S3);
  assert(config.checkpoint_directory == "cli/checkpoints");
  assert(config.s3_checkpoint.endpoint == "http://cli-minio:9000");
  assert(config.s3_checkpoint.bucket == "cli-bucket");
  assert(config.s3_checkpoint.access_key == "cli-access");
  assert(config.s3_checkpoint.secret_key == "cli-secret");
  assert(config.s3_checkpoint.region == "cli-region");
  assert(config.s3_checkpoint.prefix == "cli");
  assert(config.poll_loop_limit == 1);
  assert(config.once);
}

void test_command_line_markets_config_overrides_environment() {
  const auto env_path = write_config_file(
      "env-override.markets.conf",
      single_market_config_text(3, "ENV-PERP", true));
  const auto cli_path = write_config_file(
      "cli-override.markets.conf",
      single_market_config_text(4, "CLI-PERP", false));

  EngineAppEnvironment environment{};
  environment.markets_config = env_path.string();

  const auto config =
      parse_engine_app_config({"--markets-config", cli_path.string()},
                              environment)
          .config;

  assert(config.markets.size() == 1);
  assert_custom_market(config.markets.front(), 4, "CLI-PERP", false);
}

void test_valid_multi_market_file() {
  const auto path = write_config_file(
      "multi.markets.conf",
      "# engine_app market config\n"
      "\n"
      "[[market]]\n"
      "market_id = 1\n"
      "market_name = SOL-PERP\n"
      "tick_size = 5\n"
      "lot_size = 2\n"
      "min_quantity = 2\n"
      "max_quantity = 2000\n"
      "min_price = 10\n"
      "max_price = 200000\n"
      "ring_capacity_ticks = 2048\n"
      "threshold_percentage = 15\n"
      "initial_base_tick = -20\n"
      "price_scale = 2\n"
      "quantity_scale = 3\n"
      "maker_fee_rate = 4\n"
      "taker_fee_rate = 6\n"
      "trading_enabled = true\n"
      "\n"
      "[[market]]\n"
      "market_id = 2\n"
      "market_name = BTC-PERP\n"
      "tick_size = 10\n"
      "lot_size = 1\n"
      "min_quantity = 1\n"
      "max_quantity = 500\n"
      "min_price = 100\n"
      "max_price = 10000000\n"
      "ring_capacity_ticks = 4096\n"
      "threshold_percentage = 20\n"
      "initial_base_tick = 100\n"
      "price_scale = 1\n"
      "quantity_scale = 4\n"
      "maker_fee_rate = 7\n"
      "taker_fee_rate = 9\n"
      "trading_enabled = false\n");

  const auto config =
      parse_engine_app_config({"--markets-config=" + path.string()}).config;

  assert(config.markets.size() == 2);
  assert_custom_market(config.markets[0], 1, "SOL-PERP", true);

  const auto& btc = config.markets[1];
  assert(btc.market_id == 2);
  assert(btc.market_name == "BTC-PERP");
  assert(btc.symbol_config.symbolId == 2);
  assert(btc.symbol_config.tickSize.ticks() == 10);
  assert(btc.symbol_config.lotSize.lots() == 1);
  assert(btc.symbol_config.minQuantity.lots() == 1);
  assert(btc.symbol_config.maxQuantity.lots() == 500);
  assert(btc.symbol_config.minPrice.ticks() == 100);
  assert(btc.symbol_config.maxPrice.ticks() == 10000000);
  assert(btc.symbol_config.ringCapacityTicks == 4096);
  assert(btc.symbol_config.thresholdPercentage == 20);
  assert(btc.symbol_config.initialBaseTick == 100);
  assert(btc.symbol_config.priceScale == 1);
  assert(btc.symbol_config.quantityScale == 4);
  assert(btc.symbol_config.makerFeeRate == 7);
  assert(btc.symbol_config.takerFeeRate == 9);
  assert(!btc.symbol_config.tradingEnabled);
}

void test_invalid_markets_config_file_fails_clearly() {
  const auto path = write_config_file(
      "invalid.markets.conf",
      "[[market]]\n"
      "market_id = 1\n"
      "market_name = BROKEN-PERP\n");

  try {
    (void)parse_engine_app_config({"--markets-config", path.string()});
    assert(false);
  } catch (const std::invalid_argument& error) {
    assert_contains(error.what(), "missing required key tick_size");
  }
}

void test_poll_limit_option() {
  const std::vector<std::string> args{"--poll-limit", "3"};
  const auto config = parse_engine_app_config(args).config;

  assert(config.poll_loop_limit == 3);
  assert(!config.once);
}

void test_help_is_reported() {
  const auto parsed = parse_engine_app_config({"--help"});
  assert(parsed.help_requested);

  const auto usage = engine_app_usage("engine_app");
  assert_contains(usage, "--bootstrap-servers");
  assert_contains(usage, "--checkpoint-store");
  assert_contains(usage, "default s3");
  assert_contains(usage, "--checkpoint-s3-endpoint");
  assert_contains(usage, "--markets-config");
  assert_contains(usage, "CEX_ENGINE_CHECKPOINT_STORE");
  assert_contains(usage, "CEX_ENGINE_CHECKPOINT_DIR");
  assert_contains(usage, "CEX_ENGINE_CHECKPOINT_S3_BUCKET");
  assert_contains(usage, "CEX_ENGINE_MARKETS_CONFIG");
}

void test_invalid_options_fail_clearly() {
  try {
    (void)parse_engine_app_config({"--poll-limit", "0"});
    assert(false);
  } catch (const std::invalid_argument& error) {
    assert_contains(error.what(), "positive integer");
  }

  try {
    (void)parse_engine_app_config({"--bootstrap-servers"});
    assert(false);
  } catch (const std::invalid_argument& error) {
    assert_contains(error.what(), "requires a value");
  }

  try {
    (void)parse_engine_app_config({"--unknown"});
    assert(false);
  } catch (const std::invalid_argument& error) {
    assert_contains(error.what(), "unknown");
  }

  try {
    (void)parse_engine_app_config({"--checkpoint-store", "disk"});
    assert(false);
  } catch (const std::invalid_argument& error) {
    assert_contains(error.what(), "s3 or file");
  }
}

}  // namespace

int main() {
  try {
    test_defaults_are_minio_friendly();
    test_file_checkpoint_store_is_opt_in_fallback();
    test_environment_overrides_defaults();
    test_environment_markets_config_overrides_default_market();
    test_command_line_overrides_environment();
    test_command_line_markets_config_overrides_environment();
    test_valid_multi_market_file();
    test_invalid_markets_config_file_fails_clearly();
    test_poll_limit_option();
    test_help_is_reported();
    test_invalid_options_fail_clearly();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
