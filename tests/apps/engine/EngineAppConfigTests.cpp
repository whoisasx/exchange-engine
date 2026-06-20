#include "apps/engine/EngineAppConfig.hpp"

#include <cassert>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace cex::apps::engine;

namespace {

void assert_contains(const std::string& value, const std::string& token) {
  assert(value.find(token) != std::string::npos);
}

void test_defaults_are_local_friendly() {
  const auto parsed = parse_engine_app_config({});
  const auto& config = parsed.config;

  assert(!parsed.help_requested);
  assert(config.bootstrap_servers == "127.0.0.1:9092");
  assert(config.consumer_group_id == "cex-engine");
  assert(config.input_topic == cex::runtime::EngineInputTopic);
  assert(config.replies_topic == cex::runtime::EngineRepliesTopic);
  assert(config.events_topic == cex::runtime::EngineEventsTopic);
  assert(config.checkpoint_directory == ".data/engine/checkpoints");
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

void test_environment_overrides_defaults() {
  const EngineAppEnvironment environment{
      .bootstrap_servers = "redpanda:9092",
      .consumer_group_id = "local-engine",
      .checkpoint_directory = "tmp/checkpoints",
      .poll_loop_limit = "7",
  };

  const auto config = parse_engine_app_config({}, environment).config;

  assert(config.bootstrap_servers == "redpanda:9092");
  assert(config.consumer_group_id == "local-engine");
  assert(config.checkpoint_directory == "tmp/checkpoints");
  assert(config.poll_loop_limit == 7);
  assert(!config.once);
}

void test_command_line_overrides_environment() {
  const EngineAppEnvironment environment{
      .bootstrap_servers = "env:9092",
      .consumer_group_id = "env-group",
      .checkpoint_directory = "env/checkpoints",
      .poll_loop_limit = "9",
  };
  const std::vector<std::string> args{
      "--bootstrap-servers=cli:9092",
      "--group-id",
      "cli-group",
      "--checkpoint-dir=cli/checkpoints",
      "--once",
  };

  const auto config = parse_engine_app_config(args, environment).config;

  assert(config.bootstrap_servers == "cli:9092");
  assert(config.consumer_group_id == "cli-group");
  assert(config.checkpoint_directory == "cli/checkpoints");
  assert(config.poll_loop_limit == 1);
  assert(config.once);
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
  assert_contains(usage, "CEX_ENGINE_CHECKPOINT_DIR");
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
}

}  // namespace

int main() {
  try {
    test_defaults_are_local_friendly();
    test_environment_overrides_defaults();
    test_command_line_overrides_environment();
    test_poll_limit_option();
    test_help_is_reported();
    test_invalid_options_fail_clearly();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
