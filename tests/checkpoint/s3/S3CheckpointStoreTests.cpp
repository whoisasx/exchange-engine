#include "checkpoint/s3/S3CheckpointStore.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <utility>

using namespace cex::checkpoint;

namespace {

void assert_contains(const std::string& value, const std::string& token) {
  assert(value.find(token) != std::string::npos);
}

std::optional<std::string> read_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return std::nullopt;
  }
  return std::string(value);
}

bool integration_enabled() {
  const auto enabled = read_env("CEX_ENGINE_S3_CHECKPOINT_TEST");
  return enabled.has_value() && *enabled == "1";
}

EngineCheckpoint make_checkpoint(std::string checkpoint_id,
                                 std::int64_t next_offset) {
  EngineCheckpoint checkpoint;
  checkpoint.checkpoint_id = std::move(checkpoint_id);
  checkpoint.core_snapshot.sequenceState = SequenceState{
      .nextSequence = 7,
      .nextTradeId = 11,
      .nextEventId = 13,
  };
  checkpoint.source_position = CheckpointSourcePosition{
      .topic = cex::runtime::EngineInputTopic,
      .partition = 0,
      .next_offset = next_offset,
  };
  checkpoint.public_sequences.emplace(1, 7);
  return checkpoint;
}

void test_defaults_match_local_minio() {
  S3CheckpointStore store(S3CheckpointStoreConfig{});

  assert(store.config().endpoint == "http://127.0.0.1:59000");
  assert(store.config().bucket == "exchange-checkpoints");
  assert(store.config().access_key == "minioadmin");
  assert(store.config().secret_key == "minioadmin");
  assert(store.config().region == "us-east-1");
  assert(store.config().prefix.empty());
  assert(store.checkpoint_key_for_id("checkpoint-0001") ==
         "checkpoint-0001.checkpoint");
}

void test_prefix_and_endpoint_are_normalized() {
  S3CheckpointStore store(S3CheckpointStoreConfig{
      .endpoint = "http://127.0.0.1:59000/",
      .prefix = "/engine-a",
  });

  assert(store.config().endpoint == "http://127.0.0.1:59000");
  assert(store.config().prefix == "engine-a/");
  assert(store.checkpoint_key_for_id("checkpoint-0001") ==
         "engine-a/checkpoint-0001.checkpoint");
}

void test_invalid_config_fails_clearly() {
  try {
    (void)S3CheckpointStore(S3CheckpointStoreConfig{.endpoint = "127.0.0.1"});
    assert(false);
  } catch (const std::invalid_argument& error) {
    assert_contains(error.what(), "http:// or https://");
  }

  try {
    (void)S3CheckpointStore(S3CheckpointStoreConfig{.bucket = ""});
    assert(false);
  } catch (const std::invalid_argument& error) {
    assert_contains(error.what(), "bucket");
  }
}

void test_minio_round_trip_when_enabled() {
  if (!integration_enabled()) {
    return;
  }

  const auto nonce =
      std::chrono::steady_clock::now().time_since_epoch().count();
  S3CheckpointStoreConfig config;
  config.endpoint =
      read_env("CEX_ENGINE_CHECKPOINT_S3_ENDPOINT").value_or(config.endpoint);
  config.bucket =
      read_env("CEX_ENGINE_CHECKPOINT_S3_BUCKET").value_or(config.bucket);
  config.access_key = read_env("CEX_ENGINE_CHECKPOINT_S3_ACCESS_KEY")
                          .value_or(config.access_key);
  config.secret_key = read_env("CEX_ENGINE_CHECKPOINT_S3_SECRET_KEY")
                          .value_or(config.secret_key);
  config.region =
      read_env("CEX_ENGINE_CHECKPOINT_S3_REGION").value_or(config.region);
  config.prefix = "s3-checkpoint-store-tests/" + std::to_string(nonce);

  S3CheckpointStore store(std::move(config));
  store.save(make_checkpoint("checkpoint-0001", 1202));
  store.save(make_checkpoint("checkpoint-0002", 1203));

  const auto loaded = store.load_latest();
  assert(loaded.has_value());
  assert(loaded->checkpoint_id == "checkpoint-0002");
  assert(loaded->source_position.next_offset == 1203);
  assert(loaded->public_sequences.at(1) == 7);
}

}  // namespace

int main() {
  try {
    test_defaults_match_local_minio();
    test_prefix_and_endpoint_are_normalized();
    test_invalid_config_fails_clearly();
    test_minio_round_trip_when_enabled();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
