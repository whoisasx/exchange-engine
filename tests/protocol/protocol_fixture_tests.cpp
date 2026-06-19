#include "protocol/Message.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#ifndef PROTOCOL_EXAMPLES_DIR
#error "PROTOCOL_EXAMPLES_DIR must be defined"
#endif

namespace {

std::string read_file(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("failed to open " + path.string());
  }

  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

void require(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::vector<std::filesystem::path> list_json_fixtures(
    const std::filesystem::path& directory) {
  std::vector<std::filesystem::path> fixtures;
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (entry.is_regular_file() && entry.path().extension() == ".json") {
      fixtures.push_back(entry.path());
    }
  }

  std::sort(fixtures.begin(), fixtures.end());
  return fixtures;
}

void test_fixture_contracts() {
  const std::filesystem::path examples_dir = PROTOCOL_EXAMPLES_DIR;
  require(std::filesystem::is_directory(examples_dir),
          "examples directory does not exist: " + examples_dir.string());

  const auto fixtures = list_json_fixtures(examples_dir);
  require(!fixtures.empty(), "no JSON fixtures found in docs/examples");

  std::map<protocol::MessageClass, int> counts;
  for (const auto& fixture : fixtures) {
    const std::string body = read_file(fixture);
    const protocol::ProtocolMessage message =
        protocol::parse_protocol_message(body);

    require(!message.type.empty(), fixture.string() + " has an empty type");
    require(message.payload.is_object(),
            fixture.string() + " payload is not an object");

    const auto classification = protocol::classify_engine_fixture_path(fixture);
    require(classification.has_value(),
            fixture.string() + " does not have a known fixture suffix");
    ++counts[*classification];
  }

  require(counts[protocol::MessageClass::Input] > 0,
          "expected at least one input fixture");
  require(counts[protocol::MessageClass::Reply] > 0,
          "expected at least one reply fixture");
  require(counts[protocol::MessageClass::Event] > 0,
          "expected at least one event fixture");
  require(counts[protocol::MessageClass::Input] == 7,
          "expected 7 input fixtures");
  require(counts[protocol::MessageClass::Reply] == 6,
          "expected 6 reply fixtures");
  require(counts[protocol::MessageClass::Event] == 20,
          "expected 20 event fixtures");
}

void test_fixture_classification() {
  require(protocol::classify_engine_fixture_path("engine-place-order.command.json") ==
              protocol::MessageClass::Input,
          "command fixtures must classify as input");
  require(protocol::classify_engine_fixture_path("engine-mark-price-updated.input.json") ==
              protocol::MessageClass::Input,
          "input fixtures must classify as input");
  require(protocol::classify_engine_fixture_path("engine-order-accepted.reply.json") ==
              protocol::MessageClass::Reply,
          "reply fixtures must classify as reply");
  require(protocol::classify_engine_fixture_path("engine-order-opened.event.json") ==
              protocol::MessageClass::Event,
          "event fixtures must classify as event");
  require(!protocol::classify_engine_fixture_path("README.md").has_value(),
          "non-fixtures must not classify as protocol messages");
}

void test_protocol_validation_errors() {
  try {
    static_cast<void>(protocol::parse_protocol_message("[]"));
    require(false, "array root should not parse as a protocol message");
  } catch (const protocol::ProtocolError&) {
  }

  try {
    static_cast<void>(protocol::parse_protocol_message(R"({"payload":{}})"));
    require(false, "missing type should not parse as a protocol message");
  } catch (const protocol::ProtocolError&) {
  }

  try {
    static_cast<void>(protocol::parse_protocol_message(
        R"({"type":"OrderAccepted","payload":[]})"));
    require(false, "array payload should not parse as a protocol message");
  } catch (const protocol::ProtocolError&) {
  }

  try {
    static_cast<void>(protocol::parse_protocol_message(R"({"type":)"));
    require(false, "invalid JSON should not parse as a protocol message");
  } catch (const protocol::JsonParseError&) {
  }
}

}  // namespace

int main() {
  try {
    test_fixture_classification();
    test_protocol_validation_errors();
    test_fixture_contracts();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
