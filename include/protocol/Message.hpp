#pragma once

#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "protocol/Json.hpp"

namespace protocol {

enum class MessageClass {
  Input,
  Reply,
  Event,
};

struct ProtocolMessage {
  std::string type;
  JsonValue payload;
};

class ProtocolError : public std::runtime_error {
 public:
  explicit ProtocolError(std::string message);
};

ProtocolMessage parse_protocol_message(std::string_view text);
std::optional<MessageClass> classify_engine_fixture_path(
    const std::filesystem::path& path);
std::string_view to_string(MessageClass message_class) noexcept;

}  // namespace protocol
