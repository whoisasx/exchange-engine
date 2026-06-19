#include "protocol/Message.hpp"

#include <utility>

namespace protocol {

ProtocolError::ProtocolError(std::string message)
    : std::runtime_error(std::move(message)) {}

ProtocolMessage parse_protocol_message(std::string_view text) {
  JsonValue root = parse_json(text);
  const auto* root_object = root.as_object();
  if (root_object == nullptr) {
    throw ProtocolError("protocol message root must be a JSON object");
  }

  const JsonValue* type_value = root.find("type");
  if (type_value == nullptr || !type_value->is_string()) {
    throw ProtocolError("protocol message must contain top-level string field 'type'");
  }

  const auto* type = type_value->as_string();
  if (type->empty()) {
    throw ProtocolError("protocol message type must not be empty");
  }

  const JsonValue* payload = root.find("payload");
  if (payload == nullptr || !payload->is_object()) {
    throw ProtocolError(
        "protocol message must contain top-level object field 'payload'");
  }

  return ProtocolMessage{*type, *payload};
}

std::optional<MessageClass> classify_engine_fixture_path(
    const std::filesystem::path& path) {
  const std::string filename = path.filename().string();
  if (filename.ends_with(".command.json") ||
      filename.ends_with(".input.json")) {
    return MessageClass::Input;
  }
  if (filename.ends_with(".reply.json")) {
    return MessageClass::Reply;
  }
  if (filename.ends_with(".event.json")) {
    return MessageClass::Event;
  }
  return std::nullopt;
}

std::string_view to_string(MessageClass message_class) noexcept {
  switch (message_class) {
    case MessageClass::Input:
      return "input";
    case MessageClass::Reply:
      return "reply";
    case MessageClass::Event:
      return "event";
  }
  return "unknown";
}

}  // namespace protocol
