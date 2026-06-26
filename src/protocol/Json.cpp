#include "protocol/Json.hpp"

#include <cctype>
#include <cstdint>
#include <sstream>
#include <utility>

namespace protocol {

JsonValue::JsonValue() noexcept : storage_(nullptr) {}

JsonValue::JsonValue(std::nullptr_t) noexcept : storage_(nullptr) {}

JsonValue::JsonValue(bool value) : storage_(value) {}

JsonValue::JsonValue(JsonNumber value) : storage_(std::move(value)) {}

JsonValue::JsonValue(std::string value) : storage_(std::move(value)) {}

JsonValue::JsonValue(Array value) : storage_(std::move(value)) {}

JsonValue::JsonValue(Object value) : storage_(std::move(value)) {}

bool JsonValue::is_null() const noexcept {
  return std::holds_alternative<std::nullptr_t>(storage_);
}

bool JsonValue::is_string() const noexcept {
  return std::holds_alternative<std::string>(storage_);
}

bool JsonValue::is_object() const noexcept {
  return std::holds_alternative<Object>(storage_);
}

const bool* JsonValue::as_bool() const noexcept {
  return std::get_if<bool>(&storage_);
}

const JsonNumber* JsonValue::as_number() const noexcept {
  return std::get_if<JsonNumber>(&storage_);
}

const std::string* JsonValue::as_string() const noexcept {
  return std::get_if<std::string>(&storage_);
}

const JsonValue::Array* JsonValue::as_array() const noexcept {
  return std::get_if<Array>(&storage_);
}

const JsonValue::Object* JsonValue::as_object() const noexcept {
  return std::get_if<Object>(&storage_);
}

const JsonValue* JsonValue::find(std::string_view key) const {
  const auto* object = as_object();
  if (object == nullptr) {
    return nullptr;
  }

  const auto found = object->find(key);
  if (found == object->end()) {
    return nullptr;
  }

  return &found->second;
}

JsonParseError::JsonParseError(std::size_t, std::string message)
    : std::runtime_error(std::move(message)) {}

namespace {

class Parser {
 public:
  explicit Parser(std::string_view text) : text_(text) {}

  JsonValue parse() {
    JsonValue value = parse_value();
    skip_whitespace();
    if (!is_at_end()) {
      fail("unexpected trailing content");
    }
    return value;
  }

 private:
  JsonValue parse_value() {
    skip_whitespace();
    if (is_at_end()) {
      fail("expected JSON value");
    }

    const char current = text_[pos_];
    if (current == '{') {
      return JsonValue(parse_object());
    }
    if (current == '[') {
      return JsonValue(parse_array());
    }
    if (current == '"') {
      return JsonValue(parse_string());
    }
    if (current == 't') {
      parse_literal("true");
      return JsonValue(true);
    }
    if (current == 'f') {
      parse_literal("false");
      return JsonValue(false);
    }
    if (current == 'n') {
      parse_literal("null");
      return JsonValue(nullptr);
    }
    if (current == '-' || is_digit(current)) {
      return JsonValue(parse_number());
    }

    fail("expected JSON value");
  }

  JsonValue::Object parse_object() {
    expect('{');
    skip_whitespace();

    JsonValue::Object object;
    if (consume('}')) {
      return object;
    }

    while (true) {
      skip_whitespace();
      if (peek() != '"') {
        fail("expected object key string");
      }

      std::string key = parse_string();
      skip_whitespace();
      expect(':');
      JsonValue value = parse_value();
      const auto [_, inserted] = object.emplace(std::move(key), std::move(value));
      if (!inserted) {
        fail("duplicate object key");
      }

      skip_whitespace();
      if (consume('}')) {
        return object;
      }
      expect(',');
    }
  }

  JsonValue::Array parse_array() {
    expect('[');
    skip_whitespace();

    JsonValue::Array array;
    if (consume(']')) {
      return array;
    }

    while (true) {
      array.push_back(parse_value());
      skip_whitespace();
      if (consume(']')) {
        return array;
      }
      expect(',');
    }
  }

  std::string parse_string() {
    expect('"');

    std::string result;
    while (!is_at_end()) {
      const unsigned char current = static_cast<unsigned char>(text_[pos_++]);
      if (current == '"') {
        return result;
      }
      if (current == '\\') {
        parse_escape(result);
        continue;
      }
      if (current < 0x20U) {
        fail("unescaped control character in string");
      }
      result.push_back(static_cast<char>(current));
    }

    fail("unterminated string");
  }

  void parse_escape(std::string& result) {
    if (is_at_end()) {
      fail("unterminated escape sequence");
    }

    const char escaped = text_[pos_++];
    switch (escaped) {
      case '"':
      case '\\':
      case '/':
        result.push_back(escaped);
        return;
      case 'b':
        result.push_back('\b');
        return;
      case 'f':
        result.push_back('\f');
        return;
      case 'n':
        result.push_back('\n');
        return;
      case 'r':
        result.push_back('\r');
        return;
      case 't':
        result.push_back('\t');
        return;
      case 'u':
        append_unicode_escape(result);
        return;
      default:
        fail("invalid escape sequence");
    }
  }

  void append_unicode_escape(std::string& result) {
    const auto first = parse_hex_code_unit();
    std::uint32_t codepoint = first;

    if (is_high_surrogate(first)) {
      if (!consume('\\') || !consume('u')) {
        fail("expected low surrogate escape");
      }

      const auto second = parse_hex_code_unit();
      if (!is_low_surrogate(second)) {
        fail("expected low surrogate");
      }

      codepoint = 0x10000U +
                  (((static_cast<std::uint32_t>(first) - 0xD800U) << 10U) |
                   (static_cast<std::uint32_t>(second) - 0xDC00U));
    } else if (is_low_surrogate(first)) {
      fail("unexpected low surrogate");
    }

    append_utf8(codepoint, result);
  }

  std::uint16_t parse_hex_code_unit() {
    if (pos_ + 4 > text_.size()) {
      fail("incomplete unicode escape");
    }

    std::uint16_t value = 0;
    for (int i = 0; i < 4; ++i) {
      const char current = text_[pos_++];
      value = static_cast<std::uint16_t>((value << 4U) | hex_value(current));
    }
    return value;
  }

  static bool is_high_surrogate(std::uint16_t value) noexcept {
    return value >= 0xD800U && value <= 0xDBFFU;
  }

  static bool is_low_surrogate(std::uint16_t value) noexcept {
    return value >= 0xDC00U && value <= 0xDFFFU;
  }

  static void append_utf8(std::uint32_t codepoint, std::string& output) {
    if (codepoint <= 0x7FU) {
      output.push_back(static_cast<char>(codepoint));
      return;
    }
    if (codepoint <= 0x7FFU) {
      output.push_back(static_cast<char>(0xC0U | (codepoint >> 6U)));
      output.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
      return;
    }
    if (codepoint <= 0xFFFFU) {
      output.push_back(static_cast<char>(0xE0U | (codepoint >> 12U)));
      output.push_back(
          static_cast<char>(0x80U | ((codepoint >> 6U) & 0x3FU)));
      output.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
      return;
    }

    output.push_back(static_cast<char>(0xF0U | (codepoint >> 18U)));
    output.push_back(static_cast<char>(0x80U | ((codepoint >> 12U) & 0x3FU)));
    output.push_back(static_cast<char>(0x80U | ((codepoint >> 6U) & 0x3FU)));
    output.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
  }

  JsonNumber parse_number() {
    const auto start = pos_;
    consume('-');

    if (is_at_end()) {
      fail("incomplete number");
    }

    if (consume('0')) {
      if (!is_at_end() && is_digit(peek())) {
        fail("leading zero in number");
      }
    } else if (is_non_zero_digit(peek())) {
      consume_digits();
    } else {
      fail("expected number digits");
    }

    if (consume('.')) {
      if (is_at_end() || !is_digit(peek())) {
        fail("expected fractional digits");
      }
      consume_digits();
    }

    if (!is_at_end() && (peek() == 'e' || peek() == 'E')) {
      ++pos_;
      if (!is_at_end() && (peek() == '+' || peek() == '-')) {
        ++pos_;
      }
      if (is_at_end() || !is_digit(peek())) {
        fail("expected exponent digits");
      }
      consume_digits();
    }

    return JsonNumber{std::string(text_.substr(start, pos_ - start))};
  }

  void consume_digits() {
    while (!is_at_end() && is_digit(peek())) {
      ++pos_;
    }
  }

  void parse_literal(std::string_view literal) {
    if (text_.substr(pos_, literal.size()) != literal) {
      fail("invalid literal");
    }
    pos_ += literal.size();
  }

  void skip_whitespace() {
    while (!is_at_end()) {
      const char current = text_[pos_];
      if (current != ' ' && current != '\t' && current != '\n' &&
          current != '\r') {
        return;
      }
      ++pos_;
    }
  }

  char peek() const {
    if (is_at_end()) {
      return '\0';
    }
    return text_[pos_];
  }

  bool consume(char expected) {
    if (!is_at_end() && text_[pos_] == expected) {
      ++pos_;
      return true;
    }
    return false;
  }

  void expect(char expected) {
    if (!consume(expected)) {
      std::ostringstream message;
      message << "expected '" << expected << "'";
      fail(message.str());
    }
  }

  [[nodiscard]] bool is_at_end() const noexcept {
    return pos_ >= text_.size();
  }

  [[noreturn]] void fail(std::string message) const {
    throw JsonParseError(pos_, std::move(message));
  }

  static bool is_digit(char value) noexcept {
    return value >= '0' && value <= '9';
  }

  static bool is_non_zero_digit(char value) noexcept {
    return value >= '1' && value <= '9';
  }

  std::uint16_t hex_value(char value) const {
    if (value >= '0' && value <= '9') {
      return static_cast<std::uint16_t>(value - '0');
    }
    if (value >= 'a' && value <= 'f') {
      return static_cast<std::uint16_t>(10 + value - 'a');
    }
    if (value >= 'A' && value <= 'F') {
      return static_cast<std::uint16_t>(10 + value - 'A');
    }
    fail("invalid unicode escape");
  }

  std::string_view text_;
  std::size_t pos_ = 0;
};

}  // namespace

JsonValue parse_json(std::string_view text) {
  return Parser(text).parse();
}

}  // namespace protocol
