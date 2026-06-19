#pragma once

#include <cstddef>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace protocol {

struct JsonNumber {
  std::string text;
};

class JsonValue {
 public:
  using Array = std::vector<JsonValue>;
  using Object = std::map<std::string, JsonValue, std::less<>>;
  using Storage =
      std::variant<std::nullptr_t, bool, JsonNumber, std::string, Array, Object>;

  JsonValue() noexcept;
  explicit JsonValue(std::nullptr_t) noexcept;
  explicit JsonValue(bool value);
  explicit JsonValue(JsonNumber value);
  explicit JsonValue(std::string value);
  explicit JsonValue(Array value);
  explicit JsonValue(Object value);

  [[nodiscard]] bool is_null() const noexcept;
  [[nodiscard]] bool is_bool() const noexcept;
  [[nodiscard]] bool is_number() const noexcept;
  [[nodiscard]] bool is_string() const noexcept;
  [[nodiscard]] bool is_array() const noexcept;
  [[nodiscard]] bool is_object() const noexcept;

  [[nodiscard]] const bool* as_bool() const noexcept;
  [[nodiscard]] const JsonNumber* as_number() const noexcept;
  [[nodiscard]] const std::string* as_string() const noexcept;
  [[nodiscard]] const Array* as_array() const noexcept;
  [[nodiscard]] const Object* as_object() const noexcept;
  [[nodiscard]] const JsonValue* find(std::string_view key) const;

  [[nodiscard]] const Storage& storage() const noexcept;

 private:
  Storage storage_;
};

class JsonParseError : public std::runtime_error {
 public:
  JsonParseError(std::size_t offset, std::string message);

  [[nodiscard]] std::size_t offset() const noexcept;

 private:
  std::size_t offset_;
};

JsonValue parse_json(std::string_view text);

}  // namespace protocol
