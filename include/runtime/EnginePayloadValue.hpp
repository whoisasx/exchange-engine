#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace cex::runtime {

struct PayloadNumber {
  std::string text;

  friend bool operator==(const PayloadNumber&,
                         const PayloadNumber&) = default;
};

class PayloadValue {
 public:
  using Array = std::vector<PayloadValue>;
  using Object = std::map<std::string, PayloadValue, std::less<>>;
  using Storage = std::variant<std::nullptr_t,
                               bool,
                               PayloadNumber,
                               std::string,
                               Array,
                               Object>;

  PayloadValue() noexcept : storage_(nullptr) {}
  PayloadValue(std::nullptr_t) noexcept : storage_(nullptr) {}
  PayloadValue(const char* value) : storage_(std::string(value ? value : "")) {}
  PayloadValue(std::string value) : storage_(std::move(value)) {}

  [[nodiscard]] static PayloadValue integer(std::int64_t value) {
    return number_text(std::to_string(value));
  }

  [[nodiscard]] static PayloadValue number_text(std::string value) {
    PayloadValue result;
    result.storage_ = PayloadNumber{.text = std::move(value)};
    return result;
  }

  [[nodiscard]] static PayloadValue boolean(bool value) {
    PayloadValue result;
    result.storage_ = value;
    return result;
  }

  [[nodiscard]] static PayloadValue null() {
    return PayloadValue(nullptr);
  }

  [[nodiscard]] static PayloadValue array(Array values) {
    PayloadValue result;
    result.storage_ = std::move(values);
    return result;
  }

  [[nodiscard]] static PayloadValue object(Object fields) {
    PayloadValue result;
    result.storage_ = std::move(fields);
    return result;
  }

  [[nodiscard]] bool is_null() const noexcept {
    return std::holds_alternative<std::nullptr_t>(storage_);
  }

  [[nodiscard]] const bool* as_bool() const noexcept {
    return std::get_if<bool>(&storage_);
  }

  [[nodiscard]] const PayloadNumber* as_number() const noexcept {
    return std::get_if<PayloadNumber>(&storage_);
  }

  [[nodiscard]] const std::string* as_string() const noexcept {
    return std::get_if<std::string>(&storage_);
  }

  [[nodiscard]] const Array* as_array() const noexcept {
    return std::get_if<Array>(&storage_);
  }

  [[nodiscard]] const Object* as_object() const noexcept {
    return std::get_if<Object>(&storage_);
  }

  [[nodiscard]] const Storage& storage() const noexcept {
    return storage_;
  }

  friend bool operator==(const PayloadValue&,
                         const PayloadValue&) = default;

 private:
  Storage storage_;
};

[[nodiscard]] inline bool payload_value_equals_string(
    const PayloadValue& left,
    std::string_view right) {
  const auto* text = left.as_string();
  return text != nullptr && *text == right;
}

[[nodiscard]] inline bool operator==(const PayloadValue& left,
                                     std::string_view right) {
  return payload_value_equals_string(left, right);
}

[[nodiscard]] inline bool operator==(const PayloadValue& left,
                                     const char* right) {
  return payload_value_equals_string(left, std::string_view(right));
}

[[nodiscard]] inline bool operator==(const PayloadValue& left,
                                     const std::string& right) {
  return payload_value_equals_string(left, std::string_view(right));
}

[[nodiscard]] inline bool operator==(std::string_view left,
                                     const PayloadValue& right) {
  return payload_value_equals_string(right, left);
}

[[nodiscard]] inline bool operator==(const char* left,
                                     const PayloadValue& right) {
  return payload_value_equals_string(right, std::string_view(left));
}

[[nodiscard]] inline bool operator==(const std::string& left,
                                     const PayloadValue& right) {
  return payload_value_equals_string(right, std::string_view(left));
}

}  // namespace cex::runtime
