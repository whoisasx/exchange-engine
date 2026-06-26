#include"FixedPoint.hpp"

#include <limits>
#include <stdexcept>
#include<string_view>

namespace {
[[nodiscard]] std::string_view trim_ascii_space(std::string_view value) {
  while (!value.empty() && (value.front() == ' ' || value.front() == '\t' ||
                            value.front() == '\n' || value.front() == '\r')) {
    value.remove_prefix(1);
  }
  while (!value.empty() && (value.back() == ' ' || value.back() == '\t' ||
                            value.back() == '\n' || value.back() == '\r')) {
    value.remove_suffix(1);
  }
  return value;
}

void append_digit(unsigned __int128& value, unsigned digit) {
  value = (value * 10) + digit;
}

[[nodiscard]] unsigned __int128 parse_scaled_magnitude(std::string_view input, int scale, bool allowNegative, bool& negative) {
  if (scale < 0) {
    throw std::invalid_argument("fixed-point scale must be non-negative");
  }

  auto value = trim_ascii_space(input);
  if (value.empty()) {
    throw std::invalid_argument("fixed-point value is empty");
  }

  negative = false;
  if (value.front() == '+' || value.front() == '-') {
    negative = value.front() == '-';
    value.remove_prefix(1);
  }
  if (negative && !allowNegative) {
    throw std::invalid_argument("negative quantity is not allowed");
  }
  if (value.empty()) {
    throw std::invalid_argument("fixed-point value has no digits");
  }

  unsigned __int128 result = 0;
  bool seenDigit = false;
  bool seenDecimalPoint = false;
  int fractionalDigits = 0;

  for (char ch : value) {
    if (ch == '.') {
      if (seenDecimalPoint) {
        throw std::invalid_argument("fixed-point value has multiple decimal points");
      }
      seenDecimalPoint = true;
      continue;
    }

    if (ch < '0' || ch > '9') {
      throw std::invalid_argument("fixed-point value contains a non-digit");
    }

    seenDigit = true;
    const auto digit = static_cast<unsigned>(ch - '0');
    if (seenDecimalPoint) {
      if (fractionalDigits >= scale) {
        if (digit != 0) {
          throw std::invalid_argument("fixed-point value has too much precision");
        }
        continue;
      }
      ++fractionalDigits;
    }
    append_digit(result, digit);
  }

  if (!seenDigit) {
    throw std::invalid_argument("fixed-point value has no digits");
  }

  while (fractionalDigits < scale) {
    append_digit(result, 0);
    ++fractionalDigits;
  }

  return result;
}

}

Price price(std::string_view value, int scale){
  bool negative = false;
  const auto magnitude = parse_scaled_magnitude(value, scale, true, negative);
  const auto maxValue = static_cast<unsigned __int128>(std::numeric_limits<int64_t>::max());
  if (magnitude > maxValue) {
    throw std::out_of_range("price is outside int64 range");
  }

  const auto ticks = static_cast<int64_t>(magnitude);
  return Price::from_ticks(negative ? -ticks : ticks);
}
