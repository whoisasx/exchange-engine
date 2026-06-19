#pragma once
#include<cassert>
#include<cstdint>
#include<limits>
#include<compare>
#include<string>
#include<string_view>

class Price{
  int64_t raw_ticks;
  explicit constexpr Price(int64_t ticks): raw_ticks(ticks){}

public:
  constexpr Price(): raw_ticks(0){}

  static constexpr Price from_ticks(int64_t ticks){
    return Price(ticks);
  }
  [[nodiscard]] constexpr int64_t ticks() const {
    return raw_ticks;
  }
  constexpr Price& add_ticks(int64_t ticks){
    raw_ticks+=ticks;
    return *this;
  }
  constexpr Price& subtract_ticks(int64_t ticks){
    raw_ticks-=ticks;
    return *this;
  }

  [[nodiscard]] constexpr int64_t operator-(const Price& other) const {
    return raw_ticks-other.raw_ticks;
  }
  [[nodiscard]] auto operator<=>(const Price&) const=default;
  [[nodiscard]] constexpr bool is_valid() const { 
    return raw_ticks>0;
  }

  static constexpr Price max(){
    return Price(std::numeric_limits<int64_t>::max());
  }
  static constexpr Price min(){
    return Price(std::numeric_limits<int64_t>::min());
  }
};

class Quantity{
  uint64_t raw_lots;
  explicit constexpr Quantity(uint64_t lots): raw_lots(lots){}

public:
  constexpr Quantity(): raw_lots(0){}

  static constexpr Quantity from_lots(uint64_t lots){
    return Quantity(lots);
  }

  [[nodiscard]] constexpr uint64_t lots() const {
    return raw_lots;
  }

  [[nodiscard]] constexpr bool is_zero() const {
    return raw_lots==0;
  }

  [[nodiscard]] constexpr bool is_valid() const {
    return raw_lots>0;
  }

  constexpr Quantity& add_lots(uint64_t lots){
    raw_lots+=lots;
    return *this;
  }

  constexpr Quantity& subtract_lots(uint64_t lots){
    raw_lots-=lots;
    return *this;
  }

  [[nodiscard]] static constexpr Quantity min(Quantity a, Quantity b){
    return Quantity(a.raw_lots<b.raw_lots ? a.raw_lots : b.raw_lots);
  }

  [[nodiscard]] auto operator<=>(const Quantity&) const=default;
};


class Notional{
  int64_t raw_value;
  explicit constexpr Notional(uint64_t value): raw_value(value){}
public:
  static constexpr Notional from_raw(uint64_t value){
    return Notional(value);
  }

  static constexpr Notional from_price_quantity(Price price, Quantity quantity){

    //add a check value<=uint64_t maxvalue
    const auto value=static_cast<unsigned __int128>(price.ticks())*static_cast<unsigned __int128>(quantity.lots());

    return Notional(static_cast<uint64_t>(value));
  }

  [[nodiscard]] constexpr int64_t raw() const {
    return raw_value;
  }
};

Price price(std::string_view value, int scale);
std::string price_to_string(Price price, int scale);

Quantity quantity_from_decimal_string(std::string_view value, int scale);
std::string quantity_to_string(Quantity quantity, int scale);
