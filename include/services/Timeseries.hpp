#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "services/ProtocolMessage.hpp"

namespace services {

enum class CandleInterval {
  OneMinute,
  FiveMinutes,
  FifteenMinutes,
  OneHour,
  OneDay,
};

struct TradeExecutedDto {
  MarketSequenceKey key;
  std::int64_t engine_timestamp_ms{0};
  std::int64_t price{0};
  std::int64_t quantity{0};
  std::string trade_id;
  std::optional<std::string> execution_reason;
  StreamPosition source_position;
};

enum class TimeseriesConsumeStatus {
  Ignored,
  Duplicate,
  Written,
  Invalid,
};

struct TimeseriesConsumeResult {
  TimeseriesConsumeStatus status{TimeseriesConsumeStatus::Ignored};
  std::optional<MarketSequenceKey> idempotency_key;
  std::string reason;
};

class ITimeseriesStore {
public:
  virtual ~ITimeseriesStore() = default;

  [[nodiscard]] virtual bool has_trade(const MarketSequenceKey& key) const = 0;

  // Real adapters should persist this as timeseries_trades keyed by
  // (market_id, engine_sequence).
  virtual void upsert_trade(const TradeExecutedDto& trade) = 0;

  // Candle writes are idempotent aggregations derived only from TradeExecuted.
  virtual void upsert_candle(CandleInterval interval,
                             const TradeExecutedDto& trade) = 0;

  virtual void commit_offset(const StreamPosition& position) = 0;
};

class TimeseriesService {
public:
  explicit TimeseriesService(ITimeseriesStore& store);

  // Contract: consume engine.events TradeExecuted only. Use
  // (market_id, engine_sequence) for ordering and idempotency because
  // engine_sequence is per market, not global.
  [[nodiscard]] TimeseriesConsumeResult consume(const ProtocolMessage& message);

  [[nodiscard]] static std::optional<TradeExecutedDto> parse_trade_executed(
      const ProtocolMessage& message);
  [[nodiscard]] static std::array<CandleInterval, 5> supported_intervals();
  [[nodiscard]] static std::int64_t interval_width_ms(CandleInterval interval);
  [[nodiscard]] static std::int64_t candle_bucket_start_ms(
      CandleInterval interval,
      std::int64_t engine_timestamp_ms);

private:
  ITimeseriesStore& store_;
};

}  // namespace services
