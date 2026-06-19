#include "services/Timeseries.hpp"

namespace services {

TimeseriesService::TimeseriesService(ITimeseriesStore& store) : store_(store) {}

TimeseriesConsumeResult TimeseriesService::consume(
    const ProtocolMessage& message) {
  if (message.topic != EngineEventsTopic || message.type != "TradeExecuted") {
    return TimeseriesConsumeResult{TimeseriesConsumeStatus::Ignored,
                                   std::nullopt,
                                   {}};
  }

  const auto trade = parse_trade_executed(message);
  if (!trade.has_value()) {
    return TimeseriesConsumeResult{TimeseriesConsumeStatus::Invalid,
                                   std::nullopt,
                                   "TradeExecuted requires market_id, "
                                   "engine_sequence, engine_timestamp_ms, "
                                   "price, and quantity"};
  }

  if (store_.has_trade(trade->key)) {
    return TimeseriesConsumeResult{TimeseriesConsumeStatus::Duplicate,
                                   trade->key,
                                   {}};
  }

  store_.upsert_trade(*trade);
  for (const auto interval : supported_intervals()) {
    store_.upsert_candle(interval, *trade);
  }
  store_.commit_offset(message.position());

  return TimeseriesConsumeResult{TimeseriesConsumeStatus::Written,
                                 trade->key,
                                 {}};
}

std::optional<TradeExecutedDto> TimeseriesService::parse_trade_executed(
    const ProtocolMessage& message) {
  if (message.topic != EngineEventsTopic || message.type != "TradeExecuted") {
    return std::nullopt;
  }

  const auto market_id = payload_u64(message, "market_id");
  const auto engine_sequence = payload_u64(message, "engine_sequence");
  const auto engine_timestamp_ms = payload_i64(message, "engine_timestamp_ms");
  const auto price = payload_i64(message, "price");
  const auto quantity = payload_i64(message, "quantity");
  if (!market_id.has_value() || !engine_sequence.has_value() ||
      !engine_timestamp_ms.has_value() || !price.has_value() ||
      !quantity.has_value()) {
    return std::nullopt;
  }

  return TradeExecutedDto{
      MarketSequenceKey{*market_id, *engine_sequence},
      *engine_timestamp_ms,
      *price,
      *quantity,
      payload_string(message, "trade_id").value_or(""),
      payload_string(message, "execution_reason"),
      message.position(),
  };
}

std::array<CandleInterval, 5> TimeseriesService::supported_intervals() {
  return {CandleInterval::OneMinute,
          CandleInterval::FiveMinutes,
          CandleInterval::FifteenMinutes,
          CandleInterval::OneHour,
          CandleInterval::OneDay};
}

std::int64_t TimeseriesService::interval_width_ms(CandleInterval interval) {
  switch (interval) {
    case CandleInterval::OneMinute:
      return 60'000;
    case CandleInterval::FiveMinutes:
      return 5 * 60'000;
    case CandleInterval::FifteenMinutes:
      return 15 * 60'000;
    case CandleInterval::OneHour:
      return 60 * 60'000;
    case CandleInterval::OneDay:
      return 24 * 60 * 60'000;
  }
  return 60'000;
}

std::int64_t TimeseriesService::candle_bucket_start_ms(
    CandleInterval interval,
    std::int64_t engine_timestamp_ms) {
  const auto width = interval_width_ms(interval);
  return (engine_timestamp_ms / width) * width;
}

}  // namespace services
