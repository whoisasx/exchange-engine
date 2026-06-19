#include "services/FakeEngineSmoke.hpp"
#include "services/Ledger.hpp"
#include "services/OrderbookProjector.hpp"
#include "services/Timeseries.hpp"
#include "services/WebsocketFanout.hpp"

#include <cassert>
#include <optional>
#include <unordered_set>
#include <vector>

using namespace services;

namespace {

struct CapturingLedgerStore final : ILedgerStore {
  std::unordered_set<StreamPosition, StreamPositionHash> seen;
  std::vector<ProtocolMessage> journals;
  std::vector<LedgerEntry> entries;

  [[nodiscard]] bool already_recorded(
      const StreamPosition& position) const override {
    return seen.contains(position);
  }

  void append_journal_and_entries(
      const ProtocolMessage& wallet_event,
      const std::vector<LedgerEntry>& new_entries) override {
    seen.insert(wallet_event.position());
    journals.push_back(wallet_event);
    entries.insert(entries.end(), new_entries.begin(), new_entries.end());
  }
};

struct CapturingTimeseriesStore final : ITimeseriesStore {
  std::unordered_set<MarketSequenceKey, MarketSequenceKeyHash> trades_seen;
  std::vector<TradeExecutedDto> trades;
  std::vector<CandleInterval> candle_writes;
  std::vector<StreamPosition> offsets;

  [[nodiscard]] bool has_trade(const MarketSequenceKey& key) const override {
    return trades_seen.contains(key);
  }

  void upsert_trade(const TradeExecutedDto& trade) override {
    trades_seen.insert(trade.key);
    trades.push_back(trade);
  }

  void upsert_candle(CandleInterval interval,
                     const TradeExecutedDto& /*trade*/) override {
    candle_writes.push_back(interval);
  }

  void commit_offset(const StreamPosition& position) override {
    offsets.push_back(position);
  }
};

struct CapturingOrderbookStore final : IOrderbookStateStore {
  std::unordered_set<MarketSequenceKey, MarketSequenceKeyHash> deltas_seen;
  std::vector<OrderBookDeltaDto> deltas;
  std::vector<StreamPosition> offsets;

  [[nodiscard]] bool has_applied_delta(
      const MarketSequenceKey& key) const override {
    return deltas_seen.contains(key);
  }

  void apply_delta(const OrderBookDeltaDto& delta) override {
    deltas_seen.insert(delta.key);
    deltas.push_back(delta);
  }

  void commit_offset(const StreamPosition& position) override {
    offsets.push_back(position);
  }
};

struct CapturingWebsocketSink final : IWebsocketSink {
  std::vector<FanoutEvent> events;

  void publish(const FanoutEvent& event) override {
    events.push_back(event);
  }
};

void ledger_consumes_wallet_events_only() {
  CapturingLedgerStore store;
  LedgerService ledger(store);

  const ProtocolMessage engine_event{
      EngineEventsTopic,
      "TradeExecuted",
      {{"market_id", "1"}, {"engine_sequence", "10"}},
      std::nullopt,
      0,
      1,
  };
  const auto ignored = ledger.consume(engine_event);
  assert(ignored.status == LedgerConsumeStatus::Ignored);
  assert(store.journals.empty());

  const ProtocolMessage deposit{
      WalletEventsTopic,
      "DepositApplied",
      {{"request_id", "req-1"},
       {"user_id", "42"},
       {"asset", "USD"},
       {"amount", "1000"},
       {"reference_id", "dep-1"}},
      std::nullopt,
      2,
      50,
  };
  const auto written = ledger.consume(deposit);
  assert(written.status == LedgerConsumeStatus::Written);
  assert(store.journals.size() == 1);
  assert(store.entries.size() == 1);
  assert(store.entries[0].kind == "DEPOSIT");
  assert(store.entries[0].user_id == 42);
  assert(store.entries[0].total_delta == 1000);
  assert(store.entries[0].locked_delta == 0);
  assert(store.entries[0].reference_id == "dep-1");

  const auto duplicate = ledger.consume(deposit);
  assert(duplicate.status == LedgerConsumeStatus::Duplicate);

  const ProtocolMessage account_delta{
      WalletEventsTopic,
      "AccountDeltaApplied",
      {{"user_id", "42"},
       {"asset", "USD"},
       {"total_delta", "-7"},
       {"locked_delta", "0"},
       {"kind", "FEE_CHARGED"},
       {"reference_id", "fee-1"}},
      std::nullopt,
      2,
      51,
  };
  const auto fee = ledger.consume(account_delta);
  assert(fee.status == LedgerConsumeStatus::Written);
  assert(store.entries.back().kind == "FEE_CHARGED");
  assert(store.entries.back().total_delta == -7);
}

void timeseries_uses_market_sequence_idempotency() {
  CapturingTimeseriesStore store;
  TimeseriesService timeseries(store);

  const ProtocolMessage trade{
      EngineEventsTopic,
      "TradeExecuted",
      {{"market_id", "7"},
       {"engine_sequence", "123"},
       {"engine_timestamp_ms", "1710000000123"},
       {"price", "101"},
       {"quantity", "5"},
       {"trade_id", "trade-1"},
       {"execution_reason", "normal"}},
      std::nullopt,
      0,
      22,
  };

  const auto written = timeseries.consume(trade);
  assert(written.status == TimeseriesConsumeStatus::Written);
  assert(written.idempotency_key.has_value());
  assert(written.idempotency_key->market_id == 7);
  assert(written.idempotency_key->engine_sequence == 123);
  assert(store.trades.size() == 1);
  assert(store.candle_writes.size() == 5);
  assert(store.offsets.size() == 1);

  const auto duplicate = timeseries.consume(trade);
  assert(duplicate.status == TimeseriesConsumeStatus::Duplicate);

  const ProtocolMessage mark{
      EngineEventsTopic,
      "MarkPriceUpdated",
      {{"market_id", "7"}, {"engine_sequence", "124"}},
      std::nullopt,
      0,
      23,
  };
  const auto ignored = timeseries.consume(mark);
  assert(ignored.status == TimeseriesConsumeStatus::Ignored);
}

void orderbook_consumes_orderbook_delta() {
  CapturingOrderbookStore store;
  OrderbookProjector projector(store);

  const OrderBookDeltaDto delta{
      MarketSequenceKey{5, 9},
      1710000003000,
      {PriceLevelDeltaDto{100, 10}},
      {PriceLevelDeltaDto{101, 0}},
      StreamPosition{EngineEventsTopic, 0, 77},
  };

  const auto applied = projector.project(delta);
  assert(applied.status == OrderbookProjectorStatus::Applied);
  assert(applied.idempotency_key.has_value());
  assert(store.deltas.size() == 1);
  assert(store.deltas[0].bids[0].quantity == 10);
  assert(store.deltas[0].asks[0].quantity == 0);

  const auto duplicate = projector.project(delta);
  assert(duplicate.status == OrderbookProjectorStatus::Duplicate);

  const ProtocolMessage envelope{
      EngineEventsTopic,
      "OrderBookDelta",
      {{"market_id", "5"},
       {"engine_sequence", "10"},
       {"engine_timestamp_ms", "1710000004000"}},
      std::nullopt,
      0,
      78,
  };
  const auto from_envelope = projector.consume(envelope);
  assert(from_envelope.status == OrderbookProjectorStatus::Applied);
}

void websocket_routes_wallet_events_by_payload_user_id() {
  CapturingWebsocketSink sink;
  WebsocketFanout fanout(sink);

  const ProtocolMessage wallet_event{
      WalletEventsTopic,
      "AccountDeltaApplied",
      {{"user_id", "99"}, {"kind", "FUNDING_PAYMENT"}},
      std::nullopt,
      1,
      12,
  };

  const auto routed = fanout.route(wallet_event);
  assert(routed.status == WebsocketFanoutStatus::Routed);
  assert(routed.events.size() == 1);
  assert(sink.events.size() == 1);
  assert(sink.events[0].outbound_type == "AccountEvent");
  assert(sink.events[0].route.kind == FanoutRouteKind::PrivateUser);
  assert(sink.events[0].route.user_id == 99);

  const ProtocolMessage missing_user{
      WalletEventsTopic,
      "DepositApplied",
      {{"amount", "10"}},
      std::nullopt,
      1,
      13,
  };
  const auto invalid = fanout.route(missing_user);
  assert(invalid.status == WebsocketFanoutStatus::Invalid);
}

void fake_engine_smoke_validates_engine_input_contract() {
  const TopicConfig valid{
      EngineInputTopic,
      FakeEngineSmokeContract::RequiredEngineInputPartitions,
      FakeEngineSmokeContract::RequiredEngineInputRetentionMs,
  };
  assert(FakeEngineSmokeContract::is_valid_engine_input(valid));

  const TopicConfig invalid{EngineInputTopic, 2, 60'000};
  const auto issues = FakeEngineSmokeContract::validate_engine_input(invalid);
  assert(issues.size() == 2);
  assert(issues[0].field == "partitions");
  assert(issues[1].field == "retention.ms");
}

}  // namespace

int main() {
  ledger_consumes_wallet_events_only();
  timeseries_uses_market_sequence_idempotency();
  orderbook_consumes_orderbook_delta();
  websocket_routes_wallet_events_by_payload_user_id();
  fake_engine_smoke_validates_engine_input_contract();
  return 0;
}
