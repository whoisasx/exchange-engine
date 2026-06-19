# Timeseries Service

`apps/timeseries` consumes `engine.events` and writes trade history and market candles.

Run:

```sh
cargo run -p timeseries
```

Source events:

- `TradeExecuted` is the source for trades and candles.
- Liquidation executions are still trades and carry `execution_reason`.
- `MarkPriceUpdated`, funding rate, and funding settlement events may be mirrored into history tables later, but they do not create or update candles.

For every `TradeExecuted`, the service writes:

- `timeseries_trades`: one idempotency row keyed by `(market_id, engine_sequence)`.
- `candles`: OHLCV rows for `1m`, `5m`, `15m`, `1h`, and `1d`.
- `timeseries_offsets`: consumed Redpanda offsets.

`engine_sequence` is per-market. Use `(market_id, engine_sequence)` for market ordering and idempotency; global ordering across markets is not guaranteed. Use `engine_timestamp_ms` to choose the candle bucket.

Read candles through the REST server:

```text
GET /api/markets/{market_id}/candles?interval=1m&start_ms=1710000000000&end_ms=1710003600000&limit=500
```

Supported intervals are `1m`, `5m`, `15m`, `1h`, and `1d`. The response is sorted oldest to newest after selecting the latest matching rows up to `limit`.
