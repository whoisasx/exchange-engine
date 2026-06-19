# WebSocket Live Updates

`apps/ws` is the live fanout service for connected clients. It consumes `engine.events` and `wallet.events` from Redpanda and forwards newly observed events to authenticated sockets.

It does not connect to Postgres, build read models, recover missed client messages, or own durable facts. `engine.events` and `wallet.events` must carry the account and market routing fields needed by websocket clients.

Run:

```sh
cargo run -p ws
```

Connect:

```text
GET /ws?token=<jwt>
Authorization: Bearer <jwt>
```

The token uses the same HS256 `JWT_SECRET` and claim shape as the REST server: `userid`, `username`, and `exp`.

## Client Messages

Subscribe to market feeds:

```json
{"type":"Subscribe","payload":{"markets":[1,2]}}
```

Unsubscribe from market feeds:

```json
{"type":"Unsubscribe","payload":{"markets":[1]}}
```

Application-level ping:

```json
{"type":"Ping","payload":{"nonce":"optional"}}
```

## Server Messages

Private account updates are sent to the authenticated user. They may come from either `engine.events` or `wallet.events`:

```json
{
  "type": "AccountEvent",
  "payload": {
    "source": "engine",
    "event": {"type":"OrderOpened","payload":{}},
    "metadata": {"topic":"engine.events","partition":0,"offset":123}
  }
}
```

Market updates are sent only after subscribing to a market:

```json
{
  "type": "MarketEvent",
  "payload": {
    "market_id": 1,
    "source": "engine",
    "event": {"type":"TradeExecuted","payload":{}},
    "metadata": {"topic":"engine.events","partition":0,"offset":124}
  }
}
```

Forward engine market and account-impacting events unchanged, including:

- `OrderBookDelta`
- `TradeExecuted`
- `MarkPriceUpdated`
- `FundingRateUpdated`
- `FundingPaymentApplied`
- `PositionChanged`
- `RiskStateUpdated`
- `LiquidationStarted`
- `LiquidationExecuted`
- `LiquidationCompleted`
- `AdlExecuted`
- `AccountDelta`

Forward `wallet.events` that carry authenticated user/account routing as private account updates. Ledger and balance facts come from wallet events, not from websocket delivery.

Wallet event payloads are documented in `docs/wallet-events.md`. WS routes all
wallet events by `payload.user_id`, including `AccountDeltaApplied` for fees,
funding payments, liquidation settlement, ADL settlement, and other
engine-originated balance mutations after wallet applies them.

Engine replies are request lifecycle messages only. If surfaced over websocket, they are not durable facts and must not be used as accounting, order, fill, or balance truth.

The service starts from latest Redpanda offsets. It is live fanout only. For state that needs gap-free live stitching, clients should subscribe before fetching the REST snapshot.

Engine event payloads are forwarded unchanged, including `engine_sequence` and `engine_timestamp_ms`. Clients that combine snapshots with live market updates should use the engine sequence from the payload for ordering, not websocket receive time.

For orderbook state:

1. Open websocket and subscribe to the market.
2. Buffer `OrderBookDelta` market events.
3. Fetch `GET /api/markets/{market_id}/orderbook?depth=50`.
4. Drop buffered deltas with `engine_sequence <= snapshot.engine_sequence`.
5. Apply remaining and future deltas in sequence order.
