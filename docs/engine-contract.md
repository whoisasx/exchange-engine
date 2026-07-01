# Engine Stream Contract

This contract describes the current engine stream protocol used by this
workspace and the agreement with the C++ matching engine.

The workspace protocol uses `engine.input` for engine-affecting inputs.
`ENGINE_COMMANDS_TOPIC` remains an alias for `ENGINE_INPUT_TOPIC`, and
`ENGINE_COMMANDS_LEGACY_TOPIC` names the old `engine.commands` topic for
compatibility.

Documentation is flattened under `docs/`. JSON examples and protocol fixtures live under `docs/examples/`.

The upstream workspace protocol source of truth is:

- `crates/protocol/src/common.rs`
- `crates/protocol/src/engine.rs`
- `crates/protocol/src/wallet.rs`

All stream messages are JSON encoded with this shape:

```json
{
  "type": "VariantName",
  "payload": {}
}
```

```mermaid
flowchart LR
    wallet[exchange wallet] --> input[(engine.input)]
    ingress[engine-ingress tools] --> input
    input --> worker0[engine worker<br/>market 1 / partition 4]
    input --> worker1[engine worker<br/>market 2 / partition 5]

    worker0 --> replies[(engine.replies)]
    worker1 --> replies
    worker0 --> events[(engine.events)]
    worker1 --> events
    worker0 --> checkpoint[(S3/file checkpoint)]
    worker1 --> checkpoint

    replies --> server[exchange server reply consumer]
    events --> walletConsumer[exchange wallet]
    events --> projector[exchange projector]
    events --> ws[exchange websocket]
    events --> timeseries[exchange timeseries]
```

## Topics

| Topic | Producer | Consumer | Purpose |
| --- | --- | --- | --- |
| `engine.input` | wallet / MVP engine ingress | C++ engine | Market-partitioned engine-affecting inputs for orders, cancels, mark price, and funding. |
| `engine.replies` | C++ engine | server reply consumer | Request lifecycle replies only. |
| `engine.events` | C++ engine | wallet, projector, websocket, ledger, timeseries | Durable engine facts and state changes. |

For MVP, the wallet process is the engine ingress publisher because it already owns the hot-path reservation flow. Mark price and funding forwarding must stay isolated from wallet balance logic so this path can later move into a dedicated `engine-ingress` service.

## Topic Provisioning

Provisioning must not rely on broker defaults for `engine.input`. Local, e2e, and production topic creation must create it with an explicit partition count and 30-minute time retention:

```sh
rpk topic create --if-not-exists --partitions 8 -c retention.ms=1800000 engine.input
rpk topic alter-config engine.input --set retention.ms=1800000
```

The partition count must cover the configured engine market ownership map. Each `[[market]]` in the engine markets config owns one non-negative `input_partition`, and duplicate partition ownership is rejected. The exchange publisher uses the same stable key partitioning for `market_id`, so the engine markets config must use the partition result for the configured topic partition count. The 30-minute `retention.ms` setting matches the MVP recovery window in the engine plan.

## Ordering

| Topic | Record key | Ordering rule |
| --- | --- | --- |
| `engine.input` | `market_id` as string | Inputs for one market must land on that market's configured input partition and are processed by that partition's single-threaded worker. |
| `engine.replies` | `request_id` | Engine must publish to `payload.envelope.reply_partition` from the original request input. |
| `engine.events` | `market_id` as string | Engine should keep events for one market ordered on the same partition. |

All engine-affecting inputs enter `engine.input` keyed by `market_id`. Mark price and funding are first-class inputs in that log; they are not embedded into user order commands. Each engine worker owns one input partition, one runtime, one recovery coordinator, and one checkpoint namespace, so a single market remains single-threaded while different markets can run in parallel.

For every `market_id`, the engine must emit a strictly increasing `engine_sequence` on every event for that market. Redpanda offsets are still used by consumers for replay progress, but consumers should use `(market_id, engine_sequence)` for market ordering. Global ordering across markets is not guaranteed.

Engine events must be self-routeable by payload. WebSocket consumers must not need database lookups to identify the affected account or market.

## Checkpoint And Offset Contract

`EngineCheckpoint.source_position` records the input topic, partition, and exclusive `next_offset` represented by the checkpoint. `next_offset=N` means every input offset `< N` for that source partition is already included in engine state and deduplication metadata. Recovery must seek the input consumer to exactly `N` and silently replay `[N, high_watermark)`.

Broker committed offsets are not the recovery source of truth. They are operational resume hints and should align with checkpoint progress after a successful live cycle. The production-safe live order is:

1. Process one input and durably publish its replies/events.
2. Save a checkpoint with `source_position.next_offset = input.offset + 1`.
3. Commit the consumed input offset. The current rdkafka committer stores `input.offset + 1` as the Kafka resume offset.

If checkpoint save fails, the engine must fail loudly and not commit the input offset. A crash after checkpoint save but before commit is recoverable because startup recovery seeks to the checkpoint `next_offset`. A crash after commit but before checkpoint is unsafe when startup relies on the consumer group position, so app startup must use checkpoint recovery and live processing must place checkpoint durability before broker commit.

```mermaid
sequenceDiagram
    participant I as engine.input
    participant E as engine_app
    participant O as engine.replies/events
    participant S as Checkpoint store
    participant C as Broker offset commit

    E->>I: Poll offset N
    E->>E: Process into runtime state
    E->>O: Publish replies/events
    E->>S: Save checkpoint next_offset=N+1
    E->>C: Commit resume offset N+1

    Note over S,C: Checkpoint durability happens before broker commit.
```

## Common Types

Enums are serialized as exact uppercase strings:

```text
Asset: USDC, USDT, SOL, ETH, BTC, PERP, HYP
Side: LONG, SHORT
OrderType: LIMIT, MARKET
ExecutionReason: TRADE, LIQUIDATION
```

Numeric IDs, sequence, timestamp, price, quantity, amount, and margin fields are integer values compatible with signed 64-bit integers unless a field is explicitly defined as a string ID.

Margin is isolated only in the MVP. Engine risk state is scoped by `user_id + market_id`; cross-margin and cross-market liquidation are outside this contract.

`CommandEnvelope` is carried from server to wallet to engine for request-originated inputs:

```json
{
  "request_id": "req_01HZ8YEXAMPLE",
  "idempotency_key": "client-order-1",
  "user_id": 42,
  "reply_partition": 0
}
```

The engine must copy `request_id` into request lifecycle replies so the server can resolve the original request. Durable side effects must be represented by events, not replies.

## Engine Inputs

Engine inputs go to `engine.input` and are ordered by the final engine input log.

Current `EngineInput` variants:

- `PlaceOrder`
- `CancelOrder`
- `LiquidatePosition`
- `MarkPriceUpdated`
- `FundingRateUpdated`
- `FundingSettlementTick`

Later input variants may include `AddIsolatedMargin`, `RemoveIsolatedMargin`, `RiskConfigUpdated`, `MarketConfigUpdated`, and trusted keeper hints such as `EvaluateLiquidation`.

`LiquidatePosition` is a compatibility request path. In the target flow, liquidation is engine-owned. Any external liquidation input should be treated as a trusted evaluation hint; the engine must recalculate eligibility, quantity, and execution path from current engine state.

Input example locations under `docs/examples`:

| Input | Fixture |
| --- | --- |
| `PlaceOrder` | `docs/examples/engine-place-order.command.json` |
| `PlaceOrder` reduce-only | `docs/examples/engine-place-order-reduce-only.command.json` |
| `CancelOrder` | `docs/examples/engine-cancel-order.command.json` |
| `MarkPriceUpdated` | `docs/examples/engine-mark-price-updated.input.json` |
| `FundingRateUpdated` | `docs/examples/engine-funding-rate-updated.input.json` |
| `FundingSettlementTick` | `docs/examples/engine-funding-settlement-tick.input.json` |
| `LiquidatePosition` compatibility hint | `docs/examples/engine-liquidate-position.command.json` |

### PlaceOrder

Routing field: `market_id`

Fixtures:

- `docs/examples/engine-place-order.command.json`
- `docs/examples/engine-place-order-reduce-only.command.json`

Payload fields:

```text
input_id, envelope, reservation_id, order_id, market_id, market_name, side, order_type, quantity, price, reduce_only, margin_asset, reserved_margin_amount, leverage
```

`input_id` is optional. `order_id` is assigned before engine ingress and must be preserved by the engine. `margin_asset`, `reserved_margin_amount`, and `leverage` are first-class fields in the current workspace protocol and have defaults only for backward-compatible deserialization of older JSON.

Required engine behavior:

- Validate and accept or reject the order using current orderbook, position, mark price, fee, and isolated margin state.
- Treat `reduce_only=true` as a hard constraint: the order must not increase or reverse the user's isolated position.
- Publish exactly one `OrderAccepted` or `OrderRejected` reply for the request.
- If the order rests on the book, publish `OrderOpened`.
- If the order matches, publish `TradeExecuted`.
- Expire any reduce-only unmatched remainder that would create exposure; do not publish `OrderOpened` for that remainder.

The server may validate close quantity against projector state, but the engine owns final reduce-only enforcement because it has current matching and position state.

### CancelOrder

Routing field: `market_id`

Fixture: `docs/examples/engine-cancel-order.command.json`

Required engine behavior:

- Publish exactly one `CancelAccepted` or `CancelRejected` reply for the request.
- If cancellation releases reserved funds, publish a durable engine event such as `OrderCancelled` or `ReservationReleased`.

### MarkPriceUpdated

Routing field: `market_id`

Mark price is a separate ordered risk input, not a field attached to every user order.

Payload fields:

```text
input_id, market_id, mark_price, index_price, source_timestamp_ms, published_at_ms, valid_until_ms, source_sequence, source_status
```

`input_id` is optional.

Required engine behavior:

- Store the latest valid mark and index price for the market.
- Run liquidation checks for the affected market after applying the mark.
- If the mark is stale, reject risk-increasing orders and allow reduce-only and cancel actions.

### FundingRateUpdated

Routing field: `market_id`

Funding rate is a separate ordered input sent on the funding cadence. The engine stores the latest funding rate and interval in market and checkpoint state.

Payload fields:

```text
input_id, market_id, funding_interval_id, rate, rate_scale, interval_start_ms, interval_end_ms, source_timestamp_ms
```

`input_id` is optional.

### FundingSettlementTick

Routing field: `market_id`

Funding settlement is explicit. A rate update tells the engine the current rate; a settlement tick tells it to apply funding for an interval.

Payload fields:

```text
input_id, market_id, funding_interval_id, settle_at_ms
```

`input_id` is optional.

Required engine behavior:

- Apply funding to open isolated positions for the market.
- Emit durable events for account deltas, funding payments, and any resulting risk state changes.
- Run liquidation checks for the affected market after settlement.

## Liquidation Contract

Liquidation eligibility is engine-owned state. Read models may show indicative risk, but they are not authoritative.

The liquidation loop must run after:

- every executed trade
- every `MarkPriceUpdated`
- every `FundingSettlementTick`
- startup restore after replay catches up

Liquidation is market-local for MVP isolated margin. The engine uses the latest mark/index price, isolated position state, margin state, and available execution path. Liquidation execution must be reduce-only for the liquidated account and must not increase or reverse exposure.

Liquidation side effects are events, not request replies. A liquidation execution should emit `TradeExecuted` with `execution_reason="LIQUIDATION"` plus the relevant liquidation, fee, position, risk, and account-delta events.

If the compatibility `LiquidatePosition` request path remains enabled, `LiquidationAccepted` and `LiquidationRejected` are lifecycle replies for that request only. They are not durable liquidation facts.

## Replies

Replies go to `engine.replies` and must be produced to the original request envelope's `reply_partition`.

Replies are request lifecycle messages only. They unblock HTTP or RPC callers; they are not durable accounting facts and must not be used as the source of truth for fills, balances, funding, liquidation, or orderbook state.

| Reply | Fixture |
| --- | --- |
| `OrderAccepted` | `docs/examples/engine-order-accepted.reply.json` |
| `OrderRejected` | `docs/examples/engine-order-rejected.reply.json` |
| `CancelAccepted` | `docs/examples/engine-cancel-accepted.reply.json` |
| `CancelRejected` | `docs/examples/engine-cancel-rejected.reply.json` |
| `LiquidationAccepted` | `docs/examples/engine-liquidation-accepted.reply.json` |
| `LiquidationRejected` | `docs/examples/engine-liquidation-rejected.reply.json` |

Reply variants include `request_id`. `source_input_id` and `source_input_offset` are optional audit fields when available.

```text
OrderAccepted: request_id, source_input_id, source_input_offset, order_id, reservation_id
OrderRejected: request_id, source_input_id, source_input_offset, reservation_id, reason
CancelAccepted: request_id, source_input_id, source_input_offset, order_id
CancelRejected: request_id, source_input_id, source_input_offset, order_id, reason
LiquidationAccepted: request_id, source_input_id, source_input_offset, liquidation_id, order_id
LiquidationRejected: request_id, source_input_id, source_input_offset, liquidation_id, reason
```

`OrderRejected.reservation_id` may be `null` if no reservation should be released by downstream consumers.

Mark price, funding, startup liquidation, and engine-originated liquidation flows do not produce request lifecycle replies.

## Events

Events go to `engine.events`. Market-scoped events should be keyed by `market_id`; engine-wide events such as `EngineCheckpointCommitted` are not tied to a single market.

Market-scoped engine event payloads carry idempotent event metadata:

```text
engine_event_id: stable unique event ID when available
market_id: market affected by the event
engine_sequence: strictly increasing per market_id
engine_timestamp_ms: Unix epoch milliseconds assigned by the engine
source_input_id and source_input_offset: optional source engine input identity for replay/audit
```

Producers should set `engine_event_id` and keep it stable across retries of the same emitted event. Consumers use it to deduplicate exact event delivery when present. Consumers use `(market_id, engine_sequence)` for market ordering and gap detection. For ordering, consumers should trust `engine_sequence` before `engine_timestamp_ms`; timestamps are for bucketing, display, and latency analysis.

`EngineCheckpointCommitted` is engine-wide. It carries checkpoint metadata, `engine_input_next_offset`, and `market_sequences` instead of a single `market_id` and `engine_sequence`. For MVP this event is future audit only; engine recovery uses the private checkpoint artifact plus the retained `engine.input` log.

Event fixture locations under `docs/examples`:

| Event | Fixture |
| --- | --- |
| `OrderOpened` | `docs/examples/engine-order-opened.event.json` |
| `OrderCancelled` | `docs/examples/engine-order-cancelled.event.json` |
| `OrderExpired` | `docs/examples/engine-order-expired.event.json` |
| `ReservationReleased` | `docs/examples/engine-reservation-released.event.json` |
| `TradeExecuted` | `docs/examples/engine-trade-executed.event.json` |
| `TradeExecuted` with `execution_reason="LIQUIDATION"` | `docs/examples/engine-trade-executed-liquidation.event.json` |
| `OrderBookDelta` | `docs/examples/engine-orderbook-delta.event.json` |
| `MarkPriceUpdated` | `docs/examples/engine-mark-price-updated.event.json` |
| `FundingRateUpdated` | `docs/examples/engine-funding-rate-updated.event.json` |
| `FundingPaymentApplied` | `docs/examples/engine-funding-payment-applied.event.json` |
| `PositionChanged` | `docs/examples/engine-position-changed.event.json` |
| `RiskStateUpdated` | `docs/examples/engine-risk-state-updated.event.json` |
| `LiquidationStarted` | `docs/examples/engine-liquidation-started.event.json` |
| `LiquidationExecuted` | `docs/examples/engine-liquidation-executed.event.json` |
| `LiquidationCompleted` | `docs/examples/engine-liquidation-completed.event.json` |
| `AdlExecuted` | `docs/examples/engine-adl-executed.event.json` |
| `AccountDelta` | `docs/examples/engine-account-delta.event.json` |

The C++ `protocol_fixture_tests` target validates every JSON fixture in
`docs/examples` for top-level protocol shape and known fixture classification.

Consumers route market-scoped events with `market_id` and account fields such as `user_id`, `maker_user_id`, `taker_user_id`, `payments[].user_id`, or other variant-specific account identifiers. Engine-wide checkpoint events are recovery/audit signals and are not user-routed.

Event consumers use engine events as follows:

- Wallet consumes money-moving engine events such as reservation release, trade settlement, funding payment, fee charge, liquidation settlement, insurance fund transfer, ADL settlement, and account delta.
- Projector consumes engine events to update orders, fills, positions, orderbook, risk state, funding history, and mark price history.
- WebSocket consumes engine and wallet events for live user and market updates.
- Ledger consumes `wallet.events` as the accounting source. Engine events may be audit context, but they are not the balance-mutation source of truth.
- Timeseries consumes `TradeExecuted` for candles and trade history using `engine_timestamp_ms` for buckets and `(market_id, engine_sequence)` for idempotency.

`TradeExecuted.settlements` is the wallet-facing settlement instruction. Each settlement becomes a wallet settlement command. Liquidation trades may omit settlements for synthetic liquidation order IDs unless those IDs correspond to wallet reservations.

`OrderBookDelta` is price-level based. `bids` are LONG-side levels, `asks` are SHORT-side levels, and `quantity=0` means delete that price level.

Orderbook snapshots and deltas use the same per-market sequence. A snapshot carries the latest applied `engine_sequence`. Clients that need a gap-free orderbook should connect to websocket first, subscribe to the market, buffer live `OrderBookDelta` messages, then fetch the REST snapshot. After the snapshot is loaded, discard buffered deltas with `engine_sequence <= snapshot.engine_sequence` and apply only greater sequences.

## Compatibility Rules

- Do not rename JSON fields without updating protocol tests and fixtures.
- Do not change enum casing.
- Always include the top-level `type` and `payload` fields.
- Unknown extra fields may be ignored by some consumers today, but the engine
  should not rely on that for required behavior.
- New optional fields should be added in a backward-compatible way.
- Required field changes should be treated as a protocol version change.

## Fixture Validation

The C++ protocol fixture test parses every fixture in `docs/examples`.

Run:

```sh
ctest --test-dir build --output-on-failure -R '^protocol_fixture_tests$'
```
