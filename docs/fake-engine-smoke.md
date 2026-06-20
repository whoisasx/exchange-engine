# Fake Engine Smoke Harness

## Current C++ engine smoke

This C++ repo now has a local, deterministic engine smoke that does not require
Redpanda, wallet, projector, websocket, or any other external service:

```sh
scripts/engine-smoke.sh
```

The script configures and builds the repo with CMake, then runs the
`engine_smoke` CTest target. The smoke drives `EngineRuntime` and `EngineOutbox`
with an in-process publisher, using `docs/examples/engine-place-order.command.json`
for the resting order, a crossing order fixture generated in the test, duplicate
handling, and a cancel flow. It asserts replies/events are serialized to
parseable JSON and verifies public `TradeExecuted` / `OrderBookDelta`
`engine_sequence` behavior.

Redpanda is optional for this local smoke. If `rpk` is installed, the script
prints the local `engine.input` provisioning commands. To run those commands
against an available local Redpanda cluster:

```sh
scripts/engine-smoke.sh --provision-redpanda
```

The provisioning keeps `engine.input` explicit as a single-partition topic with
`retention.ms=1800000`:

```sh
rpk topic create --if-not-exists --partitions 1 -c retention.ms=1800000 engine.input
rpk topic alter-config engine.input --set retention.ms=1800000
```

## Broader service smoke

`apps/fake-engine` is a development-only contract smoke stand-in for the engine
in the broader service workspace. It is not a production matching, margin,
funding, or liquidation engine. It exists to exercise the flattened stream
contract docs and downstream consumers. Those wallet, projector, websocket,
server, and fake-engine services are not part of this C++ repo yet, so this
end-to-end service smoke remains future/integration scope here.

Run the automated smoke:

```sh
scripts/e2e-smoke.sh
```

The script starts local Postgres and Redpanda with Docker Compose, creates the stream topics, starts `wallet`, `projector`, `timeseries`, `orderbook-archiver`, `ledger`, `fake-engine`, `ws`, and `server`, then drives the REST and websocket flow with `apps/e2e-smoke`.

The broader smoke script provisions `engine.input` explicitly as a
single-partition topic with `retention.ms=1800000`. For manual local setup,
apply the same setting before starting services:

```sh
rpk topic create --if-not-exists --partitions 1 -c retention.ms=1800000 engine.input
rpk topic alter-config engine.input --set retention.ms=1800000
```

To run it manually, start the services alongside each other:

```sh
cargo run -p wallet
cargo run -p projector
cargo run -p timeseries
cargo run -p orderbook-archiver
cargo run -p ledger
cargo run -p fake-engine
cargo run -p ws
cargo run -p server
```

Scope:

- Uses the flattened docs layout, including `docs/engine-contract.md` and `docs/examples/`.
- Treats wallet as the MVP ingress to the ordered engine input log: wallet reserves collateral, rejects insufficient-fund orders, then forwards reserved place/cancel inputs.
- Keeps mark price and funding as separate ordered engine inputs, not fields attached to every user order.
- Publishes request replies on `engine.replies` and durable market/account facts on `engine.events`.
- Emits smoke-level `OrderOpened`, `OrderCancelled`, `TradeExecuted`, and `OrderBookDelta` events with per-market `engine_sequence` and `engine_timestamp_ms`.
- Matches a new order against the oldest opposite-side resting order on the same market when prices cross.
- May observe `wallet.events` only to keep smoke settlement amounts compatible with wallet accounting.
- Does not implement production recovery, risk, funding, liquidation, ADL, or checkpoint behavior.

Smoke path:

1. Deposit enough collateral for two user pairs.
2. Place resting orders on two markets.
3. Confirm each subscribed websocket only receives market events for its subscribed market.
4. Place opposite-side crossing orders on both markets.
5. Confirm the server receives engine replies, wallet receives settlement events, projector writes the order/fill/position/orderbook read models, timeseries writes candles, ledger writes audit entries, and websocket clients receive account plus market updates per market.
6. Close one primary-market position with a reduce-only flow and confirm secondary-market subscribers do not receive primary-market events.

Plan expectations that this smoke harness only stands in for:

- Liquidation should run after trades, mark updates, funding settlement ticks, and startup restore after replay catches up.
- Wallet may forward mark and funding inputs for MVP, but that path must remain isolated from balance logic so it can later move into dedicated engine ingress.
- Engine checkpoints are private recovery artifacts; orderbook snapshots are public market-data artifacts and are not enough for recovery.
