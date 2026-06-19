# Matching Engine Deep Dive

This is the only retained planning note for the C++ engine internals. The
authoritative stream contract lives in `docs/`; this file explains how the
in-memory matching core should work, especially the ring-buffer price ladder.

## Current Direction

The engine implementation target is C++ only.

Rust files under `docs/reference/` are reference material for schema shape and
fixtures. They are not runtime code.

The engine is split into layers:

```text
engine.input JSON
  -> C++ protocol parser
  -> C++ engine runtime
  -> adapter boundary
  -> EngineCore / OrderBook / SideBook / RingPriceLadder
  -> adapter enriches core output
  -> engine.replies + engine.events JSON
```

The matching core must stay small and deterministic. It should not know about
Redpanda, Postgres, websocket clients, ledger rows, wallet storage, or JSON.

The adapter/runtime owns:

- request IDs and idempotency keys from the wire protocol
- server-provided `order_id`
- reservation IDs and wallet metadata
- topic, partition, and offset metadata
- public `engine_event_id`
- per-market public `engine_sequence`
- timestamps used by consumers
- conversion between protocol strings and core enums

The matching core owns:

- symbol order books
- price/quantity validation
- place/cancel handling
- price-time priority
- trade generation
- internal snapshots
- internal sequence/idempotency state needed by the core API

## Order Flow

The server assigns `order_id` before engine ingress. The engine must preserve
that ID; it should not generate order IDs for normal user orders.

```text
server
  -> wallet reserves collateral
  -> engine.input PlaceOrder
  -> adapter maps to PlaceOrderCommand
  -> EngineCore::process
  -> OrderBook::process_place
  -> internal core events
  -> adapter publishes public replies/events
```

Protocol-to-core mapping:

```text
market_id       -> SymbolId
LONG            -> Buy
SHORT           -> Sell
LIMIT / MARKET  -> Limit / Market
order_id        -> OrderId
request_id      -> adapter-owned lifecycle reply key
idempotency_key -> adapter-owned external dedupe key
reservation_id  -> adapter-owned wallet metadata
```

The adapter may derive numeric core command IDs from request/idempotency strings
only to satisfy the current core API. The original strings remain the protocol
source of truth.

## Ring Buffer Price Ladder

The order book does not use a `std::map` for every price level. Each side uses a
hybrid structure:

```text
SideBook
  RingPriceLadder  near active market window
  FarPriceMap      prices outside the ring window
  cachedBestLevel  best non-empty price level
```

The ring ladder is an in-memory optimization only. It is not part of the public
contract, recovery contract, REST shape, websocket shape, or downstream service
state.

### Tick Model

Prices are represented as integer ticks. A symbol config supplies the tick size.

```text
priceTick = price.ticks() / tickSizeTicks
```

The ring covers a contiguous tick window:

```text
baseTick = lowest tick currently represented by the ring
capacity = number of ticks covered by the ring
headIndex = physical slot for baseTick
```

Slot lookup:

```text
offset = priceTick - baseTick
slot = (headIndex + offset) % capacity
```

If `offset` is outside `[0, capacity)`, the price level belongs in
`FarPriceMap`.

### Active Slot Bitmap

`RingPriceLadder` keeps an `activeSlotBitmap` next to the slot array.

This avoids scanning every slot when the cached best level is empty. For bids,
the ladder searches for the highest active slot. For asks, it searches for the
lowest active slot.

```text
bids: find last active level
asks: find first active level
```

Empty levels are cleared and their bitmap bit is marked inactive.

### Cached Best Level

`SideBook` keeps `cachedBestLevel`.

On insert:

1. Create or find the price level in the ring or far map.
2. Append the order to that level.
3. If the inserted level is better than the cached best, replace the cache.

On fill/cancel:

1. Decrease the level total.
2. Remove the order from the intrusive FIFO list if needed.
3. Remove empty levels from the ring/far map.
4. Refresh `cachedBestLevel` if the previous best emptied.

This gives the hot operations the intended shape:

```text
insert near spread: O(1)
best order lookup: O(1) from cached best
cancel by order_id: O(1) via orderById pointer lookup
same-price FIFO: O(1) intrusive linked list
far price insert: O(log n) fallback map
```

## Price Level FIFO

Each `PriceLevel` stores same-price orders in FIFO order:

```text
PriceLevel
  price
  totalQuantity
  head Order*
  tail Order*
  orderCount
```

Each `Order` is its own linked-list node:

```text
Order
  orderId
  clientOrderId
  userId
  symbolId
  side
  price
  originalQuantity
  remainingQuantity
  previousOrder
  nextOrder
  priceLevel
```

There is no separate queue node allocation. The order map points directly to the
resting `Order`, so cancellation can unlink it without scanning the book.

## Ring Recentering

The current implementation only recenters an empty ring. That is intentional for
the current safe implementation stage.

```text
if ring has active levels:
  do not recenter
else:
  choose new baseTick around referenceTick
  reset headIndex
  clear slots and bitmap
```

Moving a non-empty ring requires preserving price levels and order pointers. Do
not add that until tests cover:

- active level relocation
- cached best refresh
- order pointer stability
- snapshot/restore equivalence
- no loss of FIFO priority

Until then, prices outside the active window go to `FarPriceMap`.

## Public Orderbook Events

The core may emit compact internal deltas such as:

```text
symbolId, side, price, totalQuantityAtPrice
```

The public protocol shape is different:

```text
market_id
engine_sequence
engine_timestamp_ms
bids[]
asks[]
```

That transformation belongs in the adapter/projector boundary, not inside the
matching structures.

Mapping:

```text
Buy  -> bids
Sell -> asks
quantity = 0 means delete this price level
```

Consumers order public market data by `(market_id, engine_sequence)`.

## What Must Not Be Added To The Core

Do not push these into `OrderBook`, `SideBook`, `PriceLevel`, or
`RingPriceLadder`:

- JSON parsing
- Redpanda topic names or offsets
- REST/websocket response shape
- ledger/accounting writes
- wallet balance mutation
- public event IDs
- request reply partitions
- database transaction behavior
- cross-service recovery logic

Those belong to protocol, adapter, runtime, or downstream service layers.

## Next Implementation Step

The next useful engine step is an in-process C++ runtime:

1. Parse typed `EngineInput` JSON.
2. Validate `PlaceOrder` / `CancelOrder` fields.
3. Map to adapter DTOs.
4. Call `EngineCore`.
5. Convert internal core events into protocol-shaped replies/events.
6. Assert output against fixtures or focused expected messages.

Only after that should Redpanda I/O be wired in.
