# Orderbook Market Data

The orderbook read model is a public market-data artifact built from `OrderBookDelta` events on `engine.events`. It is not an engine recovery checkpoint; engine recovery uses separate private `EngineCheckpoint` artifacts.

`OrderBookDelta` carries changed price levels only:

```json
{
  "type": "OrderBookDelta",
  "payload": {
    "engine_sequence": 4,
    "engine_timestamp_ms": 1710000003000,
    "market_id": 1,
    "bids": [{"price": 100, "quantity": 10}],
    "asks": []
  }
}
```

`bids` are LONG-side levels, `asks` are SHORT-side levels, and `quantity=0` removes a level. Consumers should order deltas by `(market_id, engine_sequence)`; `engine_sequence` is per market, and global ordering across markets is not guaranteed.

The projector writes the current public snapshot to `orderbook_state` and `orderbook_levels` for REST reads, history, and client recovery.

`apps/orderbook-archiver` consumes `OrderBookSnapshotCreated` events and writes a local object-store-shaped JSON artifact. The default local store is `.data/orderbook-archiver/objects`, with bucket/key settings that can later map to S3:

```text
ORDERBOOK_ARCHIVE_BUCKET=exchange-market-data
ORDERBOOK_ARCHIVE_KEY_PREFIX=orderbooks/snapshots
ORDERBOOK_ARCHIVE_LOCAL_ROOT=.data/orderbook-archiver/objects
ORDERBOOK_ARCHIVER_OFFSET_LOCAL_ROOT=.data/orderbook-archiver/offsets
```

Read snapshots through the REST server:

```text
GET /api/markets/{market_id}/orderbook?depth=50
```

The response includes `market_id` and `engine_sequence`. Clients should connect to websocket first, subscribe, buffer `OrderBookDelta` events for the market, fetch this snapshot, then apply only deltas with the same `market_id` and `engine_sequence > snapshot.engine_sequence`.
