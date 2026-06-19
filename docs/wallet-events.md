# Wallet Event Schema

`wallet.events` is the accounting source consumed by ledger and the private
account source consumed by websocket. Every wallet event is JSON encoded as:

```json
{
  "type": "VariantName",
  "payload": {}
}
```

Wallet events must carry `user_id` so consumers can route without database
lookups.

## Variants

### FundsReserved

Emitted when wallet locks collateral for an accepted order intent.

```text
request_id, user_id, reservation_id, asset, amount
```

Ledger entry:

```text
kind=RESERVE, total_delta=0, locked_delta=+amount, reference_id=reservation_id
```

### FundsReleased

Emitted when wallet unlocks reserved collateral after cancel, expiry, reject,
or other engine release events.

```text
user_id, reservation_id, asset, amount, reason
```

Ledger entry:

```text
kind=RELEASE, total_delta=0, locked_delta=-amount, reference_id=reservation_id
```

### TradeSettled

Emitted when wallet applies a trade settlement against a reservation.

```text
user_id, fill_id, reservation_id, debit_asset, debit_amount, credit_asset, credit_amount
```

Ledger entries:

```text
kind=TRADE_DEBIT, total_delta=-debit_amount, locked_delta=-debit_amount, reference_id=fill_id
kind=TRADE_CREDIT, total_delta=+credit_amount, locked_delta=0, reference_id=fill_id
```

### DepositApplied

Emitted when a deposit updates wallet balance.

```text
request_id, user_id, asset, amount, reference_id, total, locked
```

Ledger entry:

```text
kind=DEPOSIT, total_delta=+amount, locked_delta=0, reference_id=reference_id
```

### WithdrawalApplied

Emitted when a withdrawal updates wallet balance.

```text
request_id, user_id, asset, amount, destination, total, locked
```

Ledger entry:

```text
kind=WITHDRAWAL, total_delta=-amount, locked_delta=0, reference_id=request_id
```

### AccountDeltaApplied

Emitted when wallet applies an engine-originated account delta such as fee,
funding payment, liquidation settlement, ADL settlement, insurance transfer,
or other balance mutation that does not fit reservation settlement.

```text
user_id, asset, total_delta, locked_delta, kind, reference_id, total, locked
```

Ledger entry:

```text
kind=<event.kind>, total_delta=<event.total_delta>, locked_delta=<event.locked_delta>, reference_id=reference_id
```

`kind` is intentionally dynamic. Current producers use values such as
`TRADE_FEE`, `FUNDING_PAYMENT`, `FEE_CHARGED`, and `ACCOUNT_DELTA`.

## Consumer Rules

- Ledger journals every `wallet.events` record and normalizes it into
  `ledger_entries`.
- Websocket routes every wallet event as a private `AccountEvent` using
  `payload.user_id`.
- Engine events are audit context for money movement. Wallet events are the
  accounting source of truth.
