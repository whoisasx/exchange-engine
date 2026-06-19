use serde::{Deserialize, Serialize};

use crate::{
    common::{Asset, CommandEnvelope, OrderType, Side},
    engine::{CancelOrder, ReservedPlaceOrder},
};

pub const WALLET_COMMANDS_TOPIC: &str = "wallet.commands";
pub const WALLET_REPLIES_TOPIC: &str = "wallet.replies";
pub const WALLET_EVENTS_TOPIC: &str = "wallet.events";

fn default_leverage() -> i64 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum WalletCommand {
    PlaceOrderIntent(PlaceOrderIntent),
    CancelOrderIntent(CancelOrderIntent),
    Deposit(Deposit),
    Withdraw(Withdraw),
    ReleaseReservation(ReleaseReservation),
    SettleTrade(SettleTrade),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceOrderIntent {
    pub envelope: CommandEnvelope,
    pub market_id: i64,
    pub market_name: String,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: i64,
    pub price: i64,
    pub margin_asset: Asset,
    pub required_margin: i64,
    #[serde(default = "default_leverage")]
    pub leverage: i64,
    pub reduce_only: bool,
}

impl PlaceOrderIntent {
    pub fn into_reserved_order(self, reservation_id: String) -> ReservedPlaceOrder {
        ReservedPlaceOrder {
            input_id: None,
            envelope: self.envelope,
            reservation_id,
            market_id: self.market_id,
            market_name: self.market_name,
            side: self.side,
            order_type: self.order_type,
            quantity: self.quantity,
            price: self.price,
            reduce_only: self.reduce_only,
            margin_asset: self.margin_asset,
            reserved_margin_amount: self.required_margin,
            leverage: self.leverage,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrderIntent {
    pub envelope: CommandEnvelope,
    pub market_id: i64,
    pub order_id: i64,
}

impl CancelOrderIntent {
    pub fn into_engine_cancel_order(self) -> CancelOrder {
        CancelOrder {
            input_id: None,
            envelope: self.envelope,
            market_id: self.market_id,
            order_id: self.order_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deposit {
    pub envelope: CommandEnvelope,
    pub asset: Asset,
    pub amount: i64,
    pub reference_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Withdraw {
    pub envelope: CommandEnvelope,
    pub asset: Asset,
    pub amount: i64,
    pub destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseReservation {
    pub reservation_id: String,
    pub amount: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettleTrade {
    pub fill_id: i64,
    pub reservation_id: String,
    pub debit_asset: Asset,
    pub debit_amount: i64,
    pub credit_asset: Asset,
    pub credit_amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum WalletReply {
    FundsReserved(FundsReserved),
    InsufficientFunds(InsufficientFunds),
    BalanceUpdated(BalanceUpdated),
    CommandAccepted(CommandAccepted),
    CommandRejected(CommandRejected),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundsReserved {
    pub request_id: String,
    pub reservation_id: String,
    pub asset: Asset,
    pub amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsufficientFunds {
    pub request_id: String,
    pub asset: Asset,
    pub required: i64,
    pub available: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceUpdated {
    pub request_id: String,
    pub asset: Asset,
    pub total: i64,
    pub locked: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandAccepted {
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandRejected {
    pub request_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum WalletEvent {
    FundsReserved(WalletFundsReserved),
    FundsReleased(WalletFundsReleased),
    TradeSettled(WalletTradeSettled),
    DepositApplied(WalletDepositApplied),
    WithdrawalApplied(WalletWithdrawalApplied),
    AccountDeltaApplied(WalletAccountDeltaApplied),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletFundsReserved {
    pub request_id: String,
    pub user_id: i64,
    pub reservation_id: String,
    pub asset: Asset,
    pub amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletFundsReleased {
    pub user_id: i64,
    pub reservation_id: String,
    pub asset: Asset,
    pub amount: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletTradeSettled {
    pub user_id: i64,
    pub fill_id: i64,
    pub reservation_id: String,
    pub debit_asset: Asset,
    pub debit_amount: i64,
    pub credit_asset: Asset,
    pub credit_amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletDepositApplied {
    pub request_id: String,
    pub user_id: i64,
    pub asset: Asset,
    pub amount: i64,
    pub reference_id: String,
    pub total: i64,
    pub locked: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletWithdrawalApplied {
    pub request_id: String,
    pub user_id: i64,
    pub asset: Asset,
    pub amount: i64,
    pub destination: String,
    pub total: i64,
    pub locked: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletAccountDeltaApplied {
    pub user_id: i64,
    pub asset: Asset,
    pub total_delta: i64,
    pub locked_delta: i64,
    pub kind: String,
    pub reference_id: String,
    pub total: i64,
    pub locked: i64,
}

#[cfg(test)]
mod tests {
    use crate::common::{CommandEnvelope, OrderType, Side};

    use super::*;

    #[test]
    fn place_order_intent_becomes_reserved_engine_order() {
        let intent = PlaceOrderIntent {
            envelope: CommandEnvelope {
                request_id: String::from("req-1"),
                idempotency_key: String::from("order-1"),
                user_id: 42,
                reply_partition: 0,
            },
            market_id: 1,
            market_name: String::from("SOL-PERP"),
            side: Side::LONG,
            order_type: OrderType::LIMIT,
            quantity: 10,
            price: 20,
            margin_asset: Asset::USDC,
            required_margin: 200,
            leverage: 10,
            reduce_only: true,
        };

        let order = intent.into_reserved_order(String::from("res-1"));

        assert_eq!(order.envelope.request_id, "req-1");
        assert_eq!(order.reservation_id, "res-1");
        assert_eq!(order.market_id, 1);
        assert_eq!(order.quantity, 10);
        assert_eq!(order.price, 20);
        assert!(order.reduce_only);
        assert_eq!(order.margin_asset, Asset::USDC);
        assert_eq!(order.reserved_margin_amount, 200);
        assert_eq!(order.leverage, 10);
    }

    #[test]
    fn cancel_order_intent_becomes_engine_cancel_order() {
        let intent = CancelOrderIntent {
            envelope: CommandEnvelope {
                request_id: String::from("req-1"),
                idempotency_key: String::from("cancel-1"),
                user_id: 42,
                reply_partition: 0,
            },
            market_id: 1,
            order_id: 99,
        };

        let cancel = intent.into_engine_cancel_order();

        assert_eq!(cancel.envelope.request_id, "req-1");
        assert_eq!(cancel.market_id, 1);
        assert_eq!(cancel.order_id, 99);
    }

    #[test]
    fn wallet_event_funds_reserved_carries_user_id() {
        let event = WalletEvent::FundsReserved(WalletFundsReserved {
            request_id: String::from("req-1"),
            user_id: 42,
            reservation_id: String::from("res-1"),
            asset: Asset::USDC,
            amount: 100,
        });
        let value = serde_json::to_value(event).expect("event should serialize");

        assert_eq!(value["type"], "FundsReserved");
        assert_eq!(value["payload"]["user_id"], 42);
        assert_eq!(value["payload"]["reservation_id"], "res-1");
    }

    #[test]
    fn wallet_account_delta_event_carries_dynamic_kind_and_balance() {
        let event = WalletEvent::AccountDeltaApplied(WalletAccountDeltaApplied {
            user_id: 42,
            asset: Asset::USDC,
            total_delta: -3,
            locked_delta: 0,
            kind: String::from("TRADE_FEE"),
            reference_id: String::from("fill:7:fee:0:42:TAKER"),
            total: 997,
            locked: 0,
        });
        let value = serde_json::to_value(event).expect("event should serialize");

        assert_eq!(value["type"], "AccountDeltaApplied");
        assert_eq!(value["payload"]["user_id"], 42);
        assert_eq!(value["payload"]["kind"], "TRADE_FEE");
        assert_eq!(value["payload"]["total_delta"], -3);
        assert_eq!(value["payload"]["total"], 997);
    }

    #[test]
    fn wallet_reply_funds_reserved_does_not_carry_user_id() {
        let reply = WalletReply::FundsReserved(FundsReserved {
            request_id: String::from("req-1"),
            reservation_id: String::from("res-1"),
            asset: Asset::USDC,
            amount: 100,
        });
        let value = serde_json::to_value(reply).expect("reply should serialize");

        assert_eq!(value["type"], "FundsReserved");
        assert!(value["payload"].get("user_id").is_none());
    }
}
