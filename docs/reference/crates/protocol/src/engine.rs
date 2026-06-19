use serde::{Deserialize, Serialize};

use crate::common::{Asset, CommandEnvelope, OrderType, Side};

pub const ENGINE_INPUT_TOPIC: &str = "engine.input";
pub const ENGINE_COMMANDS_TOPIC: &str = ENGINE_INPUT_TOPIC;
pub const ENGINE_COMMANDS_LEGACY_TOPIC: &str = "engine.commands";
pub const ENGINE_REPLIES_TOPIC: &str = "engine.replies";
pub const ENGINE_EVENTS_TOPIC: &str = "engine.events";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EngineCommand {
    PlaceOrder(ReservedPlaceOrder),
    CancelOrder(CancelOrder),
    LiquidatePosition(LiquidatePosition),
    MarkPriceUpdated(MarkPriceUpdatedInput),
    FundingRateUpdated(FundingRateUpdatedInput),
    FundingSettlementTick(FundingSettlementTickInput),
}

pub type EngineInput = EngineCommand;

fn default_margin_asset() -> Asset {
    Asset::USDC
}

fn default_leverage() -> i64 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservedPlaceOrder {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_id: Option<String>,
    pub envelope: CommandEnvelope,
    pub reservation_id: String,
    pub order_id: i64,
    pub market_id: i64,
    pub market_name: String,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: i64,
    pub price: i64,
    pub reduce_only: bool,
    #[serde(default = "default_margin_asset")]
    pub margin_asset: Asset,
    #[serde(default)]
    pub reserved_margin_amount: i64,
    #[serde(default = "default_leverage")]
    pub leverage: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrder {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_id: Option<String>,
    pub envelope: CommandEnvelope,
    pub market_id: i64,
    pub order_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiquidatePosition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_id: Option<String>,
    pub envelope: CommandEnvelope,
    pub liquidation_id: String,
    pub market_id: i64,
    pub market_name: String,
    pub liquidated_user_id: i64,
    pub position_side: Side,
    pub quantity: i64,
    pub price: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkPriceUpdatedInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_id: Option<String>,
    pub market_id: i64,
    pub mark_price: i64,
    pub index_price: i64,
    pub source_timestamp_ms: i64,
    pub published_at_ms: i64,
    pub valid_until_ms: i64,
    pub source_sequence: i64,
    pub source_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingRateUpdatedInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_id: Option<String>,
    pub market_id: i64,
    pub funding_interval_id: String,
    pub rate: i64,
    pub rate_scale: i64,
    pub interval_start_ms: i64,
    pub interval_end_ms: i64,
    pub source_timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingSettlementTickInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_id: Option<String>,
    pub market_id: i64,
    pub funding_interval_id: String,
    pub settle_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EngineReply {
    OrderAccepted(OrderAccepted),
    OrderRejected(OrderRejected),
    CancelAccepted(CancelAccepted),
    CancelRejected(CancelRejected),
    LiquidationAccepted(LiquidationAccepted),
    LiquidationRejected(LiquidationRejected),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderAccepted {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub order_id: i64,
    pub reservation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRejected {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub reservation_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAccepted {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub order_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRejected {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub order_id: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiquidationAccepted {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub liquidation_id: String,
    pub order_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiquidationRejected {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub liquidation_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionReason {
    #[serde(rename = "TRADE")]
    TRADE,
    #[serde(rename = "LIQUIDATION")]
    LIQUIDATION,
}

fn default_execution_reason() -> ExecutionReason {
    ExecutionReason::TRADE
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EngineEvent {
    OrderOpened(OrderOpened),
    OrderCancelled(OrderCancelled),
    OrderExpired(OrderExpired),
    ReservationReleased(ReservationReleased),
    TradeExecuted(TradeExecuted),
    OrderBookDelta(OrderBookDelta),
    MarkPriceUpdated(MarkPriceUpdated),
    FundingRateUpdated(FundingRateUpdated),
    FundingPaymentApplied(FundingPaymentApplied),
    PositionChanged(PositionChanged),
    RiskStateUpdated(RiskStateUpdated),
    FeeCharged(FeeCharged),
    LiquidationStarted(LiquidationStarted),
    LiquidationExecuted(LiquidationExecuted),
    LiquidationCompleted(LiquidationCompleted),
    AdlExecuted(AdlExecuted),
    AccountDelta(AccountDelta),
    OrderBookSnapshotCreated(OrderBookSnapshotCreated),
    EngineCheckpointCommitted(EngineCheckpointCommitted),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderOpened {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub order_id: i64,
    pub reservation_id: String,
    pub user_id: i64,
    pub market_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCancelled {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub order_id: i64,
    pub reservation_id: String,
    pub user_id: i64,
    pub market_id: i64,
    pub released_amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderExpired {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub order_id: i64,
    pub reservation_id: String,
    pub user_id: i64,
    pub expired_quantity: i64,
    pub released_amount: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservationReleased {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub reservation_id: String,
    pub user_id: i64,
    pub asset: Asset,
    pub released_amount: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeExecuted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub fill_id: i64,
    pub market_id: i64,
    pub price: i64,
    pub quantity: i64,
    pub maker_order_id: i64,
    pub taker_order_id: i64,
    pub maker_user_id: i64,
    pub taker_user_id: i64,
    pub maker_reservation_id: Option<String>,
    pub taker_reservation_id: Option<String>,
    #[serde(default = "default_execution_reason")]
    pub execution_reason: ExecutionReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liquidation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liquidated_user_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_side: Option<Side>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liquidation_fee: Option<AssetAmount>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fee_deltas: Vec<FeeDelta>,
    #[serde(default)]
    pub settlements: Vec<TradeSettlement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetAmount {
    pub asset: Asset,
    pub amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeDelta {
    pub user_id: i64,
    pub asset: Asset,
    pub amount: i64,
    pub fee_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeSettlement {
    pub reservation_id: String,
    pub debit_asset: Asset,
    pub debit_amount: i64,
    pub credit_asset: Asset,
    pub credit_amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBookDelta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub market_id: i64,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBookLevel {
    pub price: i64,
    pub quantity: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkPriceUpdated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub mark_price: i64,
    pub index_price: i64,
    pub valid_until_ms: i64,
    pub source_sequence: i64,
    pub source_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingRateUpdated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub funding_interval_id: String,
    pub rate: i64,
    pub rate_scale: i64,
    pub interval_start_ms: i64,
    pub interval_end_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingPaymentApplied {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub funding_interval_id: String,
    pub payments: Vec<FundingPayment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingPayment {
    pub user_id: i64,
    pub position_id: String,
    pub side: Side,
    pub asset: Asset,
    pub amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionChanged {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub user_id: i64,
    pub position_id: String,
    pub side: Side,
    pub quantity: i64,
    pub entry_price: i64,
    pub mark_price: i64,
    pub isolated_margin: i64,
    pub realized_pnl: i64,
    pub unrealized_pnl: i64,
    pub maintenance_margin: i64,
    pub liquidation_price: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskStateUpdated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub user_id: i64,
    pub position_id: String,
    pub mark_price: i64,
    pub equity: i64,
    pub maintenance_margin: i64,
    pub margin_ratio: i64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeCharged {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub fee_id: String,
    pub user_id: i64,
    pub asset: Asset,
    pub amount: i64,
    pub fee_type: String,
    pub destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiquidationStarted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub liquidation_id: String,
    pub user_id: i64,
    pub position_id: String,
    pub side: Side,
    pub quantity: i64,
    pub mark_price: i64,
    pub maintenance_margin: i64,
    pub equity: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiquidationExecuted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub liquidation_id: String,
    pub user_id: i64,
    pub position_id: String,
    pub fill_id: i64,
    pub price: i64,
    pub quantity: i64,
    pub execution_reason: ExecutionReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiquidationCompleted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub liquidation_id: String,
    pub user_id: i64,
    pub position_id: String,
    pub final_status: String,
    pub remaining_quantity: i64,
    pub insurance_fund_delta: i64,
    pub bad_debt: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdlExecuted {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub adl_id: String,
    pub liquidation_id: String,
    pub liquidated_user_id: i64,
    pub deleveraged_user_id: i64,
    pub position_side: Side,
    pub quantity: i64,
    pub price: i64,
    pub rank: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountDelta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub account_delta_id: String,
    pub user_id: i64,
    pub asset: Asset,
    pub total_delta: i64,
    pub locked_delta: i64,
    pub reason: String,
    pub reference_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBookSnapshotCreated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_event_id: Option<String>,
    pub market_id: i64,
    pub engine_sequence: i64,
    pub engine_timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input_offset: Option<i64>,
    pub snapshot_id: String,
    pub uri: String,
    pub checksum_sha256: String,
    pub byte_size: i64,
    pub schema_version: i64,
    pub last_engine_sequence: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineCheckpointCommitted {
    pub checkpoint_id: String,
    pub engine_timestamp_ms: i64,
    pub schema_version: i64,
    pub engine_build: String,
    pub config_hash: String,
    pub engine_input_next_offset: i64,
    pub uri: String,
    pub checksum_sha256: String,
    pub byte_size: i64,
    pub market_sequences: Vec<MarketSequence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSequence {
    pub market_id: i64,
    pub engine_sequence: i64,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        fs,
        path::{Path, PathBuf},
    };

    use serde_json::Value;

    use super::*;

    #[test]
    fn engine_input_topic_is_target_topic_with_legacy_alias() {
        assert_eq!(ENGINE_INPUT_TOPIC, "engine.input");
        assert_eq!(ENGINE_COMMANDS_TOPIC, ENGINE_INPUT_TOPIC);
        assert_eq!(ENGINE_COMMANDS_LEGACY_TOPIC, "engine.commands");
    }

    #[test]
    fn trade_executed_defaults_missing_settlements() {
        let event = serde_json::from_str::<EngineEvent>(
            r#"{
                "type":"TradeExecuted",
                "payload":{
                    "engine_sequence":1,
                    "engine_timestamp_ms":1710000000000,
                    "fill_id":1,
                    "market_id":2,
                    "price":100,
                    "quantity":5,
                    "maker_order_id":10,
                    "taker_order_id":11,
                    "maker_user_id":42,
                    "taker_user_id":43,
                    "maker_reservation_id":"res-maker",
                    "taker_reservation_id":"res-taker"
                }
            }"#,
        )
        .expect("legacy trade event should deserialize");

        match event {
            EngineEvent::TradeExecuted(trade) => {
                assert_eq!(trade.execution_reason, ExecutionReason::TRADE);
                assert!(trade.settlements.is_empty());
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn place_order_defaults_target_margin_fields_for_legacy_json() {
        let input = serde_json::from_str::<EngineInput>(
            r#"{
                "type":"PlaceOrder",
                "payload":{
                    "envelope":{
                        "request_id":"req-1",
                        "idempotency_key":"order-1",
                        "user_id":42,
                        "reply_partition":0
                    },
                    "reservation_id":"res-1",
                    "order_id":9001,
                    "market_id":1,
                    "market_name":"SOL-PERP",
                    "side":"LONG",
                    "order_type":"LIMIT",
                    "quantity":10,
                    "price":20,
                    "reduce_only":false
                }
            }"#,
        )
        .expect("legacy place order should deserialize");

        match input {
            EngineInput::PlaceOrder(order) => {
                assert_eq!(order.input_id, None);
                assert_eq!(order.order_id, 9001);
                assert_eq!(order.margin_asset, Asset::USDC);
                assert_eq!(order.reserved_margin_amount, 0);
                assert_eq!(order.leverage, 1);
            }
            other => panic!("unexpected input: {other:?}"),
        }
    }

    #[test]
    fn all_engine_fixtures_match_protocol_contract() {
        let mut checked = 0;

        for path in engine_fixture_paths() {
            let file_name = path
                .file_name()
                .and_then(|file_name| file_name.to_str())
                .expect("fixture filename should be utf-8");
            let json = fs::read_to_string(&path).expect("fixture should be readable");

            if file_name.ends_with(".command.json") || file_name.ends_with(".input.json") {
                assert_input_fixture(&json);
            } else if file_name.ends_with(".reply.json") {
                assert_reply_fixture(&json);
            } else if file_name.ends_with(".event.json") {
                assert_event_fixture(&json);
            } else {
                panic!("fixture filename does not declare protocol stream kind: {file_name}");
            }

            checked += 1;
        }

        assert_eq!(checked, 33, "unexpected number of engine JSON fixtures");
    }

    #[test]
    fn engine_fixtures_carry_conformance_metadata() {
        let mut input_fixtures = 0;
        let mut reply_fixtures = 0;
        let mut event_fixtures = 0;
        let mut checkpoint_next_offsets = Vec::new();
        let mut event_ids = BTreeSet::new();
        let mut market_sequences = BTreeSet::new();
        let mut max_source_input_offset = None::<i64>;

        for path in engine_fixture_paths() {
            let file_name = path
                .file_name()
                .and_then(|file_name| file_name.to_str())
                .expect("fixture filename should be utf-8");
            let json = fs::read_to_string(&path).expect("fixture should be readable");
            let fixture = serde_json::from_str::<Value>(&json).expect("fixture should be JSON");

            assert_message_shape(file_name, &fixture);
            let payload = fixture_payload(file_name, &fixture);

            if file_name.ends_with(".command.json") || file_name.ends_with(".input.json") {
                input_fixtures += 1;
                assert_input_metadata(file_name, payload);
            } else if file_name.ends_with(".reply.json") {
                reply_fixtures += 1;
                assert_reply_metadata(file_name, payload, &mut max_source_input_offset);
            } else if file_name.ends_with(".event.json") {
                event_fixtures += 1;
                let event_type = non_empty_string(file_name, &fixture, "type");
                if event_type == "EngineCheckpointCommitted" {
                    assert_checkpoint_metadata(file_name, payload, &mut checkpoint_next_offsets);
                } else {
                    assert_market_event_metadata(
                        file_name,
                        event_type,
                        payload,
                        &mut event_ids,
                        &mut market_sequences,
                        &mut max_source_input_offset,
                    );
                }
            } else {
                panic!("fixture filename does not declare protocol stream kind: {file_name}");
            }
        }

        assert_eq!(input_fixtures, 7, "unexpected engine input fixture count");
        assert_eq!(reply_fixtures, 6, "unexpected engine reply fixture count");
        assert_eq!(event_fixtures, 20, "unexpected engine event fixture count");
        assert_eq!(
            checkpoint_next_offsets.len(),
            1,
            "expected one engine checkpoint fixture"
        );

        let checkpoint_next_offset = checkpoint_next_offsets[0];
        let max_source_input_offset =
            max_source_input_offset.expect("fixtures should carry source input offsets");
        assert!(
            checkpoint_next_offset > max_source_input_offset,
            "checkpoint next offset {checkpoint_next_offset} must be after fixture source offset {max_source_input_offset}"
        );
    }

    fn assert_input_fixture(json: &str) {
        let parsed = serde_json::from_str::<EngineInput>(json)
            .expect("input fixture should match EngineInput");
        assert_fixture_round_trips(json, &parsed);
    }

    fn assert_reply_fixture(json: &str) {
        let parsed = serde_json::from_str::<EngineReply>(json)
            .expect("reply fixture should match EngineReply");
        assert_fixture_round_trips(json, &parsed);
    }

    fn assert_event_fixture(json: &str) {
        let parsed = serde_json::from_str::<EngineEvent>(json)
            .expect("event fixture should match EngineEvent");
        assert_fixture_round_trips(json, &parsed);
    }

    fn assert_fixture_round_trips<T>(json: &str, parsed: &T)
    where
        T: serde::Serialize,
    {
        let fixture = serde_json::from_str::<Value>(json).expect("fixture should be valid JSON");
        let serialized = serde_json::to_value(parsed).expect("protocol value should serialize");

        assert_eq!(serialized, fixture);
    }

    fn engine_fixture_paths() -> Vec<PathBuf> {
        let examples_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/examples");
        let mut paths = Vec::new();

        for entry in fs::read_dir(&examples_dir).expect("docs/examples should be readable") {
            let entry = entry.expect("example directory entry should be readable");
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
                paths.push(path);
            }
        }

        paths.sort();
        paths
    }

    fn assert_message_shape(file_name: &str, fixture: &Value) {
        non_empty_string(file_name, fixture, "type");
        fixture_payload(file_name, fixture);
    }

    fn assert_input_metadata(file_name: &str, payload: &Value) {
        non_empty_string(file_name, payload, "input_id");
        positive_i64(file_name, payload, "market_id");

        if file_name.ends_with(".command.json") {
            let envelope = payload
                .get("envelope")
                .filter(|value| value.is_object())
                .unwrap_or_else(|| panic!("{file_name} payload.envelope must be an object"));

            non_empty_string(file_name, envelope, "request_id");
            non_empty_string(file_name, envelope, "idempotency_key");
            non_negative_i64(file_name, envelope, "user_id");
            non_negative_i64(file_name, envelope, "reply_partition");
        }
    }

    fn assert_reply_metadata(
        file_name: &str,
        payload: &Value,
        max_source_input_offset: &mut Option<i64>,
    ) {
        non_empty_string(file_name, payload, "request_id");
        non_empty_string(file_name, payload, "source_input_id");
        let source_input_offset = non_negative_i64(file_name, payload, "source_input_offset");
        update_max_source_input_offset(max_source_input_offset, source_input_offset);
    }

    fn assert_market_event_metadata(
        file_name: &str,
        event_type: &str,
        payload: &Value,
        event_ids: &mut BTreeSet<String>,
        market_sequences: &mut BTreeSet<(i64, i64)>,
        max_source_input_offset: &mut Option<i64>,
    ) {
        let engine_event_id = non_empty_string(file_name, payload, "engine_event_id");
        assert!(
            event_ids.insert(engine_event_id.to_owned()),
            "{file_name} duplicates engine_event_id {engine_event_id}"
        );

        let market_id = positive_i64(file_name, payload, "market_id");
        let engine_sequence = positive_i64(file_name, payload, "engine_sequence");
        assert!(
            market_sequences.insert((market_id, engine_sequence)),
            "{file_name} duplicates engine_sequence {engine_sequence} for market_id {market_id}"
        );

        positive_i64(file_name, payload, "engine_timestamp_ms");
        non_empty_string(file_name, payload, "source_input_id");
        let source_input_offset = non_negative_i64(file_name, payload, "source_input_offset");
        update_max_source_input_offset(max_source_input_offset, source_input_offset);

        if event_type == "OrderBookSnapshotCreated" {
            assert_eq!(
                positive_i64(file_name, payload, "last_engine_sequence"),
                engine_sequence,
                "{file_name} snapshot should advertise the latest applied engine sequence"
            );
        }
    }

    fn assert_checkpoint_metadata(
        file_name: &str,
        payload: &Value,
        checkpoint_next_offsets: &mut Vec<i64>,
    ) {
        non_empty_string(file_name, payload, "checkpoint_id");
        positive_i64(file_name, payload, "engine_timestamp_ms");
        positive_i64(file_name, payload, "schema_version");
        non_empty_string(file_name, payload, "engine_build");
        non_empty_string(file_name, payload, "config_hash");
        non_empty_string(file_name, payload, "uri");
        non_empty_string(file_name, payload, "checksum_sha256");
        positive_i64(file_name, payload, "byte_size");

        let engine_input_next_offset = positive_i64(file_name, payload, "engine_input_next_offset");
        checkpoint_next_offsets.push(engine_input_next_offset);

        let market_sequences = payload
            .get("market_sequences")
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("{file_name} payload.market_sequences must be an array"));
        assert!(
            !market_sequences.is_empty(),
            "{file_name} checkpoint must carry per-market engine sequences"
        );

        let mut market_ids = BTreeSet::new();
        for market_sequence in market_sequences {
            let market_id = positive_i64(file_name, market_sequence, "market_id");
            assert!(
                market_ids.insert(market_id),
                "{file_name} checkpoint duplicates market_id {market_id}"
            );
            positive_i64(file_name, market_sequence, "engine_sequence");
        }
    }

    fn fixture_payload<'a>(file_name: &str, fixture: &'a Value) -> &'a Value {
        fixture
            .get("payload")
            .filter(|value| value.is_object())
            .unwrap_or_else(|| panic!("{file_name} payload must be an object"))
    }

    fn non_empty_string<'a>(file_name: &str, value: &'a Value, field: &str) -> &'a str {
        let text = value
            .get(field)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{file_name} {field} must be a string"));
        assert!(!text.is_empty(), "{file_name} {field} must not be empty");
        text
    }

    fn positive_i64(file_name: &str, value: &Value, field: &str) -> i64 {
        let number = value
            .get(field)
            .and_then(Value::as_i64)
            .unwrap_or_else(|| panic!("{file_name} {field} must be an integer"));
        assert!(number > 0, "{file_name} {field} must be positive");
        number
    }

    fn non_negative_i64(file_name: &str, value: &Value, field: &str) -> i64 {
        let number = value
            .get(field)
            .and_then(Value::as_i64)
            .unwrap_or_else(|| panic!("{file_name} {field} must be an integer"));
        assert!(number >= 0, "{file_name} {field} must be non-negative");
        number
    }

    fn update_max_source_input_offset(max_source_input_offset: &mut Option<i64>, offset: i64) {
        *max_source_input_offset = Some(match *max_source_input_offset {
            Some(current) => current.max(offset),
            None => offset,
        });
    }
}
