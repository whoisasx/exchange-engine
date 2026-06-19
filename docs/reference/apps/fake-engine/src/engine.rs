use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use protocol::{
    common::{Asset, Side},
    engine::{
        CancelAccepted, CancelRejected, EngineCommand, EngineEvent, EngineReply, ExecutionReason,
        FundingPaymentApplied, FundingRateUpdated as FundingRateUpdatedEvent,
        FundingRateUpdatedInput, FundingSettlementTickInput, LiquidatePosition,
        LiquidationAccepted, LiquidationRejected, MarkPriceUpdated as MarkPriceUpdatedEvent,
        MarkPriceUpdatedInput, OrderAccepted, OrderBookDelta, OrderBookLevel, OrderCancelled,
        OrderOpened, OrderRejected, ReservedPlaceOrder, TradeExecuted, TradeSettlement,
    },
    wallet::{WalletEvent, WalletFundsReserved},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyPublication {
    pub partition: i32,
    pub key: String,
    pub reply: EngineReply,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPublication {
    pub key: String,
    pub event: EngineEvent,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FakeEngineOutput {
    pub replies: Vec<ReplyPublication>,
    pub events: Vec<EventPublication>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct InputMetadata {
    input_id: Option<String>,
    input_offset: Option<i64>,
}

impl InputMetadata {
    fn from_input(input: &EngineCommand, input_offset: Option<i64>) -> Self {
        let input_id = match input {
            EngineCommand::PlaceOrder(order) => order.input_id.clone(),
            EngineCommand::CancelOrder(cancel) => cancel.input_id.clone(),
            EngineCommand::LiquidatePosition(liquidation) => liquidation.input_id.clone(),
            EngineCommand::MarkPriceUpdated(mark) => mark.input_id.clone(),
            EngineCommand::FundingRateUpdated(funding) => funding.input_id.clone(),
            EngineCommand::FundingSettlementTick(tick) => tick.input_id.clone(),
        };

        Self {
            input_id,
            input_offset,
        }
    }

    fn source_input_id(&self) -> Option<String> {
        self.input_id.clone()
    }

    fn source_input_offset(&self) -> Option<i64> {
        self.input_offset
    }
}

#[derive(Debug, Clone)]
pub struct FakeEngine {
    state: Arc<Mutex<EngineState>>,
}

impl FakeEngine {
    pub fn new(order_id_start: i64, fill_id_start: i64) -> Self {
        Self {
            state: Arc::new(Mutex::new(EngineState {
                next_order_id: order_id_start,
                next_fill_id: fill_id_start,
                ..EngineState::default()
            })),
        }
    }

    pub fn observe_wallet_event(&self, event: WalletEvent) {
        if let WalletEvent::FundsReserved(event) = event {
            self.state
                .lock()
                .expect("fake engine state poisoned")
                .record_reservation(event);
        }
    }

    pub fn process_command(&self, command: EngineCommand) -> FakeEngineOutput {
        self.process_input(command, None)
    }

    pub fn process_input(
        &self,
        input: EngineCommand,
        source_input_offset: Option<i64>,
    ) -> FakeEngineOutput {
        let metadata = InputMetadata::from_input(&input, source_input_offset);

        match input {
            EngineCommand::PlaceOrder(order) => self.process_place_order(order, &metadata),
            EngineCommand::LiquidatePosition(liquidation) => {
                self.process_liquidate_position(liquidation, &metadata)
            }
            EngineCommand::MarkPriceUpdated(input) => {
                self.process_mark_price_updated(input, &metadata)
            }
            EngineCommand::FundingRateUpdated(input) => {
                self.process_funding_rate_updated(input, &metadata)
            }
            EngineCommand::FundingSettlementTick(input) => {
                self.process_funding_settlement_tick(input, &metadata)
            }
            EngineCommand::CancelOrder(cancel) => {
                let mut state = self.state.lock().expect("fake engine state poisoned");

                if let Some(order) = state.cancel_order(cancel.order_id, cancel.market_id) {
                    let level_quantity =
                        state.aggregate_quantity(order.market_id, order.side, order.price);
                    let events = vec![
                        EventPublication {
                            key: cancel.market_id.to_string(),
                            event: EngineEvent::OrderCancelled(OrderCancelled {
                                engine_event_id: None,
                                engine_sequence: state.next_engine_sequence(order.market_id),
                                engine_timestamp_ms: unix_timestamp_ms(),
                                source_input_id: metadata.source_input_id(),
                                source_input_offset: metadata.source_input_offset(),
                                order_id: order.order_id,
                                reservation_id: order.reservation_id,
                                user_id: order.user_id,
                                market_id: order.market_id,
                                released_amount: order.margin_remaining.max(1),
                            }),
                        },
                        EventPublication {
                            key: cancel.market_id.to_string(),
                            event: state.orderbook_delta_for_level(
                                order.market_id,
                                order.side,
                                order.price,
                                level_quantity,
                                &metadata,
                            ),
                        },
                    ];

                    FakeEngineOutput {
                        replies: vec![ReplyPublication {
                            partition: cancel.envelope.reply_partition,
                            key: cancel.envelope.request_id.clone(),
                            reply: EngineReply::CancelAccepted(CancelAccepted {
                                request_id: cancel.envelope.request_id,
                                source_input_id: metadata.source_input_id(),
                                source_input_offset: metadata.source_input_offset(),
                                order_id: cancel.order_id,
                            }),
                        }],
                        events,
                    }
                } else {
                    FakeEngineOutput {
                        replies: vec![ReplyPublication {
                            partition: cancel.envelope.reply_partition,
                            key: cancel.envelope.request_id.clone(),
                            reply: EngineReply::CancelRejected(CancelRejected {
                                request_id: cancel.envelope.request_id,
                                source_input_id: metadata.source_input_id(),
                                source_input_offset: metadata.source_input_offset(),
                                order_id: cancel.order_id,
                                reason: String::from("order is not resting in fake engine"),
                            }),
                        }],
                        events: Vec::new(),
                    }
                }
            }
        }
    }

    fn process_place_order(
        &self,
        order: ReservedPlaceOrder,
        metadata: &InputMetadata,
    ) -> FakeEngineOutput {
        if order.quantity <= 0
            || order.price < 0
            || order.reserved_margin_amount < 0
            || order.leverage <= 0
        {
            return FakeEngineOutput {
                replies: vec![ReplyPublication {
                    partition: order.envelope.reply_partition,
                    key: order.envelope.request_id.clone(),
                    reply: EngineReply::OrderRejected(OrderRejected {
                        request_id: order.envelope.request_id,
                        source_input_id: metadata.source_input_id(),
                        source_input_offset: metadata.source_input_offset(),
                        reservation_id: Some(order.reservation_id),
                        reason: String::from("invalid price, quantity, or margin metadata"),
                    }),
                }],
                events: Vec::new(),
            };
        }

        let mut state = self.state.lock().expect("fake engine state poisoned");
        let order_id = state.next_order_id();
        let mut incoming = state.resting_order_from_command(order_id, &order);
        let mut output = FakeEngineOutput {
            replies: vec![ReplyPublication {
                partition: order.envelope.reply_partition,
                key: order.envelope.request_id.clone(),
                reply: EngineReply::OrderAccepted(OrderAccepted {
                    request_id: order.envelope.request_id,
                    source_input_id: metadata.source_input_id(),
                    source_input_offset: metadata.source_input_offset(),
                    order_id,
                    reservation_id: order.reservation_id.clone(),
                }),
            }],
            events: Vec::new(),
        };

        if let Some(maker_order_id) = state.matching_order_id(&incoming) {
            let mut maker = state
                .resting_orders
                .remove(&maker_order_id)
                .expect("matching order should exist");
            let maker_market_id = maker.market_id;
            let maker_side = maker.side;
            let maker_price = maker.price;
            let fill =
                state.execute_fill(&mut maker, &mut incoming, ExecutionReason::TRADE, metadata);

            output.events.push(EventPublication {
                key: fill.market_id.to_string(),
                event: EngineEvent::TradeExecuted(fill),
            });

            if maker.remaining_quantity > 0 {
                state.resting_orders.insert(maker.order_id, maker);
            }

            let level_quantity = state.aggregate_quantity(maker_market_id, maker_side, maker_price);
            output.events.push(EventPublication {
                key: maker_market_id.to_string(),
                event: state.orderbook_delta_for_level(
                    maker_market_id,
                    maker_side,
                    maker_price,
                    level_quantity,
                    metadata,
                ),
            });
        }

        if incoming.remaining_quantity > 0 && incoming.reduce_only {
            return output;
        }

        if incoming.remaining_quantity > 0 {
            let incoming_market_id = incoming.market_id;
            let incoming_side = incoming.side;
            let incoming_price = incoming.price;
            output.events.push(EventPublication {
                key: incoming_market_id.to_string(),
                event: EngineEvent::OrderOpened(OrderOpened {
                    engine_event_id: None,
                    engine_sequence: state.next_engine_sequence(incoming_market_id),
                    engine_timestamp_ms: unix_timestamp_ms(),
                    source_input_id: metadata.source_input_id(),
                    source_input_offset: metadata.source_input_offset(),
                    order_id: incoming.order_id,
                    reservation_id: incoming.reservation_id.clone(),
                    user_id: incoming.user_id,
                    market_id: incoming_market_id,
                }),
            });
            state.resting_orders.insert(incoming.order_id, incoming);
            let level_quantity =
                state.aggregate_quantity(incoming_market_id, incoming_side, incoming_price);
            output.events.push(EventPublication {
                key: incoming_market_id.to_string(),
                event: state.orderbook_delta_for_level(
                    incoming_market_id,
                    incoming_side,
                    incoming_price,
                    level_quantity,
                    metadata,
                ),
            });
        }

        output
    }

    fn process_liquidate_position(
        &self,
        liquidation: LiquidatePosition,
        metadata: &InputMetadata,
    ) -> FakeEngineOutput {
        if liquidation.quantity <= 0
            || liquidation.price < 0
            || liquidation.market_id <= 0
            || liquidation.liquidated_user_id <= 0
        {
            return FakeEngineOutput {
                replies: vec![ReplyPublication {
                    partition: liquidation.envelope.reply_partition,
                    key: liquidation.envelope.request_id.clone(),
                    reply: EngineReply::LiquidationRejected(LiquidationRejected {
                        request_id: liquidation.envelope.request_id,
                        source_input_id: metadata.source_input_id(),
                        source_input_offset: metadata.source_input_offset(),
                        liquidation_id: liquidation.liquidation_id,
                        reason: String::from("invalid liquidation command"),
                    }),
                }],
                events: Vec::new(),
            };
        }

        let mut state = self.state.lock().expect("fake engine state poisoned");
        let order_id = state.next_order_id();
        let mut incoming = state.liquidation_order_from_command(order_id, &liquidation);
        let mut output = FakeEngineOutput {
            replies: vec![ReplyPublication {
                partition: liquidation.envelope.reply_partition,
                key: liquidation.envelope.request_id.clone(),
                reply: EngineReply::LiquidationAccepted(LiquidationAccepted {
                    request_id: liquidation.envelope.request_id,
                    source_input_id: metadata.source_input_id(),
                    source_input_offset: metadata.source_input_offset(),
                    liquidation_id: liquidation.liquidation_id.clone(),
                    order_id,
                }),
            }],
            events: Vec::new(),
        };

        if let Some(maker_order_id) = state.matching_order_id(&incoming) {
            let mut maker = state
                .resting_orders
                .remove(&maker_order_id)
                .expect("matching order should exist");
            let maker_market_id = maker.market_id;
            let maker_side = maker.side;
            let maker_price = maker.price;
            let fill = state.execute_fill(
                &mut maker,
                &mut incoming,
                ExecutionReason::LIQUIDATION,
                metadata,
            );

            output.events.push(EventPublication {
                key: fill.market_id.to_string(),
                event: EngineEvent::TradeExecuted(fill),
            });

            if maker.remaining_quantity > 0 {
                state.resting_orders.insert(maker.order_id, maker);
            }

            let level_quantity = state.aggregate_quantity(maker_market_id, maker_side, maker_price);
            output.events.push(EventPublication {
                key: maker_market_id.to_string(),
                event: state.orderbook_delta_for_level(
                    maker_market_id,
                    maker_side,
                    maker_price,
                    level_quantity,
                    metadata,
                ),
            });
        }

        output
    }

    fn process_mark_price_updated(
        &self,
        input: MarkPriceUpdatedInput,
        metadata: &InputMetadata,
    ) -> FakeEngineOutput {
        let mut state = self.state.lock().expect("fake engine state poisoned");
        let market_id = input.market_id;

        FakeEngineOutput {
            replies: Vec::new(),
            events: vec![EventPublication {
                key: market_id.to_string(),
                event: EngineEvent::MarkPriceUpdated(MarkPriceUpdatedEvent {
                    engine_event_id: None,
                    market_id,
                    engine_sequence: state.next_engine_sequence(market_id),
                    engine_timestamp_ms: unix_timestamp_ms(),
                    source_input_id: metadata.source_input_id(),
                    source_input_offset: metadata.source_input_offset(),
                    mark_price: input.mark_price,
                    index_price: input.index_price,
                    valid_until_ms: input.valid_until_ms,
                    source_sequence: input.source_sequence,
                    source_status: input.source_status,
                }),
            }],
        }
    }

    fn process_funding_rate_updated(
        &self,
        input: FundingRateUpdatedInput,
        metadata: &InputMetadata,
    ) -> FakeEngineOutput {
        let mut state = self.state.lock().expect("fake engine state poisoned");
        let market_id = input.market_id;

        FakeEngineOutput {
            replies: Vec::new(),
            events: vec![EventPublication {
                key: market_id.to_string(),
                event: EngineEvent::FundingRateUpdated(FundingRateUpdatedEvent {
                    engine_event_id: None,
                    market_id,
                    engine_sequence: state.next_engine_sequence(market_id),
                    engine_timestamp_ms: unix_timestamp_ms(),
                    source_input_id: metadata.source_input_id(),
                    source_input_offset: metadata.source_input_offset(),
                    funding_interval_id: input.funding_interval_id,
                    rate: input.rate,
                    rate_scale: input.rate_scale,
                    interval_start_ms: input.interval_start_ms,
                    interval_end_ms: input.interval_end_ms,
                }),
            }],
        }
    }

    fn process_funding_settlement_tick(
        &self,
        input: FundingSettlementTickInput,
        metadata: &InputMetadata,
    ) -> FakeEngineOutput {
        let mut state = self.state.lock().expect("fake engine state poisoned");
        let market_id = input.market_id;

        FakeEngineOutput {
            replies: Vec::new(),
            events: vec![EventPublication {
                key: market_id.to_string(),
                event: EngineEvent::FundingPaymentApplied(FundingPaymentApplied {
                    engine_event_id: None,
                    market_id,
                    engine_sequence: state.next_engine_sequence(market_id),
                    engine_timestamp_ms: unix_timestamp_ms(),
                    source_input_id: metadata.source_input_id(),
                    source_input_offset: metadata.source_input_offset(),
                    funding_interval_id: input.funding_interval_id,
                    payments: Vec::new(),
                }),
            }],
        }
    }
}

#[derive(Debug, Default)]
struct EngineState {
    next_order_id: i64,
    next_fill_id: i64,
    market_sequences: HashMap<i64, i64>,
    reservations: HashMap<String, ReservationInfo>,
    resting_orders: HashMap<i64, RestingOrder>,
}

impl EngineState {
    fn next_order_id(&mut self) -> i64 {
        let order_id = self.next_order_id;
        self.next_order_id += 1;
        order_id
    }

    fn next_fill_id(&mut self) -> i64 {
        let fill_id = self.next_fill_id;
        self.next_fill_id += 1;
        fill_id
    }

    fn next_engine_sequence(&mut self, market_id: i64) -> i64 {
        let sequence = self.market_sequences.entry(market_id).or_insert(0);
        *sequence += 1;
        *sequence
    }

    fn record_reservation(&mut self, event: WalletFundsReserved) {
        self.reservations.insert(
            event.reservation_id.clone(),
            ReservationInfo {
                asset: event.asset,
                remaining: event.amount,
            },
        );

        for order in self.resting_orders.values_mut() {
            if order.reservation_id == event.reservation_id {
                order.margin_asset = event.asset;
                order.margin_remaining = event.amount;
            }
        }
    }

    fn resting_order_from_command(
        &self,
        order_id: i64,
        command: &ReservedPlaceOrder,
    ) -> RestingOrder {
        let (margin_asset, margin_remaining) = match self.reservations.get(&command.reservation_id)
        {
            Some(reservation) => (reservation.asset, reservation.remaining.max(1)),
            None => (
                command.margin_asset,
                fallback_reserved_margin_amount(command),
            ),
        };

        RestingOrder {
            order_id,
            reservation_id: command.reservation_id.clone(),
            user_id: command.envelope.user_id,
            market_id: command.market_id,
            side: command.side,
            remaining_quantity: command.quantity,
            price: command.price,
            reduce_only: command.reduce_only,
            settle_on_fill: true,
            margin_asset,
            margin_remaining,
        }
    }

    fn liquidation_order_from_command(
        &self,
        order_id: i64,
        command: &LiquidatePosition,
    ) -> RestingOrder {
        RestingOrder {
            order_id,
            reservation_id: command.liquidation_id.clone(),
            user_id: command.liquidated_user_id,
            market_id: command.market_id,
            side: opposite_side(command.position_side),
            remaining_quantity: command.quantity,
            price: command.price,
            reduce_only: true,
            settle_on_fill: false,
            margin_asset: Asset::USDC,
            margin_remaining: 0,
        }
    }

    fn matching_order_id(&self, incoming: &RestingOrder) -> Option<i64> {
        self.resting_orders
            .iter()
            .filter(|(_, order)| {
                order.market_id == incoming.market_id && order.side != incoming.side
            })
            .filter(|(_, order)| prices_cross(order, incoming))
            .map(|(order_id, _)| *order_id)
            .min()
    }

    fn execute_fill(
        &mut self,
        maker: &mut RestingOrder,
        taker: &mut RestingOrder,
        execution_reason: ExecutionReason,
        metadata: &InputMetadata,
    ) -> TradeExecuted {
        let fill_quantity = maker.remaining_quantity.min(taker.remaining_quantity);
        let fill_price = if maker.price > 0 {
            maker.price
        } else {
            taker.price
        };
        let maker_debit = fill_margin(
            maker.margin_remaining,
            maker.remaining_quantity,
            fill_quantity,
        );
        let taker_debit = fill_margin(
            taker.margin_remaining,
            taker.remaining_quantity,
            fill_quantity,
        );

        maker.remaining_quantity -= fill_quantity;
        taker.remaining_quantity -= fill_quantity;
        maker.margin_remaining = (maker.margin_remaining - maker_debit).max(0);
        taker.margin_remaining = (taker.margin_remaining - taker_debit).max(0);
        self.decrease_reservation(&maker.reservation_id, maker_debit);
        self.decrease_reservation(&taker.reservation_id, taker_debit);

        let mut settlements = Vec::new();
        if maker.settle_on_fill {
            settlements.push(settlement_for(maker, maker_debit));
        }
        if taker.settle_on_fill {
            settlements.push(settlement_for(taker, taker_debit));
        }
        let is_liquidation = execution_reason == ExecutionReason::LIQUIDATION;

        TradeExecuted {
            engine_event_id: None,
            engine_sequence: self.next_engine_sequence(maker.market_id),
            engine_timestamp_ms: unix_timestamp_ms(),
            source_input_id: metadata.source_input_id(),
            source_input_offset: metadata.source_input_offset(),
            fill_id: self.next_fill_id(),
            market_id: maker.market_id,
            price: fill_price,
            quantity: fill_quantity,
            maker_order_id: maker.order_id,
            taker_order_id: taker.order_id,
            maker_user_id: maker.user_id,
            taker_user_id: taker.user_id,
            maker_reservation_id: Some(maker.reservation_id.clone()),
            taker_reservation_id: Some(taker.reservation_id.clone()),
            execution_reason,
            liquidation_id: is_liquidation.then(|| taker.reservation_id.clone()),
            liquidated_user_id: is_liquidation.then_some(taker.user_id),
            position_side: is_liquidation.then_some(opposite_side(taker.side)),
            liquidation_fee: None,
            fee_deltas: Vec::new(),
            settlements,
        }
    }

    fn decrease_reservation(&mut self, reservation_id: &str, amount: i64) {
        if let Some(reservation) = self.reservations.get_mut(reservation_id) {
            reservation.remaining = (reservation.remaining - amount).max(0);
        }
    }

    fn cancel_order(&mut self, order_id: i64, market_id: i64) -> Option<RestingOrder> {
        let order = self.resting_orders.get(&order_id)?;
        if order.market_id != market_id {
            return None;
        }

        let order = self.resting_orders.remove(&order_id)?;
        self.decrease_reservation(&order.reservation_id, order.margin_remaining);
        Some(order)
    }

    fn aggregate_quantity(&self, market_id: i64, side: Side, price: i64) -> i64 {
        self.resting_orders
            .values()
            .filter(|order| {
                order.market_id == market_id && order.side == side && order.price == price
            })
            .map(|order| order.remaining_quantity)
            .sum()
    }

    fn orderbook_delta_for_level(
        &mut self,
        market_id: i64,
        side: Side,
        price: i64,
        quantity: i64,
        metadata: &InputMetadata,
    ) -> EngineEvent {
        let level = OrderBookLevel { price, quantity };
        let (bids, asks) = match side {
            Side::LONG => (vec![level], Vec::new()),
            Side::SHORT => (Vec::new(), vec![level]),
        };

        EngineEvent::OrderBookDelta(OrderBookDelta {
            engine_event_id: None,
            engine_sequence: self.next_engine_sequence(market_id),
            engine_timestamp_ms: unix_timestamp_ms(),
            source_input_id: metadata.source_input_id(),
            source_input_offset: metadata.source_input_offset(),
            market_id,
            bids,
            asks,
        })
    }
}

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before UNIX_EPOCH")
        .as_millis() as i64
}

fn fallback_reserved_margin_amount(command: &ReservedPlaceOrder) -> i64 {
    if command.reserved_margin_amount > 0 {
        command.reserved_margin_amount
    } else if command.reduce_only {
        0
    } else {
        1
    }
}

#[derive(Debug, Clone, Copy)]
struct ReservationInfo {
    asset: Asset,
    remaining: i64,
}

#[derive(Debug, Clone)]
struct RestingOrder {
    order_id: i64,
    reservation_id: String,
    user_id: i64,
    market_id: i64,
    side: Side,
    remaining_quantity: i64,
    price: i64,
    reduce_only: bool,
    settle_on_fill: bool,
    margin_asset: Asset,
    margin_remaining: i64,
}

fn opposite_side(side: Side) -> Side {
    match side {
        Side::LONG => Side::SHORT,
        Side::SHORT => Side::LONG,
    }
}

fn prices_cross(maker: &RestingOrder, taker: &RestingOrder) -> bool {
    if maker.price == 0 || taker.price == 0 {
        return true;
    }

    match taker.side {
        Side::LONG => taker.price >= maker.price,
        Side::SHORT => taker.price <= maker.price,
    }
}

fn fill_margin(margin_remaining: i64, quantity_remaining: i64, fill_quantity: i64) -> i64 {
    if margin_remaining <= 0 {
        return 0;
    }
    if quantity_remaining <= 0 || fill_quantity >= quantity_remaining {
        return margin_remaining;
    }

    let proportional =
        ((margin_remaining as i128 * fill_quantity as i128) / quantity_remaining as i128) as i64;
    proportional.clamp(1, margin_remaining)
}

fn settlement_for(order: &RestingOrder, debit_amount: i64) -> TradeSettlement {
    TradeSettlement {
        reservation_id: order.reservation_id.clone(),
        debit_asset: order.margin_asset,
        debit_amount: debit_amount.max(1),
        credit_asset: order.margin_asset,
        credit_amount: debit_amount.max(1),
    }
}

#[cfg(test)]
mod tests {
    use protocol::{
        common::{CommandEnvelope, OrderType},
        engine::{
            CancelOrder, FundingRateUpdatedInput, FundingSettlementTickInput, LiquidatePosition,
            MarkPriceUpdatedInput,
        },
        wallet::WalletFundsReserved,
    };

    use super::*;

    #[test]
    fn place_order_accepts_and_opens_first_order() {
        let engine = FakeEngine::new(100, 200);
        engine.observe_wallet_event(WalletEvent::FundsReserved(WalletFundsReserved {
            request_id: String::from("req-1"),
            user_id: 42,
            reservation_id: String::from("res-1"),
            asset: Asset::USDC,
            amount: 500,
        }));

        let output = engine.process_command(EngineCommand::PlaceOrder(order(
            "req-1",
            "res-1",
            42,
            Side::LONG,
            10,
            100,
        )));

        assert!(matches!(
            output.replies[0].reply,
            EngineReply::OrderAccepted(OrderAccepted { order_id: 100, .. })
        ));
        assert!(matches!(
            output.events[0].event,
            EngineEvent::OrderOpened(OrderOpened {
                engine_sequence: 1,
                order_id: 100,
                ..
            })
        ));
        assert!(matches!(
            output.events[1].event,
            EngineEvent::OrderBookDelta(OrderBookDelta {
                engine_sequence: 2,
                market_id: 1,
                ..
            })
        ));
    }

    #[test]
    fn opposite_order_matches_resting_order_and_emits_trade() {
        let engine = FakeEngine::new(100, 200);
        reserve(&engine, "req-1", "res-maker", 1000);
        reserve(&engine, "req-2", "res-taker", 1000);
        let _ = engine.process_command(EngineCommand::PlaceOrder(order(
            "req-1",
            "res-maker",
            1,
            Side::LONG,
            10,
            100,
        )));

        let output = engine.process_command(EngineCommand::PlaceOrder(order(
            "req-2",
            "res-taker",
            2,
            Side::SHORT,
            10,
            100,
        )));

        assert!(matches!(
            output.replies[0].reply,
            EngineReply::OrderAccepted(OrderAccepted { order_id: 101, .. })
        ));

        let EngineEvent::TradeExecuted(trade) = &output.events[0].event else {
            panic!("expected trade event");
        };

        assert_eq!(trade.fill_id, 200);
        assert_eq!(trade.engine_sequence, 3);
        assert!(trade.engine_timestamp_ms > 0);
        assert_eq!(trade.maker_order_id, 100);
        assert_eq!(trade.taker_order_id, 101);
        assert_eq!(trade.maker_user_id, 1);
        assert_eq!(trade.taker_user_id, 2);
        assert_eq!(trade.quantity, 10);
        assert_eq!(trade.execution_reason, ExecutionReason::TRADE);
        assert_eq!(trade.settlements.len(), 2);
        assert_eq!(trade.settlements[0].debit_amount, 1000);

        let EngineEvent::OrderBookDelta(delta) = &output.events[1].event else {
            panic!("expected orderbook delta");
        };
        assert_eq!(delta.engine_sequence, 4);
        assert_eq!(delta.bids[0].price, 100);
        assert_eq!(delta.bids[0].quantity, 0);
    }

    #[test]
    fn place_order_uses_reserved_margin_amount_without_wallet_event() {
        let engine = FakeEngine::new(100, 200);
        let mut maker = order("req-1", "res-maker", 1, Side::LONG, 10, 100);
        maker.reserved_margin_amount = 600;
        maker.leverage = 5;
        let mut taker = order("req-2", "res-taker", 2, Side::SHORT, 10, 100);
        taker.reserved_margin_amount = 900;
        taker.leverage = 3;

        let _ = engine.process_command(EngineCommand::PlaceOrder(maker));
        let output = engine.process_command(EngineCommand::PlaceOrder(taker));

        let EngineEvent::TradeExecuted(trade) = &output.events[0].event else {
            panic!("expected trade event");
        };
        assert_eq!(trade.settlements.len(), 2);
        assert_eq!(trade.settlements[0].debit_amount, 600);
        assert_eq!(trade.settlements[1].debit_amount, 900);
    }

    #[test]
    fn process_input_populates_source_metadata() {
        let engine = FakeEngine::new(100, 200);
        let mut place_order = order("req-source", "res-source", 42, Side::LONG, 10, 100);
        place_order.input_id = Some(String::from("input-place-1"));
        place_order.reserved_margin_amount = 500;

        let output = engine.process_input(EngineCommand::PlaceOrder(place_order), Some(123));

        let EngineReply::OrderAccepted(reply) = &output.replies[0].reply else {
            panic!("expected order accepted reply");
        };
        assert_eq!(reply.source_input_id.as_deref(), Some("input-place-1"));
        assert_eq!(reply.source_input_offset, Some(123));

        let EngineEvent::OrderOpened(event) = &output.events[0].event else {
            panic!("expected order opened event");
        };
        assert_eq!(event.source_input_id.as_deref(), Some("input-place-1"));
        assert_eq!(event.source_input_offset, Some(123));
    }

    #[test]
    fn reduce_only_order_matches_without_opening_remainder() {
        let engine = FakeEngine::new(100, 200);
        reserve(&engine, "req-1", "res-maker", 1000);
        reserve(&engine, "req-2", "res-close", 1000);
        let _ = engine.process_command(EngineCommand::PlaceOrder(order(
            "req-1",
            "res-maker",
            1,
            Side::LONG,
            10,
            100,
        )));

        let output = engine.process_command(EngineCommand::PlaceOrder(reduce_only_order(
            "req-2",
            "res-close",
            2,
            Side::SHORT,
            10,
            0,
        )));

        assert!(matches!(
            output.replies[0].reply,
            EngineReply::OrderAccepted(OrderAccepted { order_id: 101, .. })
        ));
        assert!(
            output
                .events
                .iter()
                .all(|publication| !matches!(&publication.event, EngineEvent::OrderOpened(_)))
        );
        let EngineEvent::TradeExecuted(trade) = &output.events[0].event else {
            panic!("expected trade event");
        };
        assert_eq!(trade.quantity, 10);
        assert_eq!(trade.maker_order_id, 100);
        assert_eq!(trade.taker_order_id, 101);
    }

    #[test]
    fn reduce_only_partial_match_expires_unmatched_remainder() {
        let engine = FakeEngine::new(100, 200);
        reserve(&engine, "req-1", "res-maker", 400);
        reserve(&engine, "req-2", "res-close", 1000);
        let _ = engine.process_command(EngineCommand::PlaceOrder(order(
            "req-1",
            "res-maker",
            1,
            Side::LONG,
            4,
            100,
        )));

        let output = engine.process_command(EngineCommand::PlaceOrder(reduce_only_order(
            "req-2",
            "res-close",
            2,
            Side::SHORT,
            10,
            0,
        )));

        assert!(
            output
                .events
                .iter()
                .all(|publication| !matches!(&publication.event, EngineEvent::OrderOpened(_)))
        );
        let EngineEvent::TradeExecuted(trade) = &output.events[0].event else {
            panic!("expected trade event");
        };
        assert_eq!(trade.quantity, 4);

        let EngineEvent::OrderBookDelta(delta) = &output.events[1].event else {
            panic!("expected orderbook delta");
        };
        assert_eq!(delta.bids[0].quantity, 0);
    }

    #[test]
    fn reduce_only_unmatched_order_does_not_rest() {
        let engine = FakeEngine::new(100, 200);
        reserve(&engine, "req-1", "res-close", 1000);

        let output = engine.process_command(EngineCommand::PlaceOrder(reduce_only_order(
            "req-1",
            "res-close",
            2,
            Side::SHORT,
            10,
            0,
        )));

        assert!(matches!(
            output.replies[0].reply,
            EngineReply::OrderAccepted(OrderAccepted { order_id: 100, .. })
        ));
        assert!(output.events.is_empty());
    }

    #[test]
    fn liquidation_command_matches_as_engine_authorized_reduce_only() {
        let engine = FakeEngine::new(100, 200);
        reserve(&engine, "req-maker", "res-maker", 1000);
        let _ = engine.process_command(EngineCommand::PlaceOrder(order(
            "req-maker",
            "res-maker",
            1,
            Side::LONG,
            10,
            100,
        )));

        let output = engine.process_command(EngineCommand::LiquidatePosition(liquidate_position(
            "req-liq",
            "liq-1",
            2,
            Side::LONG,
            10,
            0,
        )));

        assert!(matches!(
            output.replies[0].reply,
            EngineReply::LiquidationAccepted(LiquidationAccepted { order_id: 101, .. })
        ));

        let EngineEvent::TradeExecuted(trade) = &output.events[0].event else {
            panic!("expected liquidation trade");
        };
        assert_eq!(trade.execution_reason, ExecutionReason::LIQUIDATION);
        assert_eq!(trade.maker_order_id, 100);
        assert_eq!(trade.taker_order_id, 101);
        assert_eq!(trade.taker_user_id, 2);
        assert_eq!(trade.taker_reservation_id, Some(String::from("liq-1")));
        assert_eq!(trade.settlements.len(), 1);
        assert_eq!(trade.settlements[0].reservation_id, "res-maker");
    }

    #[test]
    fn invalid_liquidation_command_is_rejected() {
        let engine = FakeEngine::new(100, 200);

        let output = engine.process_command(EngineCommand::LiquidatePosition(liquidate_position(
            "req-liq",
            "liq-1",
            2,
            Side::LONG,
            0,
            0,
        )));

        assert!(matches!(
            output.replies[0].reply,
            EngineReply::LiquidationRejected(LiquidationRejected { .. })
        ));
        assert!(output.events.is_empty());
    }

    #[test]
    fn cancel_resting_order_emits_cancel_event() {
        let engine = FakeEngine::new(100, 200);
        reserve(&engine, "req-1", "res-1", 500);
        let _ = engine.process_command(EngineCommand::PlaceOrder(order(
            "req-1",
            "res-1",
            42,
            Side::LONG,
            10,
            100,
        )));

        let output = engine.process_command(EngineCommand::CancelOrder(CancelOrder {
            input_id: None,
            envelope: envelope("req-cancel", 42),
            market_id: 1,
            order_id: 100,
        }));

        assert!(matches!(
            output.replies[0].reply,
            EngineReply::CancelAccepted(CancelAccepted { order_id: 100, .. })
        ));
        assert!(matches!(
            output.events[0].event,
            EngineEvent::OrderCancelled(OrderCancelled {
                engine_sequence: 3,
                order_id: 100,
                released_amount: 500,
                ..
            })
        ));
        assert!(matches!(
            output.events[1].event,
            EngineEvent::OrderBookDelta(OrderBookDelta {
                engine_sequence: 4,
                market_id: 1,
                ..
            })
        ));
    }

    #[test]
    fn market_and_funding_inputs_emit_events_without_replies() {
        let engine = FakeEngine::new(100, 200);

        let mark = engine.process_input(
            EngineCommand::MarkPriceUpdated(MarkPriceUpdatedInput {
                input_id: Some(String::from("input-mark-1")),
                market_id: 1,
                mark_price: 101,
                index_price: 100,
                source_timestamp_ms: 1_710_000_000_000,
                published_at_ms: 1_710_000_000_100,
                valid_until_ms: 1_710_000_005_100,
                source_sequence: 45_001,
                source_status: String::from("VALID"),
            }),
            Some(10),
        );
        assert!(mark.replies.is_empty());
        let EngineEvent::MarkPriceUpdated(event) = &mark.events[0].event else {
            panic!("expected mark price event");
        };
        assert_eq!(event.engine_sequence, 1);
        assert_eq!(event.source_input_id.as_deref(), Some("input-mark-1"));
        assert_eq!(event.source_input_offset, Some(10));
        assert_eq!(event.mark_price, 101);

        let funding_rate = engine.process_input(
            EngineCommand::FundingRateUpdated(FundingRateUpdatedInput {
                input_id: Some(String::from("input-funding-rate-1")),
                market_id: 1,
                funding_interval_id: String::from("funding-1"),
                rate: 25,
                rate_scale: 1_000_000,
                interval_start_ms: 1_710_000_000_000,
                interval_end_ms: 1_710_028_800_000,
                source_timestamp_ms: 1_710_000_001_000,
            }),
            Some(11),
        );
        assert!(funding_rate.replies.is_empty());
        let EngineEvent::FundingRateUpdated(event) = &funding_rate.events[0].event else {
            panic!("expected funding rate event");
        };
        assert_eq!(event.engine_sequence, 2);
        assert_eq!(
            event.source_input_id.as_deref(),
            Some("input-funding-rate-1")
        );
        assert_eq!(event.source_input_offset, Some(11));
        assert_eq!(event.rate, 25);

        let settlement = engine.process_input(
            EngineCommand::FundingSettlementTick(FundingSettlementTickInput {
                input_id: Some(String::from("input-funding-settle-1")),
                market_id: 1,
                funding_interval_id: String::from("funding-1"),
                settle_at_ms: 1_710_028_800_000,
            }),
            Some(12),
        );
        assert!(settlement.replies.is_empty());
        let EngineEvent::FundingPaymentApplied(event) = &settlement.events[0].event else {
            panic!("expected funding payment event");
        };
        assert_eq!(event.engine_sequence, 3);
        assert_eq!(
            event.source_input_id.as_deref(),
            Some("input-funding-settle-1")
        );
        assert_eq!(event.source_input_offset, Some(12));
        assert!(event.payments.is_empty());
    }

    fn reserve(engine: &FakeEngine, request_id: &str, reservation_id: &str, amount: i64) {
        engine.observe_wallet_event(WalletEvent::FundsReserved(WalletFundsReserved {
            request_id: String::from(request_id),
            user_id: 0,
            reservation_id: String::from(reservation_id),
            asset: Asset::USDC,
            amount,
        }));
    }

    fn order(
        request_id: &str,
        reservation_id: &str,
        user_id: i64,
        side: Side,
        quantity: i64,
        price: i64,
    ) -> ReservedPlaceOrder {
        ReservedPlaceOrder {
            input_id: None,
            envelope: envelope(request_id, user_id),
            reservation_id: String::from(reservation_id),
            market_id: 1,
            market_name: String::from("SOL-PERP"),
            side,
            order_type: OrderType::LIMIT,
            quantity,
            price,
            reduce_only: false,
            margin_asset: Asset::USDC,
            reserved_margin_amount: 0,
            leverage: 1,
        }
    }

    fn reduce_only_order(
        request_id: &str,
        reservation_id: &str,
        user_id: i64,
        side: Side,
        quantity: i64,
        price: i64,
    ) -> ReservedPlaceOrder {
        let mut order = order(request_id, reservation_id, user_id, side, quantity, price);
        order.order_type = if price == 0 {
            OrderType::MARKET
        } else {
            OrderType::LIMIT
        };
        order.reduce_only = true;
        order
    }

    fn liquidate_position(
        request_id: &str,
        liquidation_id: &str,
        liquidated_user_id: i64,
        position_side: Side,
        quantity: i64,
        price: i64,
    ) -> LiquidatePosition {
        LiquidatePosition {
            input_id: None,
            envelope: envelope(request_id, 0),
            liquidation_id: String::from(liquidation_id),
            market_id: 1,
            market_name: String::from("SOL-PERP"),
            liquidated_user_id,
            position_side,
            quantity,
            price,
            request_source: None,
        }
    }

    fn envelope(request_id: &str, user_id: i64) -> CommandEnvelope {
        CommandEnvelope {
            request_id: String::from(request_id),
            idempotency_key: format!("{request_id}-key"),
            user_id,
            reply_partition: 0,
        }
    }
}
