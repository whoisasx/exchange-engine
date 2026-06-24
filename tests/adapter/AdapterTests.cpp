#include "adapter/EngineAdapter.hpp"

#include <cassert>
#include <cstdint>
#include <stdexcept>

using namespace cex::adapter;

namespace {

PlaceOrderInput make_place_order() {
  return PlaceOrderInput{
      .input_id = std::string{"input_place_001"},
      .envelope =
          CommandEnvelope{
              .request_id = "req_place_001",
              .idempotency_key = "client-order-001",
              .user_id = 42,
              .reply_partition = 3,
          },
      .order_id = 9001,
      .reservation_id = "res_place_001",
      .market_id = 1,
      .market_name = "SOL-PERP",
      .side = AdapterSide::Long,
      .order_type = AdapterOrderType::Limit,
      .time_in_force = AdapterTimeInForce::Gtc,
      .price = 100,
      .quantity = 10,
      .reduce_only = false,
      .margin_asset = "USDC",
      .reserved_margin_amount = 100,
      .leverage = 10,
  };
}

CancelOrderInput make_cancel_order() {
  return CancelOrderInput{
      .input_id = std::string{"input_cancel_001"},
      .envelope =
          CommandEnvelope{
              .request_id = "req_cancel_001",
              .idempotency_key = "client-cancel-001",
              .user_id = 42,
              .reply_partition = 3,
          },
      .market_id = 1,
      .order_id = 9001,
  };
}

void test_place_order_mapping_records_metadata() {
  const auto input = make_place_order();
  const auto mapped = map_place_order_to_core(input, 77);

  assert(mapped.command.kind == EngineCommandKind::PlaceOrder);
  assert(mapped.command.placeOrder.has_value());
  assert(!mapped.command.cancelOrder.has_value());
  assert(mapped.metadata_to_record.has_value());

  const PlaceOrderCommand& command = *mapped.command.placeOrder;
  assert(command.commandId == command_id_from_request_id("req_place_001"));
  assert(command.clientOrderId == client_order_id_from_idempotency_key("client-order-001"));
  assert(command.orderId == 9001);
  assert(command.userId == 42);
  assert(command.symbolId == 1);
  assert(command.side == Buy);
  assert(command.orderType == Limit);
  assert(command.timeInForce == GTC);
  assert(command.price.ticks() == 100);
  assert(command.quantity.lots() == 10);
  assert(command.receivedSequence == 77);

  OrderMetadataStore store;
  assert(store.empty());
  assert(store.insert(*mapped.metadata_to_record));
  assert(!store.insert(*mapped.metadata_to_record));
  assert(store.size() == 1);

  const OrderMetadata* metadata = store.find(9001);
  assert(metadata != nullptr);
  assert(metadata->order_id == 9001);
  assert(metadata->market_id == 1);
  assert(metadata->user_id == 42);
  assert(metadata->reservation_id == "res_place_001");
  assert(metadata->place_request_id == "req_place_001");
  assert(metadata->place_idempotency_key == "client-order-001");
  assert(metadata->place_input_id == "input_place_001");
  assert(metadata->reply_partition == 3);
  assert(metadata->core_client_order_id == command.clientOrderId);
  assert(metadata->core_place_command_id == command.commandId);
}

void test_cancel_order_mapping() {
  const auto mapped = map_cancel_order_to_core(make_cancel_order(), 78);

  assert(mapped.command.kind == EngineCommandKind::CancelOrder);
  assert(!mapped.command.placeOrder.has_value());
  assert(mapped.command.cancelOrder.has_value());
  assert(!mapped.metadata_to_record.has_value());

  const CancelOrderCommand& command = *mapped.command.cancelOrder;
  assert(command.commandId == command_id_from_request_id("req_cancel_001"));
  assert(command.clientOrderId == client_order_id_from_idempotency_key("client-cancel-001"));
  assert(command.userId == 42);
  assert(command.symbolId == 1);
  assert(command.orderId == 9001);
  assert(command.receivedSequence == 78);
}

void test_large_exchange_user_id_maps_to_core() {
  constexpr std::int64_t user_id = 475'230'507'652'431'248LL;
  auto input = make_place_order();
  input.envelope.user_id = user_id;

  const auto mapped = map_place_order_to_core(input, 79);

  assert(mapped.command.placeOrder->userId == static_cast<UserId>(user_id));
  assert(mapped.metadata_to_record->user_id == user_id);
}

void test_market_order_allows_zero_price() {
  auto input = make_place_order();
  input.order_id = 9002;
  input.order_type = AdapterOrderType::Market;
  input.price = 0;

  const auto mapped = map_place_order_to_core(input);
  assert(mapped.command.placeOrder->orderType == Market);
  assert(mapped.command.placeOrder->price.ticks() == 0);
}

void test_public_sequences_are_per_market() {
  MarketSequenceGenerator sequences;

  assert(sequences.peek(1) == 1);
  assert(sequences.next(1) == 1);
  assert(sequences.next(1) == 2);
  assert(sequences.next(2) == 1);

  sequences.restore(1, 10);
  assert(sequences.peek(1) == 10);
  assert(sequences.next(1) == 10);
}

void test_validation_rejects_bad_boundary_inputs() {
  auto input = make_place_order();
  input.envelope.request_id.clear();
  bool threw = false;
  try {
    (void)map_place_order_to_core(input);
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  assert(threw);

  input = make_place_order();
  input.quantity = 0;
  threw = false;
  try {
    (void)map_place_order_to_core(input);
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  assert(threw);

  input = make_place_order();
  input.price = 0;
  threw = false;
  try {
    (void)map_place_order_to_core(input);
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  assert(threw);
}

} // namespace

int main() {
  test_place_order_mapping_records_metadata();
  test_cancel_order_mapping();
  test_large_exchange_user_id_maps_to_core();
  test_market_order_allows_zero_price();
  test_public_sequences_are_per_market();
  test_validation_rejects_bad_boundary_inputs();
  return 0;
}
