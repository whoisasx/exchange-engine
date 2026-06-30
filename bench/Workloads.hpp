#pragma once

#include <cstdint>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "broker/RedpandaEngineApp.hpp"
#include "core/Command.hpp"
#include "core/EngineCore.hpp"
#include "core/FixedPoint.hpp"
#include "core/SymbolConfig.hpp"
#include "runtime/EngineRuntime.hpp"

namespace cex::bench {

inline constexpr SymbolId kSymbolId = 1;

struct CoreWorkload {
  std::vector<EngineCommand> setup;
  std::vector<EngineCommand> measured;
};

struct RuntimeJsonWorkload {
  std::vector<std::string> setup;
  std::vector<std::string> measured;
};

[[nodiscard]] inline SymbolConfig make_symbol() {
  return SymbolConfig{
      .symbolId = kSymbolId,
      .baseAssetId = 1,
      .quoteAssetId = 2,
      .tickSize = Price::from_ticks(1),
      .lotSize = Quantity::from_lots(1),
      .minQuantity = Quantity::from_lots(1),
      .maxQuantity = Quantity::from_lots(1'000'000'000),
      .minPrice = Price::from_ticks(1),
      .maxPrice = Price::from_ticks(2'000'000'000),
      .ringCapacityTicks = 10'000,
      .thresholdPercentage = 10,
      .initialBaseTick = 100'000,
      .priceScale = 0,
      .quantityScale = 0,
      .makerFeeRate = 0,
      .takerFeeRate = 0,
      .tradingEnabled = true,
  };
}

[[nodiscard]] inline cex::runtime::EngineRuntime make_runtime() {
  return cex::runtime::EngineRuntime(cex::runtime::EngineRuntimeConfig{
      .symbols = {make_symbol()},
      .first_public_sequence = 1,
      .clock = [] { return 1'710'000'000'000LL; },
  });
}

[[nodiscard]] inline EngineCommand place_command(CommandId command_id,
                                                 OrderId order_id,
                                                 UserId user_id,
                                                 Side side,
                                                 Price price,
                                                 Quantity quantity,
                                                 TimeInForce tif = GTC,
                                                 OrderType type = Limit,
                                                 SymbolId symbol_id = kSymbolId) {
  return EngineCommand{
      .kind = EngineCommandKind::PlaceOrder,
      .placeOrder =
          PlaceOrderCommand{
              .commandId = command_id,
              .clientOrderId = order_id,
              .orderId = order_id,
              .userId = user_id,
              .symbolId = symbol_id,
              .side = side,
              .orderType = type,
              .timeInForce = tif,
              .price = price,
              .quantity = quantity,
              .receivedSequence = command_id,
          },
      .cancelOrder = std::nullopt,
  };
}

[[nodiscard]] inline EngineCommand cancel_command(CommandId command_id,
                                                  OrderId order_id,
                                                  UserId user_id,
                                                  SymbolId symbol_id = kSymbolId) {
  return EngineCommand{
      .kind = EngineCommandKind::CancelOrder,
      .placeOrder = std::nullopt,
      .cancelOrder =
          CancelOrderCommand{
              .commandId = command_id,
              .userId = user_id,
              .symbolId = symbol_id,
              .orderId = order_id,
              .clientOrderId = 0,
              .receivedSequence = command_id,
          },
  };
}

[[nodiscard]] inline std::string place_json(std::uint64_t command_id,
                                            std::uint64_t order_id,
                                            std::uint64_t user_id,
                                            std::uint64_t market_id,
                                            std::string side,
                                            std::int64_t price,
                                            std::uint64_t quantity,
                                            bool reduce_only = false) {
  std::ostringstream json;
  json << R"json({
  "type": "PlaceOrder",
  "payload": {
    "input_id": "bench_input_)json"
       << command_id << R"json(",
    "envelope": {
      "request_id": "bench_req_)json"
       << command_id << R"json(",
      "idempotency_key": "bench_key_)json"
       << command_id << R"json(",
      "user_id": )json"
       << user_id << R"json(,
      "reply_partition": 0
    },
    "reservation_id": "bench_res_)json"
       << command_id << R"json(",
    "order_id": )json"
       << order_id << R"json(,
    "market_id": )json"
       << market_id << R"json(,
    "market_name": "SOL-PERP",
    "side": ")json"
       << side << R"json(",
    "order_type": "LIMIT",
    "quantity": )json"
       << quantity << R"json(,
    "price": )json"
       << price << R"json(,
    "reduce_only": )json"
       << (reduce_only ? "true" : "false") << R"json(,
    "margin_asset": "USDC",
    "reserved_margin_amount": 100,
    "leverage": 10
  }
})json";
  return json.str();
}

[[nodiscard]] inline std::string cancel_json(std::uint64_t command_id,
                                             std::uint64_t order_id,
                                             std::uint64_t user_id,
                                             std::uint64_t market_id) {
  std::ostringstream json;
  json << R"json({
  "type": "CancelOrder",
  "payload": {
    "input_id": "bench_input_)json"
       << command_id << R"json(",
    "envelope": {
      "request_id": "bench_req_)json"
       << command_id << R"json(",
      "idempotency_key": "bench_key_)json"
       << command_id << R"json(",
      "user_id": )json"
       << user_id << R"json(,
      "reply_partition": 0
    },
    "market_id": )json"
       << market_id << R"json(,
    "order_id": )json"
       << order_id << R"json(
  }
})json";
  return json.str();
}

[[nodiscard]] inline EngineCommand measured_place(std::uint64_t i,
                                                  std::uint64_t command_base,
                                                  std::uint64_t seed) {
  const auto id = command_base + i;
  const auto price =
      Price::from_ticks(90'000 + static_cast<std::int64_t>((i + seed) % 100));
  return place_command(id,
                       id,
                       1 + ((i + seed) % 1'000),
                       Buy,
                       price,
                       Quantity::from_lots(1 + (i % 10)));
}

[[nodiscard]] inline CoreWorkload make_core_workload(
    const std::string& scenario,
    std::uint64_t measured_count,
    std::uint64_t book_depth,
    std::uint64_t seed) {
  CoreWorkload workload;
  workload.measured.reserve(static_cast<std::size_t>(measured_count));

  if (scenario == "place_only") {
    for (std::uint64_t i = 0; i < measured_count; ++i) {
      workload.measured.push_back(measured_place(i, 1, seed));
    }
    return workload;
  }

  if (scenario == "reject_path") {
    for (std::uint64_t i = 0; i < measured_count; ++i) {
      const auto id = 1 + i;
      workload.measured.push_back(place_command(id,
                                                id,
                                                1 + (i % 1'000),
                                                Buy,
                                                Price::from_ticks(90'000),
                                                Quantity::from_lots(1),
                                                GTC,
                                                Limit,
                                                999));
    }
    return workload;
  }

  if (scenario == "match_heavy") {
    workload.setup.reserve(static_cast<std::size_t>(measured_count));
    for (std::uint64_t i = 0; i < measured_count; ++i) {
      const auto maker_id = 1 + i;
      workload.setup.push_back(place_command(maker_id,
                                             maker_id,
                                             10'000 + (i % 1'000),
                                             Sell,
                                             Price::from_ticks(100'000),
                                             Quantity::from_lots(1)));
      const auto taker_id = 1'000'000 + i;
      workload.measured.push_back(place_command(taker_id,
                                                taker_id,
                                                20'000 + (i % 1'000),
                                                Buy,
                                                Price::from_ticks(100'000),
                                                Quantity::from_lots(1),
                                                IOC));
    }
    return workload;
  }

  if (scenario == "cancel_heavy") {
    workload.setup.reserve(static_cast<std::size_t>(measured_count));
    for (std::uint64_t i = 0; i < measured_count; ++i) {
      const auto order_id = 1 + i;
      const auto user_id = 1 + (i % 1'000);
      workload.setup.push_back(place_command(order_id,
                                             order_id,
                                             user_id,
                                             Buy,
                                             Price::from_ticks(90'000),
                                             Quantity::from_lots(1)));
      workload.measured.push_back(cancel_command(1'000'000 + i,
                                                 order_id,
                                                 user_id));
    }
    return workload;
  }

  if (scenario == "deep_book") {
    workload.setup.reserve(static_cast<std::size_t>(book_depth));
    for (std::uint64_t i = 0; i < book_depth; ++i) {
      const auto order_id = 1 + i;
      const auto price_ticks =
          50'000 + static_cast<std::int64_t>((i + seed) % 50'000);
      workload.setup.push_back(place_command(order_id,
                                             order_id,
                                             1 + ((i + seed) % 1'000),
                                             Buy,
                                             Price::from_ticks(price_ticks),
                                             Quantity::from_lots(1)));
    }
    for (std::uint64_t i = 0; i < measured_count; ++i) {
      workload.measured.push_back(measured_place(i, 10'000'000, seed));
    }
    return workload;
  }

  if (scenario == "mixed") {
    for (std::uint64_t i = 0; i < measured_count; ++i) {
      const auto group = i / 4U;
      const auto slot = i % 4U;
      const auto base = 1 + group * 10U;
      if (slot == 0) {
        workload.measured.push_back(place_command(base,
                                                  base,
                                                  1 + ((group + seed) % 1'000),
                                                  Buy,
                                                  Price::from_ticks(90'000),
                                                  Quantity::from_lots(1)));
      } else if (slot == 1) {
        workload.measured.push_back(cancel_command(base + 1U,
                                                   base,
                                                   1 + ((group + seed) % 1'000)));
      } else if (slot == 2) {
        workload.measured.push_back(place_command(base + 2U,
                                                  base + 2U,
                                                  10'000 + ((group + seed) % 1'000),
                                                  Sell,
                                                  Price::from_ticks(100'000),
                                                  Quantity::from_lots(1)));
      } else {
        workload.measured.push_back(place_command(base + 3U,
                                                  base + 3U,
                                                  20'000 + ((group + seed) % 1'000),
                                                  Buy,
                                                  Price::from_ticks(100'000),
                                                  Quantity::from_lots(1),
                                                  IOC));
      }
    }
    return workload;
  }

  throw std::invalid_argument("unknown scenario: " + scenario);
}

[[nodiscard]] inline RuntimeJsonWorkload make_runtime_json_workload(
    const std::string& scenario,
    std::uint64_t measured_count,
    std::uint64_t book_depth,
    std::uint64_t seed) {
  const CoreWorkload core =
      make_core_workload(scenario, measured_count, book_depth, seed);
  RuntimeJsonWorkload workload;
  workload.setup.reserve(core.setup.size());
  workload.measured.reserve(core.measured.size());

  const auto append_json = [](const EngineCommand& command,
                              std::vector<std::string>& output) {
    if (command.kind == EngineCommandKind::PlaceOrder &&
        command.placeOrder.has_value()) {
      const auto& place = *command.placeOrder;
      output.push_back(place_json(place.commandId,
                                  place.orderId,
                                  place.userId,
                                  place.symbolId,
                                  place.side == Buy ? "LONG" : "SHORT",
                                  place.price.ticks(),
                                  place.quantity.lots()));
      return;
    }
    if (command.kind == EngineCommandKind::CancelOrder &&
        command.cancelOrder.has_value()) {
      const auto& cancel = *command.cancelOrder;
      output.push_back(cancel_json(cancel.commandId,
                                   cancel.orderId,
                                   cancel.userId,
                                   cancel.symbolId));
      return;
    }
    throw std::invalid_argument("unsupported benchmark command");
  };

  for (const auto& command : core.setup) {
    append_json(command, workload.setup);
  }
  for (const auto& command : core.measured) {
    append_json(command, workload.measured);
  }
  return workload;
}

[[nodiscard]] inline cex::runtime::InboundEngineRecord runtime_record(
    std::string raw_json,
    std::int64_t offset) {
  return cex::runtime::InboundEngineRecord{
      .topic = cex::runtime::EngineInputTopic,
      .partition = 0,
      .offset = offset,
      .key = std::string{"1"},
      .raw_json = std::move(raw_json),
  };
}

[[nodiscard]] inline cex::broker::ConsumedRecord broker_record(
    std::string raw_json,
    std::int64_t offset) {
  return cex::broker::ConsumedRecord{
      .topic = cex::broker::EngineInputTopic,
      .partition = 0,
      .offset = offset,
      .key = std::string{"1"},
      .value = std::move(raw_json),
  };
}

}  // namespace cex::bench
