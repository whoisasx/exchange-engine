#include "checkpoint/file/FileCheckpointStore.hpp"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

namespace cex::checkpoint {
namespace {

constexpr std::string_view kMagic = "cex.engine.checkpoint.file.v1";
constexpr std::string_view kCheckpointExtension = ".checkpoint";

class CheckpointParseError final : public std::runtime_error {
 public:
  explicit CheckpointParseError(const std::string& message)
      : std::runtime_error(message) {}
};

[[nodiscard]] bool is_safe_filename_char(unsigned char value) {
  return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
         (value >= '0' && value <= '9') || value == '-' || value == '_' ||
         value == '.';
}

[[nodiscard]] char hex_digit(unsigned int value) {
  return static_cast<char>(value < 10 ? ('0' + value) : ('A' + value - 10));
}

[[nodiscard]] int hex_value(char value) {
  if (value >= '0' && value <= '9') {
    return value - '0';
  }
  if (value >= 'A' && value <= 'F') {
    return value - 'A' + 10;
  }
  if (value >= 'a' && value <= 'f') {
    return value - 'a' + 10;
  }
  return -1;
}

[[nodiscard]] std::string filename_for_checkpoint_id(
    const std::string& checkpoint_id) {
  std::string filename;
  filename.reserve(checkpoint_id.size() + kCheckpointExtension.size());

  for (const unsigned char value : checkpoint_id) {
    if (is_safe_filename_char(value)) {
      filename.push_back(static_cast<char>(value));
      continue;
    }

    filename.push_back('%');
    filename.push_back(hex_digit(value >> 4U));
    filename.push_back(hex_digit(value & 0x0FU));
  }

  if (filename.empty()) {
    filename = "checkpoint";
  }
  filename.append(kCheckpointExtension);
  return filename;
}

[[nodiscard]] std::string escape_field(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.size());

  for (const unsigned char ch : value) {
    switch (ch) {
      case '\\':
        escaped += "\\\\";
        break;
      case '\n':
        escaped += "\\n";
        break;
      case '\r':
        escaped += "\\r";
        break;
      case '\t':
        escaped += "\\t";
        break;
      default:
        if (ch < 0x20U) {
          escaped += "\\x";
          escaped.push_back(hex_digit(ch >> 4U));
          escaped.push_back(hex_digit(ch & 0x0FU));
        } else {
          escaped.push_back(static_cast<char>(ch));
        }
        break;
    }
  }

  return escaped;
}

[[nodiscard]] std::string unescape_field(const std::string& value) {
  std::string unescaped;
  unescaped.reserve(value.size());

  for (std::size_t index = 0; index < value.size(); ++index) {
    const char ch = value[index];
    if (ch != '\\') {
      unescaped.push_back(ch);
      continue;
    }

    if (++index >= value.size()) {
      throw CheckpointParseError("trailing escape in checkpoint field");
    }

    switch (value[index]) {
      case '\\':
        unescaped.push_back('\\');
        break;
      case 'n':
        unescaped.push_back('\n');
        break;
      case 'r':
        unescaped.push_back('\r');
        break;
      case 't':
        unescaped.push_back('\t');
        break;
      case 'x': {
        if (index + 2 >= value.size()) {
          throw CheckpointParseError("incomplete hex escape in checkpoint field");
        }
        const int high = hex_value(value[index + 1]);
        const int low = hex_value(value[index + 2]);
        if (high < 0 || low < 0) {
          throw CheckpointParseError("invalid hex escape in checkpoint field");
        }
        unescaped.push_back(static_cast<char>((high << 4) | low));
        index += 2;
        break;
      }
      default:
        throw CheckpointParseError("invalid escape in checkpoint field");
    }
  }

  return unescaped;
}

[[nodiscard]] std::vector<std::string> split_line(const std::string& line) {
  std::vector<std::string> fields;
  std::size_t start = 0;

  while (start <= line.size()) {
    const std::size_t end = line.find('\t', start);
    if (end == std::string::npos) {
      fields.push_back(line.substr(start));
      break;
    }
    fields.push_back(line.substr(start, end - start));
    start = end + 1;
  }

  return fields;
}

template <typename Integer>
[[nodiscard]] Integer parse_integer(const std::string& value) {
  Integer parsed{};
  const auto* begin = value.data();
  const auto* end = value.data() + value.size();
  const auto [ptr, error] = std::from_chars(begin, end, parsed);
  if (error != std::errc{} || ptr != end) {
    throw CheckpointParseError("invalid integer in checkpoint field");
  }
  return parsed;
}

[[nodiscard]] std::uint8_t parse_uint8(const std::string& value) {
  const auto parsed = parse_integer<unsigned int>(value);
  if (parsed > std::numeric_limits<std::uint8_t>::max()) {
    throw CheckpointParseError("uint8 checkpoint field out of range");
  }
  return static_cast<std::uint8_t>(parsed);
}

[[nodiscard]] bool parse_bool(const std::string& value) {
  if (value == "0") {
    return false;
  }
  if (value == "1") {
    return true;
  }
  throw CheckpointParseError("invalid boolean in checkpoint field");
}

[[nodiscard]] std::string bool_field(bool value) {
  return value ? "1" : "0";
}

template <typename Enum>
[[nodiscard]] std::string enum_field(Enum value) {
  return std::to_string(static_cast<int>(value));
}

[[nodiscard]] Side parse_side(const std::string& value) {
  const int parsed = parse_integer<int>(value);
  switch (parsed) {
    case Buy:
      return Buy;
    case Sell:
      return Sell;
    default:
      throw CheckpointParseError("invalid side in checkpoint field");
  }
}

[[nodiscard]] cex::adapter::AdapterSide parse_adapter_side(
    const std::string& value) {
  const int parsed = parse_integer<int>(value);
  switch (parsed) {
    case static_cast<int>(cex::adapter::AdapterSide::Long):
      return cex::adapter::AdapterSide::Long;
    case static_cast<int>(cex::adapter::AdapterSide::Short):
      return cex::adapter::AdapterSide::Short;
    default:
      throw CheckpointParseError("invalid adapter side in checkpoint field");
  }
}

[[nodiscard]] cex::runtime::RuntimeCommandKind parse_runtime_command_kind(
    const std::string& value) {
  const int parsed = parse_integer<int>(value);
  switch (parsed) {
    case static_cast<int>(cex::runtime::RuntimeCommandKind::PlaceOrder):
      return cex::runtime::RuntimeCommandKind::PlaceOrder;
    case static_cast<int>(cex::runtime::RuntimeCommandKind::CancelOrder):
      return cex::runtime::RuntimeCommandKind::CancelOrder;
    case static_cast<int>(cex::runtime::RuntimeCommandKind::MarkPriceUpdated):
      return cex::runtime::RuntimeCommandKind::MarkPriceUpdated;
    case static_cast<int>(cex::runtime::RuntimeCommandKind::FundingRateUpdated):
      return cex::runtime::RuntimeCommandKind::FundingRateUpdated;
    case static_cast<int>(
        cex::runtime::RuntimeCommandKind::FundingSettlementTick):
      return cex::runtime::RuntimeCommandKind::FundingSettlementTick;
    default:
      throw CheckpointParseError("invalid runtime command kind");
  }
}

[[nodiscard]] RejectReason parse_reject_reason(const std::string& value) {
  const int parsed = parse_integer<int>(value);
  switch (parsed) {
    case InvalidSymbol:
      return InvalidSymbol;
    case InvalidPrice:
      return InvalidPrice;
    case InvalidQuantity:
      return InvalidQuantity;
    case DuplicateCommand:
      return DuplicateCommand;
    case DuplicateOrder:
      return DuplicateOrder;
    case OrderNotFound:
      return OrderNotFound;
    case WouldSelfTrade:
      return WouldSelfTrade;
    case MarketClosed:
      return MarketClosed;
    case InsufficientBalanceLiquidity:
      return InsufficientBalanceLiquidity;
    default:
      throw CheckpointParseError("invalid reject reason");
  }
}

void require_field_count(const std::vector<std::string>& fields,
                         std::size_t expected) {
  if (fields.size() != expected) {
    throw CheckpointParseError("unexpected checkpoint field count");
  }
}

void write_line(std::ostream& out, const std::vector<std::string>& fields) {
  bool first = true;
  for (const auto& field : fields) {
    if (!first) {
      out << '\t';
    }
    first = false;
    out << field;
  }
  out << '\n';
}

void write_line(std::ostream& out,
                std::initializer_list<std::string> fields) {
  write_line(out, std::vector<std::string>(fields));
}

[[nodiscard]] std::vector<OrderBookSnapshot> sorted_symbol_snapshots(
    const EngineSnapshot& snapshot) {
  auto symbols = snapshot.symbolSnapshots;
  std::sort(symbols.begin(),
            symbols.end(),
            [](const OrderBookSnapshot& left, const OrderBookSnapshot& right) {
              return left.symbolId < right.symbolId;
            });
  return symbols;
}

[[nodiscard]] std::vector<OrderSnapshot> sorted_orders(
    const std::vector<OrderSnapshot>& orders) {
  auto sorted = orders;
  std::sort(sorted.begin(),
            sorted.end(),
            [](const OrderSnapshot& left, const OrderSnapshot& right) {
              return left.orderId < right.orderId;
            });
  return sorted;
}

[[nodiscard]] std::vector<std::pair<ClientOrderId, OrderId>>
sorted_client_order_map(
    const std::unordered_map<ClientOrderId, OrderId>& entries) {
  std::vector<std::pair<ClientOrderId, OrderId>> sorted;
  sorted.reserve(entries.size());
  for (const auto& [client_order_id, order_id] : entries) {
    sorted.emplace_back(client_order_id, order_id);
  }
  std::sort(sorted.begin(), sorted.end());
  return sorted;
}

[[nodiscard]] std::vector<std::pair<CommandId, CommandResultSummary>>
sorted_command_summaries(
    const std::unordered_map<CommandId, CommandResultSummary>& entries) {
  std::vector<std::pair<CommandId, CommandResultSummary>> sorted;
  sorted.reserve(entries.size());
  for (const auto& [command_id, summary] : entries) {
    sorted.emplace_back(command_id, summary);
  }
  std::sort(sorted.begin(),
            sorted.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });
  return sorted;
}

[[nodiscard]] std::vector<OrderId> sorted_order_ids(
    const std::unordered_set<OrderId>& order_ids) {
  std::vector<OrderId> sorted(order_ids.begin(), order_ids.end());
  std::sort(sorted.begin(), sorted.end());
  return sorted;
}

[[nodiscard]] std::vector<std::pair<cex::adapter::MarketId, EngineSequence>>
sorted_public_sequences(
    const std::unordered_map<cex::adapter::MarketId, EngineSequence>& entries) {
  std::vector<std::pair<cex::adapter::MarketId, EngineSequence>> sorted;
  sorted.reserve(entries.size());
  for (const auto& [market_id, next_sequence] : entries) {
    sorted.emplace_back(market_id, next_sequence);
  }
  std::sort(sorted.begin(), sorted.end());
  return sorted;
}

[[nodiscard]] std::vector<
    std::pair<cex::adapter::MarketId, cex::runtime::MarkPriceState>>
sorted_mark_prices(
    const std::unordered_map<cex::adapter::MarketId,
                             cex::runtime::MarkPriceState>& entries) {
  std::vector<
      std::pair<cex::adapter::MarketId, cex::runtime::MarkPriceState>>
      sorted;
  sorted.reserve(entries.size());
  for (const auto& [market_id, state] : entries) {
    sorted.emplace_back(market_id, state);
  }
  std::sort(sorted.begin(),
            sorted.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });
  return sorted;
}

[[nodiscard]] std::vector<
    std::pair<cex::adapter::MarketId, cex::runtime::FundingRateState>>
sorted_funding_rates(
    const std::unordered_map<cex::adapter::MarketId,
                             cex::runtime::FundingRateState>& entries) {
  std::vector<
      std::pair<cex::adapter::MarketId, cex::runtime::FundingRateState>>
      sorted;
  sorted.reserve(entries.size());
  for (const auto& [market_id, state] : entries) {
    sorted.emplace_back(market_id, state);
  }
  std::sort(sorted.begin(),
            sorted.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });
  return sorted;
}

[[nodiscard]] std::vector<cex::runtime::FundingSettlementKey>
sorted_settled_funding_intervals(
    const cex::runtime::FundingSettlementSet& entries) {
  return std::vector<cex::runtime::FundingSettlementKey>(entries.begin(),
                                                         entries.end());
}

[[nodiscard]] std::vector<
    std::pair<cex::runtime::PositionRiskKey,
              cex::runtime::IsolatedPositionState>>
sorted_positions(const cex::runtime::IsolatedPositionMap& entries) {
  std::vector<std::pair<cex::runtime::PositionRiskKey,
                        cex::runtime::IsolatedPositionState>>
      sorted;
  sorted.reserve(entries.size());
  for (const auto& [key, state] : entries) {
    sorted.emplace_back(key, state);
  }
  return sorted;
}

[[nodiscard]] std::vector<
    std::pair<cex::runtime::PositionRiskKey, cex::runtime::IsolatedRiskState>>
sorted_risk_states(const cex::runtime::IsolatedRiskMap& entries) {
  std::vector<std::pair<cex::runtime::PositionRiskKey,
                        cex::runtime::IsolatedRiskState>>
      sorted;
  sorted.reserve(entries.size());
  for (const auto& [key, state] : entries) {
    sorted.emplace_back(key, state);
  }
  return sorted;
}

[[nodiscard]] std::vector<std::pair<
    std::string,
    cex::runtime::EngineRuntimeProcessedRequestSnapshot>>
sorted_processed_requests(
    const std::unordered_map<
        std::string,
        cex::runtime::EngineRuntimeProcessedRequestSnapshot>& entries) {
  std::vector<std::pair<
      std::string,
      cex::runtime::EngineRuntimeProcessedRequestSnapshot>>
      sorted;
  sorted.reserve(entries.size());
  for (const auto& [key, processed] : entries) {
    sorted.emplace_back(key, processed);
  }
  std::sort(sorted.begin(),
            sorted.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });
  return sorted;
}

[[nodiscard]] std::vector<cex::adapter::OrderMetadata> metadata_entries(
    const EngineCheckpoint& checkpoint) {
  std::vector<cex::adapter::OrderMetadata> entries;
  std::unordered_set<OrderId> seen;

  for (const auto& symbol : checkpoint.core_snapshot.symbolSnapshots) {
    for (const auto& order : symbol.activeOrders) {
      if (!seen.insert(order.orderId).second) {
        continue;
      }

      if (const auto* metadata = checkpoint.metadata_store.find(order.orderId);
          metadata != nullptr) {
        entries.push_back(*metadata);
      }
    }
  }

  std::sort(entries.begin(),
            entries.end(),
            [](const auto& left, const auto& right) {
              return left.order_id < right.order_id;
            });
  return entries;
}

void write_symbol_config(std::ostream& out, const SymbolConfig& config) {
  write_line(out,
             {"symbol",
              std::to_string(config.symbolId),
              std::to_string(config.baseAssetId),
              std::to_string(config.quoteAssetId),
              std::to_string(config.tickSize.ticks()),
              std::to_string(config.lotSize.lots()),
              std::to_string(config.minQuantity.lots()),
              std::to_string(config.maxQuantity.lots()),
              std::to_string(config.minPrice.ticks()),
              std::to_string(config.maxPrice.ticks()),
              std::to_string(config.ringCapacityTicks),
              std::to_string(config.thresholdPercentage),
              std::to_string(config.initialBaseTick),
              std::to_string(static_cast<unsigned int>(config.priceScale)),
              std::to_string(static_cast<unsigned int>(config.quantityScale)),
              std::to_string(config.makerFeeRate),
              std::to_string(config.takerFeeRate),
              bool_field(config.tradingEnabled)});
}

void write_price_levels(std::ostream& out,
                        std::string_view count_tag,
                        std::string_view entry_tag,
                        const std::vector<PriceLevelSnapshot>& levels) {
  write_line(out, {std::string(count_tag), std::to_string(levels.size())});

  for (const auto& level : levels) {
    std::vector<std::string> fields{
        std::string(entry_tag),
        std::to_string(level.price.ticks()),
        std::to_string(level.totalQuantity.lots()),
        std::to_string(level.orderIds.size()),
    };
    fields.reserve(4 + level.orderIds.size());
    for (OrderId order_id : level.orderIds) {
      fields.push_back(std::to_string(order_id));
    }
    write_line(out, fields);
  }
}

void write_order_book(std::ostream& out, const OrderBookSnapshot& symbol) {
  write_symbol_config(out, symbol.symbolConfig);
  write_price_levels(out, "bid_levels", "bid_level", symbol.bidLevels);
  write_price_levels(out, "ask_levels", "ask_level", symbol.askLevels);

  const auto orders = sorted_orders(symbol.activeOrders);
  write_line(out, {"active_orders", std::to_string(orders.size())});
  for (const auto& order : orders) {
    write_line(out,
               {"order",
                std::to_string(order.orderId),
                std::to_string(order.clientOrderId),
                std::to_string(order.userId),
                std::to_string(order.symbolId),
                enum_field(order.side),
                std::to_string(order.price.ticks()),
                std::to_string(order.originalQuantity.lots()),
                std::to_string(order.remainingQuantity.lots()),
                std::to_string(order.sequenceAccepted)});
  }

  const auto client_orders =
      sorted_client_order_map(symbol.clientOrderIdToOrderId);
  write_line(out,
             {"client_order_map", std::to_string(client_orders.size())});
  for (const auto& [client_order_id, order_id] : client_orders) {
    write_line(out,
               {"client_order",
                std::to_string(client_order_id),
                std::to_string(order_id)});
  }
}

void write_idempotency_snapshot(std::ostream& out,
                                const IdempotencySnapshot& snapshot) {
  const auto command_summaries =
      sorted_command_summaries(snapshot.processedCommandIds);
  write_line(out,
             {"idempotency_processed_commands",
              std::to_string(command_summaries.size())});
  for (const auto& [command_id, summary] : command_summaries) {
    write_line(out,
               {"processed_command",
                std::to_string(command_id),
                bool_field(summary.accepted),
                bool_field(summary.orderId.has_value()),
                summary.orderId.has_value() ? std::to_string(*summary.orderId)
                                            : "0",
                bool_field(summary.rejectReason.has_value()),
                summary.rejectReason.has_value()
                    ? enum_field(*summary.rejectReason)
                    : "0",
                std::to_string(summary.sequence)});
  }

  const auto client_orders =
      sorted_client_order_map(snapshot.clientOrderIdToOrderId);
  write_line(out,
             {"idempotency_client_orders",
              std::to_string(client_orders.size())});
  for (const auto& [client_order_id, order_id] : client_orders) {
    write_line(out,
               {"idempotency_client_order",
                std::to_string(client_order_id),
                std::to_string(order_id)});
  }

  const auto order_ids = sorted_order_ids(snapshot.orderIds);
  write_line(out, {"idempotency_order_ids", std::to_string(order_ids.size())});
  for (OrderId order_id : order_ids) {
    write_line(out, {"idempotency_order_id", std::to_string(order_id)});
  }
}

void write_metadata_store(std::ostream& out,
                          const EngineCheckpoint& checkpoint) {
  const auto entries = metadata_entries(checkpoint);
  write_line(out, {"metadata", std::to_string(entries.size())});

  for (const auto& metadata : entries) {
    write_line(out,
               {"metadata_entry",
                std::to_string(metadata.order_id),
                std::to_string(metadata.market_id),
                std::to_string(metadata.user_id),
                enum_field(metadata.side),
                std::to_string(metadata.original_quantity),
                std::to_string(metadata.remaining_quantity),
                bool_field(metadata.reduce_only),
                escape_field(metadata.margin_asset),
                std::to_string(metadata.reserved_margin_amount),
                std::to_string(metadata.remaining_reserved_margin),
                std::to_string(metadata.leverage),
                escape_field(metadata.reservation_id),
                escape_field(metadata.place_request_id),
                escape_field(metadata.place_idempotency_key),
                bool_field(metadata.place_input_id.has_value()),
                metadata.place_input_id.has_value()
                    ? escape_field(*metadata.place_input_id)
                    : "",
                std::to_string(metadata.reply_partition),
                std::to_string(metadata.core_client_order_id),
                std::to_string(metadata.core_place_command_id)});
  }
}

void write_processed_requests(
    std::ostream& out,
    std::string_view section_tag,
    const std::unordered_map<
        std::string,
        cex::runtime::EngineRuntimeProcessedRequestSnapshot>& entries) {
  const auto sorted = sorted_processed_requests(entries);
  write_line(out, {std::string(section_tag), std::to_string(sorted.size())});

  for (const auto& [key, processed] : sorted) {
    write_line(out,
               {"processed_request",
                escape_field(key),
                enum_field(processed.command_kind),
                escape_field(processed.topic),
                std::to_string(processed.partition),
                std::to_string(processed.offset),
                bool_field(processed.input_id.has_value()),
                processed.input_id.has_value()
                    ? escape_field(*processed.input_id)
                    : "",
                escape_field(processed.idempotency_key)});
  }
}

void write_mark_prices(
    std::ostream& out,
    const std::unordered_map<cex::adapter::MarketId,
                             cex::runtime::MarkPriceState>& entries) {
  const auto sorted = sorted_mark_prices(entries);
  write_line(out, {"mark_prices", std::to_string(sorted.size())});

  for (const auto& [market_id, state] : sorted) {
    write_line(out,
               {"mark_price_state",
                std::to_string(market_id),
                std::to_string(state.mark_price),
                std::to_string(state.index_price),
                std::to_string(state.source_timestamp_ms),
                std::to_string(state.published_at_ms),
                std::to_string(state.valid_until_ms),
                std::to_string(state.source_sequence),
                escape_field(state.source_status)});
  }
}

void write_funding_rates(
    std::ostream& out,
    const std::unordered_map<cex::adapter::MarketId,
                             cex::runtime::FundingRateState>& entries) {
  const auto sorted = sorted_funding_rates(entries);
  write_line(out, {"funding_rates", std::to_string(sorted.size())});

  for (const auto& [market_id, state] : sorted) {
    write_line(out,
               {"funding_rate_state",
                std::to_string(market_id),
                escape_field(state.funding_interval_id),
                std::to_string(state.rate),
                std::to_string(state.rate_scale),
                std::to_string(state.interval_start_ms),
                std::to_string(state.interval_end_ms),
                std::to_string(state.source_timestamp_ms)});
  }
}

void write_settled_funding_intervals(
    std::ostream& out,
    const cex::runtime::FundingSettlementSet& entries) {
  const auto sorted = sorted_settled_funding_intervals(entries);
  write_line(out,
             {"settled_funding_intervals", std::to_string(sorted.size())});

  for (const auto& key : sorted) {
    write_line(out,
               {"settled_funding_interval",
                std::to_string(key.market_id),
                escape_field(key.funding_interval_id)});
  }
}

void write_positions(std::ostream& out,
                     const cex::runtime::IsolatedPositionMap& entries) {
  const auto sorted = sorted_positions(entries);
  write_line(out, {"positions", std::to_string(sorted.size())});

  for (const auto& [key, state] : sorted) {
    write_line(out,
               {"position_state",
                std::to_string(key.user_id),
                std::to_string(key.market_id),
                std::to_string(state.user_id),
                std::to_string(state.market_id),
                std::to_string(state.signed_quantity),
                std::to_string(state.average_entry_price),
                escape_field(state.margin_asset),
                std::to_string(state.isolated_margin),
                std::to_string(state.leverage),
                std::to_string(state.updated_at_ms)});
  }
}

void write_risk_states(std::ostream& out,
                       const cex::runtime::IsolatedRiskMap& entries) {
  const auto sorted = sorted_risk_states(entries);
  write_line(out, {"risk_states", std::to_string(sorted.size())});

  for (const auto& [key, state] : sorted) {
    write_line(out,
               {"risk_state",
                std::to_string(key.user_id),
                std::to_string(key.market_id),
                std::to_string(state.user_id),
                std::to_string(state.market_id),
                escape_field(state.status),
                escape_field(state.margin_asset),
                std::to_string(state.signed_quantity),
                std::to_string(state.average_entry_price),
                std::to_string(state.mark_price),
                std::to_string(state.isolated_margin),
                std::to_string(state.unrealized_pnl),
                std::to_string(state.equity),
                std::to_string(state.maintenance_margin),
                std::to_string(state.margin_ratio),
                std::to_string(state.leverage),
                std::to_string(state.updated_at_ms)});
  }
}

void write_checkpoint(std::ostream& out, const EngineCheckpoint& checkpoint) {
  out << kMagic << '\n';

  write_line(out,
             {"schema_version", std::to_string(checkpoint.schema_version)});
  write_line(out, {"checkpoint_id", escape_field(checkpoint.checkpoint_id)});
  write_line(out,
             {"source_position",
              escape_field(checkpoint.source_position.topic),
              std::to_string(checkpoint.source_position.partition),
              std::to_string(checkpoint.source_position.next_offset)});
  write_line(out,
             {"sequence_state",
              std::to_string(checkpoint.core_snapshot.sequenceState.nextSequence),
              std::to_string(checkpoint.core_snapshot.sequenceState.nextTradeId),
              std::to_string(checkpoint.core_snapshot.sequenceState.nextEventId)});

  const auto symbols = sorted_symbol_snapshots(checkpoint.core_snapshot);
  write_line(out, {"symbols", std::to_string(symbols.size())});
  for (const auto& symbol : symbols) {
    write_order_book(out, symbol);
  }

  write_idempotency_snapshot(out,
                             checkpoint.core_snapshot.idempotencyState);

  const auto public_sequences =
      sorted_public_sequences(checkpoint.public_sequences);
  write_line(out,
             {"public_sequences", std::to_string(public_sequences.size())});
  for (const auto& [market_id, next_sequence] : public_sequences) {
    write_line(out,
               {"public_sequence",
                std::to_string(market_id),
                std::to_string(next_sequence)});
  }

  write_mark_prices(out, checkpoint.mark_prices);
  write_funding_rates(out, checkpoint.funding_rates);
  write_settled_funding_intervals(out,
                                  checkpoint.settled_funding_intervals);
  write_positions(out, checkpoint.positions);
  write_risk_states(out, checkpoint.risk_states);
  write_metadata_store(out, checkpoint);
  write_processed_requests(
      out, "processed_input_ids", checkpoint.processed_input_ids);
  write_processed_requests(out,
                           "processed_idempotency_keys",
                           checkpoint.processed_idempotency_keys);

  write_line(out, {"end"});
}

class CheckpointReader {
 public:
  explicit CheckpointReader(std::istream& in) : in_(in) {}

  [[nodiscard]] EngineCheckpoint read_checkpoint() {
    const auto magic = read_raw_line();
    if (magic != kMagic) {
      throw CheckpointParseError("invalid checkpoint magic");
    }

    EngineCheckpoint checkpoint;

    {
      const auto fields = read_fields("schema_version");
      require_field_count(fields, 2);
      checkpoint.schema_version = parse_integer<std::uint32_t>(fields[1]);
    }

    {
      const auto fields = read_fields("checkpoint_id");
      require_field_count(fields, 2);
      checkpoint.checkpoint_id = unescape_field(fields[1]);
    }

    {
      const auto fields = read_fields("source_position");
      require_field_count(fields, 4);
      checkpoint.source_position = CheckpointSourcePosition{
          .topic = unescape_field(fields[1]),
          .partition = parse_integer<std::int32_t>(fields[2]),
          .next_offset = parse_integer<std::int64_t>(fields[3]),
      };
    }

    {
      const auto fields = read_fields("sequence_state");
      require_field_count(fields, 4);
      checkpoint.core_snapshot.sequenceState = SequenceState{
          .nextSequence = parse_integer<EngineSequence>(fields[1]),
          .nextTradeId = parse_integer<TradeId>(fields[2]),
          .nextEventId = parse_integer<EventId>(fields[3]),
      };
    }

    checkpoint.core_snapshot.symbolSnapshots = read_symbols();
    checkpoint.core_snapshot.idempotencyState = read_idempotency_snapshot();
    checkpoint.public_sequences = read_public_sequences();

    auto section = read_fields_any();
    if (!section.empty() && section.front() == "mark_prices") {
      checkpoint.mark_prices = read_mark_prices(section);
      section = read_fields_any();
    }

    if (!section.empty() && section.front() == "funding_rates") {
      checkpoint.funding_rates = read_funding_rates(section);
      section = read_fields_any();
    }

    if (!section.empty() && section.front() == "settled_funding_intervals") {
      checkpoint.settled_funding_intervals =
          read_settled_funding_intervals(section);
      section = read_fields_any();
    }

    if (!section.empty() && section.front() == "positions") {
      checkpoint.positions = read_positions(section);
      section = read_fields_any();
    }

    if (!section.empty() && section.front() == "risk_states") {
      checkpoint.risk_states = read_risk_states(section);
      section = read_fields_any();
    }

    if (!section.empty() && section.front() == "metadata") {
      checkpoint.metadata_store =
          read_metadata_store(section);
    } else {
      throw CheckpointParseError("unexpected checkpoint section");
    }

    checkpoint.processed_input_ids =
        read_processed_requests("processed_input_ids");
    checkpoint.processed_idempotency_keys =
        read_processed_requests("processed_idempotency_keys");

    const auto end_fields = read_fields("end");
    require_field_count(end_fields, 1);
    return checkpoint;
  }

 private:
  [[nodiscard]] std::string read_raw_line() {
    std::string line;
    if (!std::getline(in_, line)) {
      throw CheckpointParseError("unexpected end of checkpoint file");
    }
    ++line_number_;
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    return line;
  }

  [[nodiscard]] std::vector<std::string> read_fields(
      std::string_view expected_tag) {
    auto fields = read_fields_any();
    if (fields.empty() || fields.front() != expected_tag) {
      throw CheckpointParseError("unexpected checkpoint section");
    }
    return fields;
  }

  [[nodiscard]] std::vector<std::string> read_fields_any() {
    return split_line(read_raw_line());
  }

  [[nodiscard]] std::size_t count_from_fields(
      const std::vector<std::string>& fields,
      std::string_view expected_tag) {
    if (fields.empty() || fields.front() != expected_tag) {
      throw CheckpointParseError("unexpected checkpoint section");
    }
    require_field_count(fields, 2);
    return parse_integer<std::size_t>(fields[1]);
  }

  [[nodiscard]] std::size_t read_count(std::string_view expected_tag) {
    return count_from_fields(read_fields(expected_tag), expected_tag);
  }

  [[nodiscard]] SymbolConfig read_symbol_config() {
    const auto fields = read_fields("symbol");
    require_field_count(fields, 18);

    return SymbolConfig{
        .symbolId = parse_integer<SymbolId>(fields[1]),
        .baseAssetId = parse_integer<AssetId>(fields[2]),
        .quoteAssetId = parse_integer<AssetId>(fields[3]),
        .tickSize = Price::from_ticks(parse_integer<std::int64_t>(fields[4])),
        .lotSize = Quantity::from_lots(parse_integer<std::uint64_t>(fields[5])),
        .minQuantity =
            Quantity::from_lots(parse_integer<std::uint64_t>(fields[6])),
        .maxQuantity =
            Quantity::from_lots(parse_integer<std::uint64_t>(fields[7])),
        .minPrice = Price::from_ticks(parse_integer<std::int64_t>(fields[8])),
        .maxPrice = Price::from_ticks(parse_integer<std::int64_t>(fields[9])),
        .ringCapacityTicks = parse_integer<std::uint64_t>(fields[10]),
        .thresholdPercentage = parse_integer<std::uint64_t>(fields[11]),
        .initialBaseTick = parse_integer<std::int64_t>(fields[12]),
        .priceScale = parse_uint8(fields[13]),
        .quantityScale = parse_uint8(fields[14]),
        .makerFeeRate = parse_integer<FeeRate>(fields[15]),
        .takerFeeRate = parse_integer<FeeRate>(fields[16]),
        .tradingEnabled = parse_bool(fields[17]),
    };
  }

  [[nodiscard]] std::vector<PriceLevelSnapshot> read_price_levels(
      std::string_view count_tag,
      std::string_view entry_tag) {
    const std::size_t count = read_count(count_tag);
    std::vector<PriceLevelSnapshot> levels;
    levels.reserve(count);

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields(entry_tag);
      if (fields.size() < 4) {
        throw CheckpointParseError("price level section is incomplete");
      }

      const std::size_t order_count = parse_integer<std::size_t>(fields[3]);
      if (fields.size() != 4 + order_count) {
        throw CheckpointParseError("price level order count mismatch");
      }

      PriceLevelSnapshot level{
          .price = Price::from_ticks(parse_integer<std::int64_t>(fields[1])),
          .totalQuantity =
              Quantity::from_lots(parse_integer<std::uint64_t>(fields[2])),
      };
      level.orderIds.reserve(order_count);
      for (std::size_t order_index = 0; order_index < order_count;
           ++order_index) {
        level.orderIds.push_back(
            parse_integer<OrderId>(fields[4 + order_index]));
      }
      levels.push_back(std::move(level));
    }

    return levels;
  }

  [[nodiscard]] std::vector<OrderSnapshot> read_orders() {
    const std::size_t count = read_count("active_orders");
    std::vector<OrderSnapshot> orders;
    orders.reserve(count);

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("order");
      require_field_count(fields, 10);

      orders.push_back(OrderSnapshot{
          .orderId = parse_integer<OrderId>(fields[1]),
          .clientOrderId = parse_integer<ClientOrderId>(fields[2]),
          .userId = parse_integer<UserId>(fields[3]),
          .symbolId = parse_integer<SymbolId>(fields[4]),
          .side = parse_side(fields[5]),
          .price = Price::from_ticks(parse_integer<std::int64_t>(fields[6])),
          .originalQuantity =
              Quantity::from_lots(parse_integer<std::uint64_t>(fields[7])),
          .remainingQuantity =
              Quantity::from_lots(parse_integer<std::uint64_t>(fields[8])),
          .sequenceAccepted = parse_integer<EngineSequence>(fields[9]),
      });
    }

    return orders;
  }

  [[nodiscard]] std::unordered_map<ClientOrderId, OrderId>
  read_client_order_map() {
    const std::size_t count = read_count("client_order_map");
    std::unordered_map<ClientOrderId, OrderId> entries;
    entries.reserve(count);

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("client_order");
      require_field_count(fields, 3);
      const auto client_order_id = parse_integer<ClientOrderId>(fields[1]);
      const auto order_id = parse_integer<OrderId>(fields[2]);
      if (!entries.emplace(client_order_id, order_id).second) {
        throw CheckpointParseError("duplicate client order id in checkpoint");
      }
    }

    return entries;
  }

  [[nodiscard]] std::vector<OrderBookSnapshot> read_symbols() {
    const std::size_t count = read_count("symbols");
    std::vector<OrderBookSnapshot> symbols;
    symbols.reserve(count);

    for (std::size_t index = 0; index < count; ++index) {
      auto config = read_symbol_config();
      OrderBookSnapshot symbol{
          .symbolId = config.symbolId,
          .symbolConfig = config,
          .bidLevels = read_price_levels("bid_levels", "bid_level"),
          .askLevels = read_price_levels("ask_levels", "ask_level"),
          .activeOrders = read_orders(),
          .clientOrderIdToOrderId = read_client_order_map(),
      };
      symbols.push_back(std::move(symbol));
    }

    return symbols;
  }

  [[nodiscard]] IdempotencySnapshot read_idempotency_snapshot() {
    IdempotencySnapshot snapshot;

    {
      const std::size_t count =
          read_count("idempotency_processed_commands");
      snapshot.processedCommandIds.reserve(count);

      for (std::size_t index = 0; index < count; ++index) {
        const auto fields = read_fields("processed_command");
        require_field_count(fields, 8);

        const auto command_id = parse_integer<CommandId>(fields[1]);
        std::optional<OrderId> order_id;
        if (parse_bool(fields[3])) {
          order_id = parse_integer<OrderId>(fields[4]);
        }

        std::optional<RejectReason> reject_reason;
        if (parse_bool(fields[5])) {
          reject_reason = parse_reject_reason(fields[6]);
        }

        CommandResultSummary summary{
            .commandId = command_id,
            .accepted = parse_bool(fields[2]),
            .orderId = order_id,
            .rejectReason = reject_reason,
            .sequence = parse_integer<EngineSequence>(fields[7]),
        };

        if (!snapshot.processedCommandIds.emplace(command_id, summary).second) {
          throw CheckpointParseError("duplicate processed command id");
        }
      }
    }

    {
      const std::size_t count = read_count("idempotency_client_orders");
      snapshot.clientOrderIdToOrderId.reserve(count);
      for (std::size_t index = 0; index < count; ++index) {
        const auto fields = read_fields("idempotency_client_order");
        require_field_count(fields, 3);
        const auto client_order_id = parse_integer<ClientOrderId>(fields[1]);
        const auto order_id = parse_integer<OrderId>(fields[2]);
        if (!snapshot.clientOrderIdToOrderId
                 .emplace(client_order_id, order_id)
                 .second) {
          throw CheckpointParseError(
              "duplicate idempotency client order id");
        }
      }
    }

    {
      const std::size_t count = read_count("idempotency_order_ids");
      snapshot.orderIds.reserve(count);
      for (std::size_t index = 0; index < count; ++index) {
        const auto fields = read_fields("idempotency_order_id");
        require_field_count(fields, 2);
        if (!snapshot.orderIds.emplace(parse_integer<OrderId>(fields[1]))
                 .second) {
          throw CheckpointParseError("duplicate idempotency order id");
        }
      }
    }

    return snapshot;
  }

  [[nodiscard]] std::unordered_map<cex::adapter::MarketId, EngineSequence>
  read_public_sequences() {
    const std::size_t count = read_count("public_sequences");
    std::unordered_map<cex::adapter::MarketId, EngineSequence> sequences;
    sequences.reserve(count);

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("public_sequence");
      require_field_count(fields, 3);
      const auto market_id = parse_integer<cex::adapter::MarketId>(fields[1]);
      const auto next_sequence = parse_integer<EngineSequence>(fields[2]);
      if (!sequences.emplace(market_id, next_sequence).second) {
        throw CheckpointParseError("duplicate public sequence market id");
      }
    }

    return sequences;
  }

  [[nodiscard]] std::unordered_map<cex::adapter::MarketId,
                                   cex::runtime::MarkPriceState>
  read_mark_prices(const std::vector<std::string>& count_fields) {
    const std::size_t count = count_from_fields(count_fields, "mark_prices");
    std::unordered_map<cex::adapter::MarketId,
                       cex::runtime::MarkPriceState>
        mark_prices;
    mark_prices.reserve(count);

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("mark_price_state");
      require_field_count(fields, 9);

      cex::runtime::MarkPriceState state{
          .market_id = parse_integer<cex::adapter::MarketId>(fields[1]),
          .mark_price = parse_integer<cex::adapter::AdapterPrice>(fields[2]),
          .index_price = parse_integer<cex::adapter::AdapterPrice>(fields[3]),
          .source_timestamp_ms =
              parse_integer<std::int64_t>(fields[4]),
          .published_at_ms = parse_integer<std::int64_t>(fields[5]),
          .valid_until_ms = parse_integer<std::int64_t>(fields[6]),
          .source_sequence = parse_integer<std::int64_t>(fields[7]),
          .source_status = unescape_field(fields[8]),
      };

      if (!mark_prices.emplace(state.market_id, std::move(state)).second) {
        throw CheckpointParseError("duplicate mark price market id");
      }
    }

    return mark_prices;
  }

  [[nodiscard]] std::unordered_map<cex::adapter::MarketId,
                                   cex::runtime::FundingRateState>
  read_funding_rates(const std::vector<std::string>& count_fields) {
    const std::size_t count =
        count_from_fields(count_fields, "funding_rates");
    std::unordered_map<cex::adapter::MarketId,
                       cex::runtime::FundingRateState>
        funding_rates;
    funding_rates.reserve(count);

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("funding_rate_state");
      require_field_count(fields, 8);

      cex::runtime::FundingRateState state{
          .market_id = parse_integer<cex::adapter::MarketId>(fields[1]),
          .funding_interval_id = unescape_field(fields[2]),
          .rate = parse_integer<std::int64_t>(fields[3]),
          .rate_scale = parse_integer<std::int64_t>(fields[4]),
          .interval_start_ms = parse_integer<std::int64_t>(fields[5]),
          .interval_end_ms = parse_integer<std::int64_t>(fields[6]),
          .source_timestamp_ms = parse_integer<std::int64_t>(fields[7]),
      };

      if (!funding_rates.emplace(state.market_id, std::move(state)).second) {
        throw CheckpointParseError("duplicate funding rate market id");
      }
    }

    return funding_rates;
  }

  [[nodiscard]] cex::runtime::FundingSettlementSet
  read_settled_funding_intervals(
      const std::vector<std::string>& count_fields) {
    const std::size_t count =
        count_from_fields(count_fields, "settled_funding_intervals");
    cex::runtime::FundingSettlementSet settled;

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("settled_funding_interval");
      require_field_count(fields, 3);

      cex::runtime::FundingSettlementKey key{
          .market_id = parse_integer<cex::adapter::MarketId>(fields[1]),
          .funding_interval_id = unescape_field(fields[2]),
      };

      if (!settled.insert(std::move(key)).second) {
        throw CheckpointParseError("duplicate settled funding interval");
      }
    }

    return settled;
  }

  [[nodiscard]] cex::runtime::IsolatedPositionMap read_positions(
      const std::vector<std::string>& count_fields) {
    const std::size_t count = count_from_fields(count_fields, "positions");
    cex::runtime::IsolatedPositionMap positions;

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("position_state");
      require_field_count(fields, 11);

      cex::runtime::PositionRiskKey key{
          .user_id = parse_integer<cex::adapter::AdapterUserId>(fields[1]),
          .market_id = parse_integer<cex::adapter::MarketId>(fields[2]),
      };
      cex::runtime::IsolatedPositionState state{
          .user_id = parse_integer<cex::adapter::AdapterUserId>(fields[3]),
          .market_id = parse_integer<cex::adapter::MarketId>(fields[4]),
          .signed_quantity = parse_integer<std::int64_t>(fields[5]),
          .average_entry_price =
              parse_integer<cex::adapter::AdapterPrice>(fields[6]),
          .margin_asset = unescape_field(fields[7]),
          .isolated_margin = parse_integer<std::int64_t>(fields[8]),
          .leverage = parse_integer<std::int32_t>(fields[9]),
          .updated_at_ms = parse_integer<std::int64_t>(fields[10]),
      };

      if (!positions.emplace(key, std::move(state)).second) {
        throw CheckpointParseError("duplicate position key");
      }
    }

    return positions;
  }

  [[nodiscard]] cex::runtime::IsolatedRiskMap read_risk_states(
      const std::vector<std::string>& count_fields) {
    const std::size_t count = count_from_fields(count_fields, "risk_states");
    cex::runtime::IsolatedRiskMap risk_states;

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("risk_state");
      if (fields.size() != 14 && fields.size() != 17) {
        throw CheckpointParseError("unexpected checkpoint field count");
      }

      cex::runtime::PositionRiskKey key{
          .user_id = parse_integer<cex::adapter::AdapterUserId>(fields[1]),
          .market_id = parse_integer<cex::adapter::MarketId>(fields[2]),
      };
      cex::runtime::IsolatedRiskState state{
          .user_id = parse_integer<cex::adapter::AdapterUserId>(fields[3]),
          .market_id = parse_integer<cex::adapter::MarketId>(fields[4]),
          .status = unescape_field(fields[5]),
          .margin_asset = unescape_field(fields[6]),
          .signed_quantity = parse_integer<std::int64_t>(fields[7]),
          .average_entry_price =
              parse_integer<cex::adapter::AdapterPrice>(fields[8]),
          .mark_price = parse_integer<cex::adapter::AdapterPrice>(fields[9]),
          .isolated_margin = parse_integer<std::int64_t>(fields[10]),
          .unrealized_pnl = parse_integer<std::int64_t>(fields[11]),
          .equity =
              fields.size() == 17
                  ? parse_integer<std::int64_t>(fields[12])
                  : parse_integer<std::int64_t>(fields[10]) +
                        parse_integer<std::int64_t>(fields[11]),
          .maintenance_margin =
              fields.size() == 17 ? parse_integer<std::int64_t>(fields[13])
                                  : 0,
          .margin_ratio =
              fields.size() == 17 ? parse_integer<std::int64_t>(fields[14])
                                  : 0,
          .leverage = parse_integer<std::int32_t>(
              fields.size() == 17 ? fields[15] : fields[12]),
          .updated_at_ms = parse_integer<std::int64_t>(
              fields.size() == 17 ? fields[16] : fields[13]),
      };

      if (!risk_states.emplace(key, std::move(state)).second) {
        throw CheckpointParseError("duplicate risk state key");
      }
    }

    return risk_states;
  }

  [[nodiscard]] cex::adapter::OrderMetadataStore read_metadata_store() {
    return read_metadata_store(read_fields("metadata"));
  }

  [[nodiscard]] cex::adapter::OrderMetadataStore read_metadata_store(
      const std::vector<std::string>& count_fields) {
    const std::size_t count = count_from_fields(count_fields, "metadata");
    cex::adapter::OrderMetadataStore store;

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("metadata_entry");
      if (fields.size() != 12 && fields.size() != 20) {
        throw CheckpointParseError("unexpected checkpoint field count");
      }

      if (fields.size() == 12) {
        cex::adapter::OrderMetadata metadata{
            .order_id = parse_integer<OrderId>(fields[1]),
            .market_id = parse_integer<cex::adapter::MarketId>(fields[2]),
            .user_id = parse_integer<cex::adapter::AdapterUserId>(fields[3]),
            .reservation_id = unescape_field(fields[4]),
            .place_request_id = unescape_field(fields[5]),
            .place_idempotency_key = unescape_field(fields[6]),
            .place_input_id = parse_bool(fields[7])
                                  ? std::optional<std::string>{
                                        unescape_field(fields[8])}
                                  : std::nullopt,
            .reply_partition = parse_integer<std::int32_t>(fields[9]),
            .core_client_order_id = parse_integer<ClientOrderId>(fields[10]),
            .core_place_command_id = parse_integer<CommandId>(fields[11]),
        };

        if (!store.insert(std::move(metadata))) {
          throw CheckpointParseError("duplicate metadata order id");
        }
        continue;
      }

      cex::adapter::OrderMetadata metadata{
          .order_id = parse_integer<OrderId>(fields[1]),
          .market_id = parse_integer<cex::adapter::MarketId>(fields[2]),
          .user_id = parse_integer<cex::adapter::AdapterUserId>(fields[3]),
          .side = parse_adapter_side(fields[4]),
          .original_quantity =
              parse_integer<cex::adapter::AdapterQuantity>(fields[5]),
          .remaining_quantity =
              parse_integer<cex::adapter::AdapterQuantity>(fields[6]),
          .reduce_only = parse_bool(fields[7]),
          .margin_asset = unescape_field(fields[8]),
          .reserved_margin_amount = parse_integer<std::int64_t>(fields[9]),
          .remaining_reserved_margin =
              parse_integer<std::int64_t>(fields[10]),
          .leverage = parse_integer<std::int32_t>(fields[11]),
          .reservation_id = unescape_field(fields[12]),
          .place_request_id = unescape_field(fields[13]),
          .place_idempotency_key = unescape_field(fields[14]),
          .place_input_id = parse_bool(fields[15])
                                ? std::optional<std::string>{
                                      unescape_field(fields[16])}
                                : std::nullopt,
          .reply_partition = parse_integer<std::int32_t>(fields[17]),
          .core_client_order_id = parse_integer<ClientOrderId>(fields[18]),
          .core_place_command_id = parse_integer<CommandId>(fields[19]),
      };

      if (!store.insert(std::move(metadata))) {
        throw CheckpointParseError("duplicate metadata order id");
      }
    }

    return store;
  }

  [[nodiscard]] std::unordered_map<
      std::string,
      cex::runtime::EngineRuntimeProcessedRequestSnapshot>
  read_processed_requests(std::string_view section_tag) {
    const std::size_t count = read_count(section_tag);
    std::unordered_map<
        std::string,
        cex::runtime::EngineRuntimeProcessedRequestSnapshot>
        processed;
    processed.reserve(count);

    for (std::size_t index = 0; index < count; ++index) {
      const auto fields = read_fields("processed_request");
      require_field_count(fields, 9);

      auto key = unescape_field(fields[1]);
      cex::runtime::EngineRuntimeProcessedRequestSnapshot snapshot{
          .command_kind = parse_runtime_command_kind(fields[2]),
          .topic = unescape_field(fields[3]),
          .partition = parse_integer<std::int32_t>(fields[4]),
          .offset = parse_integer<std::int64_t>(fields[5]),
          .input_id = parse_bool(fields[6])
                          ? std::optional<std::string>{
                                unescape_field(fields[7])}
                          : std::nullopt,
          .idempotency_key = unescape_field(fields[8]),
      };

      if (!processed.emplace(std::move(key), std::move(snapshot)).second) {
        throw CheckpointParseError("duplicate processed request key");
      }
    }

    return processed;
  }

  std::istream& in_;
  std::size_t line_number_{0};
};

[[nodiscard]] std::optional<EngineCheckpoint> read_checkpoint_file(
    const std::filesystem::path& path) {
  std::ifstream in(path);
  if (!in) {
    return std::nullopt;
  }

  try {
    CheckpointReader reader(in);
    return reader.read_checkpoint();
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

[[nodiscard]] std::optional<std::filesystem::path> latest_checkpoint_path(
    const std::filesystem::path& directory) {
  std::error_code error;
  if (!std::filesystem::is_directory(directory, error) || error) {
    return std::nullopt;
  }

  std::vector<std::filesystem::path> candidates;
  for (std::filesystem::directory_iterator it(directory, error), end;
       !error && it != end;
       it.increment(error)) {
    std::error_code entry_error;
    if (!it->is_regular_file(entry_error) || entry_error) {
      continue;
    }
    if (it->path().extension().string() == kCheckpointExtension) {
      candidates.push_back(it->path());
    }
  }

  if (error || candidates.empty()) {
    return std::nullopt;
  }

  std::sort(candidates.begin(),
            candidates.end(),
            [](const auto& left, const auto& right) {
              return left.filename().string() < right.filename().string();
            });
  return candidates.back();
}

}  // namespace

FileCheckpointStore::FileCheckpointStore(std::filesystem::path directory)
    : directory_(std::move(directory)) {}

void FileCheckpointStore::save(EngineCheckpoint checkpoint) {
  std::error_code error;
  std::filesystem::create_directories(directory_, error);
  if (error) {
    throw std::runtime_error("failed to create checkpoint directory: " +
                             error.message());
  }

  const auto filename = filename_for_checkpoint_id(checkpoint.checkpoint_id);
  const auto final_path = directory_ / filename;
  const auto temp_path = directory_ / (filename + ".tmp");

  {
    std::ofstream out(temp_path, std::ios::trunc);
    if (!out) {
      throw std::runtime_error("failed to open checkpoint temp file");
    }
    write_checkpoint(out, checkpoint);
    out.flush();
    if (!out) {
      throw std::runtime_error("failed to write checkpoint temp file");
    }
  }

  std::filesystem::rename(temp_path, final_path, error);
  if (error) {
    std::error_code cleanup_error;
    std::filesystem::remove(final_path, cleanup_error);
    error.clear();
    std::filesystem::rename(temp_path, final_path, error);
  }
  if (error) {
    std::error_code cleanup_error;
    std::filesystem::remove(temp_path, cleanup_error);
    throw std::runtime_error("failed to commit checkpoint file: " +
                             error.message());
  }
}

std::optional<EngineCheckpoint> FileCheckpointStore::load_latest() const {
  const auto latest_path = latest_checkpoint_path(directory_);
  if (!latest_path.has_value()) {
    return std::nullopt;
  }
  return read_checkpoint_file(*latest_path);
}

const std::filesystem::path& FileCheckpointStore::directory() const noexcept {
  return directory_;
}

}  // namespace cex::checkpoint
