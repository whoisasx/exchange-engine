#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "adapter/EngineAdapter.hpp"
#include "core/Event.hpp"
#include "runtime/EngineRuntimeConfig.hpp"

namespace cex::runtime {

enum class RuntimeCommandKind {
  PlaceOrder,
  CancelOrder,
  MarkPriceUpdated,
  FundingRateUpdated,
};

struct EngineEventTranslationContext {
  RuntimeCommandKind command_kind{RuntimeCommandKind::PlaceOrder};
  InboundEngineRecord source;
  std::string request_id;
  std::optional<std::string> input_id;
  std::int32_t reply_partition{0};
  cex::adapter::AdapterOrderId order_id{0};
  cex::adapter::MarketId market_id{0};
  bool can_open_resting_order{false};
  std::optional<cex::adapter::OrderMetadata> pending_metadata;
};

struct PositionRiskKey {
  cex::adapter::AdapterUserId user_id{0};
  cex::adapter::MarketId market_id{0};

  [[nodiscard]] friend bool operator==(const PositionRiskKey&,
                                       const PositionRiskKey&) = default;
  [[nodiscard]] friend bool operator<(const PositionRiskKey& left,
                                      const PositionRiskKey& right) {
    if (left.user_id != right.user_id) {
      return left.user_id < right.user_id;
    }
    return left.market_id < right.market_id;
  }
};

struct IsolatedPositionState {
  cex::adapter::AdapterUserId user_id{0};
  cex::adapter::MarketId market_id{0};
  std::int64_t signed_quantity{0};
  cex::adapter::AdapterPrice average_entry_price{0};
  std::string margin_asset;
  std::int64_t isolated_margin{0};
  std::int32_t leverage{0};
  std::int64_t updated_at_ms{0};

  [[nodiscard]] friend bool operator==(const IsolatedPositionState&,
                                       const IsolatedPositionState&) = default;
};

struct IsolatedRiskState {
  cex::adapter::AdapterUserId user_id{0};
  cex::adapter::MarketId market_id{0};
  std::string status{"FLAT"};
  std::string margin_asset;
  std::int64_t signed_quantity{0};
  cex::adapter::AdapterPrice average_entry_price{0};
  cex::adapter::AdapterPrice mark_price{0};
  std::int64_t isolated_margin{0};
  std::int64_t unrealized_pnl{0};
  std::int64_t equity{0};
  std::int64_t maintenance_margin{0};
  std::int64_t margin_ratio{0};
  std::int32_t leverage{0};
  std::int64_t updated_at_ms{0};

  [[nodiscard]] friend bool operator==(const IsolatedRiskState&,
                                       const IsolatedRiskState&) = default;
};

using IsolatedPositionMap =
    std::map<PositionRiskKey, IsolatedPositionState>;
using IsolatedRiskMap = std::map<PositionRiskKey, IsolatedRiskState>;

struct MarkPriceState {
  cex::adapter::MarketId market_id{0};
  cex::adapter::AdapterPrice mark_price{0};
  cex::adapter::AdapterPrice index_price{0};
  std::int64_t source_timestamp_ms{0};
  std::int64_t published_at_ms{0};
  std::int64_t valid_until_ms{0};
  std::int64_t source_sequence{0};
  std::string source_status;
};

class EngineEventTranslator {
 public:
  [[nodiscard]] EngineProcessResult translate(
      const std::vector<EngineEvent>& core_events,
      EngineEventTranslationContext& context,
      cex::adapter::OrderMetadataStore& metadata_store,
      IsolatedPositionMap& positions,
      IsolatedRiskMap& risk_states,
      const std::unordered_map<cex::adapter::MarketId, MarkPriceState>&
          mark_prices,
      cex::adapter::MarketSequenceGenerator& market_sequences,
      const EngineRuntimeClock& clock) const;
};

}  // namespace cex::runtime
