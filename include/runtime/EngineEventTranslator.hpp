#pragma once

#include <optional>
#include <string>
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

class EngineEventTranslator {
 public:
  [[nodiscard]] EngineProcessResult translate(
      const std::vector<EngineEvent>& core_events,
      const EngineEventTranslationContext& context,
      const cex::adapter::OrderMetadataStore& metadata_store,
      cex::adapter::MarketSequenceGenerator& market_sequences,
      const EngineRuntimeClock& clock) const;
};

}  // namespace cex::runtime
