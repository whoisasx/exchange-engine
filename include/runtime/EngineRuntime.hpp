#pragma once

#include <optional>
#include <set>
#include <string>
#include <unordered_map>

#include "adapter/EngineAdapter.hpp"
#include "core/EngineCore.hpp"
#include "runtime/EngineEventTranslator.hpp"
#include "runtime/EngineInputParser.hpp"
#include "runtime/EngineRuntimeConfig.hpp"

namespace cex::runtime {

struct EngineRuntimeProcessedRequestSnapshot {
  RuntimeCommandKind command_kind{RuntimeCommandKind::PlaceOrder};
  std::string topic{EngineInputTopic};
  std::int32_t partition{0};
  std::int64_t offset{0};
  std::optional<std::string> input_id;
  std::string idempotency_key;
};

struct FundingRateState {
  cex::adapter::MarketId market_id{0};
  std::string funding_interval_id;
  std::int64_t rate{0};
  std::int64_t rate_scale{0};
  std::int64_t interval_start_ms{0};
  std::int64_t interval_end_ms{0};
  std::int64_t source_timestamp_ms{0};
};

struct FundingSettlementKey {
  cex::adapter::MarketId market_id{0};
  std::string funding_interval_id;

  [[nodiscard]] friend bool operator==(const FundingSettlementKey&,
                                       const FundingSettlementKey&) = default;
  [[nodiscard]] friend bool operator<(const FundingSettlementKey& left,
                                      const FundingSettlementKey& right) {
    if (left.market_id != right.market_id) {
      return left.market_id < right.market_id;
    }
    return left.funding_interval_id < right.funding_interval_id;
  }
};

using FundingSettlementSet = std::set<FundingSettlementKey>;

struct EngineRuntimeStateSnapshot {
  EngineSnapshot core_snapshot;
  cex::adapter::OrderMetadataStore metadata_store;
  std::unordered_map<cex::adapter::MarketId, EngineSequence> public_sequences;
  std::unordered_map<cex::adapter::MarketId, MarkPriceState> mark_prices;
  std::unordered_map<cex::adapter::MarketId, FundingRateState> funding_rates;
  FundingSettlementSet settled_funding_intervals;
  IsolatedPositionMap positions;
  IsolatedRiskMap risk_states;
  std::unordered_map<std::string, EngineRuntimeProcessedRequestSnapshot>
      processed_input_ids;
  std::unordered_map<std::string, EngineRuntimeProcessedRequestSnapshot>
      processed_idempotency_keys;
};

class EngineRuntime {
 public:
  explicit EngineRuntime(EngineRuntimeConfig config = {});

  void add_symbol(const SymbolConfig& symbol_config);

  [[nodiscard]] EngineProcessResult process(
      const InboundEngineRecord& record,
      ProcessingMode mode = ProcessingMode::Live);
  [[nodiscard]] EngineProcessResult process_replay(
      const InboundEngineRecord& record);

  [[nodiscard]] const EngineCore& core() const noexcept;
  [[nodiscard]] const cex::adapter::OrderMetadataStore& metadata_store()
      const noexcept;
  [[nodiscard]] const cex::adapter::MarketSequenceGenerator& market_sequences()
      const noexcept;
  [[nodiscard]] const std::unordered_map<cex::adapter::MarketId,
                                         MarkPriceState>&
  mark_prices() const noexcept;
  [[nodiscard]] const std::unordered_map<cex::adapter::MarketId,
                                         FundingRateState>&
  funding_rates() const noexcept;
  [[nodiscard]] const FundingSettlementSet& settled_funding_intervals()
      const noexcept;
  [[nodiscard]] const IsolatedPositionMap& positions() const noexcept;
  [[nodiscard]] const IsolatedRiskMap& risk_states() const noexcept;
  [[nodiscard]] EngineRuntimeStateSnapshot snapshot_state() const;
  void restore_state(const EngineRuntimeStateSnapshot& snapshot);
  [[nodiscard]] EngineProcessResult evaluate_automatic_liquidations_for_market(
      cex::adapter::MarketId market_id,
      const InboundEngineRecord& source,
      ProcessingMode mode = ProcessingMode::Live);
  [[nodiscard]] EngineProcessResult evaluate_all_automatic_liquidations(
      const InboundEngineRecord& source,
      ProcessingMode mode = ProcessingMode::Live);

 private:
  struct ProcessedRuntimeRequest {
    RuntimeCommandKind command_kind{RuntimeCommandKind::PlaceOrder};
    std::string topic{EngineInputTopic};
    std::int32_t partition{0};
    std::int64_t offset{0};
    std::optional<std::string> input_id;
    std::string idempotency_key;
  };

  [[nodiscard]] std::optional<EngineProcessResult> duplicate_result_for(
      const std::optional<std::string>& input_id,
      const std::string& idempotency_key) const;
  [[nodiscard]] std::optional<EngineProcessResult>
  duplicate_input_result_for(
      const std::optional<std::string>& input_id) const;
  [[nodiscard]] EngineProcessResult make_duplicate_result(
      EngineDuplicateReason reason,
      const std::string& key,
      const ProcessedRuntimeRequest& original) const;
  void mark_processed(RuntimeCommandKind command_kind,
                      const InboundEngineRecord& record,
                      const std::optional<std::string>& input_id,
                      const std::string& idempotency_key);
  void mark_input_processed(RuntimeCommandKind command_kind,
                            const InboundEngineRecord& record,
                            const std::optional<std::string>& input_id);

  [[nodiscard]] EngineEventTranslationContext make_translation_context(
      const InboundEngineRecord& record,
      const cex::adapter::PlaceOrderInput& input,
      const cex::adapter::OrderMetadata& pending_metadata) const;
  [[nodiscard]] EngineEventTranslationContext make_translation_context(
      const InboundEngineRecord& record,
      const cex::adapter::CancelOrderInput& input) const;
  [[nodiscard]] EngineProcessResult execute_liquidation(
      const InboundEngineRecord& record,
      const cex::adapter::LiquidatePositionInput& input,
      bool emit_replies);
  [[nodiscard]] EngineProcessResult evaluate_automatic_liquidations_for_market(
      cex::adapter::MarketId market_id,
      const InboundEngineRecord& source,
      const std::optional<std::string>& source_input_id);
  [[nodiscard]] EngineProcessResult evaluate_all_automatic_liquidations(
      const InboundEngineRecord& source,
      const std::optional<std::string>& source_input_id);
  void append_mark_price_risk_updates(
      EngineProcessResult& result,
      const InboundEngineRecord& record,
      const std::optional<std::string>& input_id,
      cex::adapter::MarketId market_id,
      cex::adapter::AdapterPrice mark_price,
      std::int64_t updated_at_ms);

  EngineCore core_;
  cex::adapter::OrderMetadataStore metadata_store_;
  cex::adapter::MarketSequenceGenerator market_sequences_;
  EngineRuntimeConfig config_;
  EngineRuntimeClock clock_;
  EngineInputParser parser_;
  EngineEventTranslator translator_;
  std::unordered_map<cex::adapter::MarketId, MarkPriceState> mark_prices_;
  std::unordered_map<cex::adapter::MarketId, FundingRateState> funding_rates_;
  FundingSettlementSet settled_funding_intervals_;
  IsolatedPositionMap positions_;
  IsolatedRiskMap risk_states_;
  std::unordered_map<std::string, ProcessedRuntimeRequest>
      processed_input_ids_;
  std::unordered_map<std::string, ProcessedRuntimeRequest>
      processed_idempotency_keys_;
};

}  // namespace cex::runtime
