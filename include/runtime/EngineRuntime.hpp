#pragma once

#include "adapter/EngineAdapter.hpp"
#include "core/EngineCore.hpp"
#include "runtime/EngineEventTranslator.hpp"
#include "runtime/EngineInputParser.hpp"
#include "runtime/EngineRuntimeConfig.hpp"

namespace cex::runtime {

class EngineRuntime {
 public:
  explicit EngineRuntime(EngineRuntimeConfig config = {});

  void add_symbol(const SymbolConfig& symbol_config);

  [[nodiscard]] EngineProcessResult process(const InboundEngineRecord& record);

  [[nodiscard]] const EngineCore& core() const noexcept;
  [[nodiscard]] const cex::adapter::OrderMetadataStore& metadata_store()
      const noexcept;
  [[nodiscard]] const cex::adapter::MarketSequenceGenerator& market_sequences()
      const noexcept;

 private:
  [[nodiscard]] EngineEventTranslationContext make_translation_context(
      const InboundEngineRecord& record,
      const cex::adapter::PlaceOrderInput& input,
      const cex::adapter::OrderMetadata& pending_metadata) const;
  [[nodiscard]] EngineEventTranslationContext make_translation_context(
      const InboundEngineRecord& record,
      const cex::adapter::CancelOrderInput& input) const;

  EngineCore core_;
  cex::adapter::OrderMetadataStore metadata_store_;
  cex::adapter::MarketSequenceGenerator market_sequences_;
  EngineRuntimeConfig config_;
  EngineRuntimeClock clock_;
  EngineInputParser parser_;
  EngineEventTranslator translator_;
};

}  // namespace cex::runtime
