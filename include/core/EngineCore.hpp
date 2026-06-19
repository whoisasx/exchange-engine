#pragma once

#include <unordered_map>
#include <vector>

#include "Command.hpp"
#include "Event.hpp"
#include "IdempotencyIndex.hpp"
#include "OrderBook.hpp"
#include "SequenceGenerator.hpp"
#include "Snapshot.hpp"
#include "SymbolConfig.hpp"
#include "Types.hpp"

struct EngineCore{
  std::unordered_map<SymbolId,OrderBook> orderBooks;
  std::unordered_map<SymbolId,SymbolConfig> symbolConfigs;
  SequenceGenerator sequenceGenerator;
  IdempotencyIndex idempotencyIndex;

  void add_symbol(SymbolConfig symbolConfig);
  std::vector<EngineEvent> process(const EngineCommand& command);
  std::vector<EngineEvent> process_many(const std::vector<EngineCommand>& commands);

  [[nodiscard]] OrderBook* get_order_book(SymbolId symbolId);
  [[nodiscard]] const OrderBook* get_order_book(SymbolId symbolId) const;
  [[nodiscard]] bool has_symbol(SymbolId symbolId) const;
  [[nodiscard]] EngineSnapshot snapshot() const;
  void restore(const EngineSnapshot& snapshot);
};
