#pragma once
#include"Types.hpp"

struct SequenceState {
  EngineSequence nextSequence;
  TradeId nextTradeId;
  EventId nextEventId;
};

struct SequenceGenerator {
  EngineSequence nextSequence=1;
  TradeId nextTradeId=1;
  EventId nextEventId=1;

  EngineSequence next_sequence();
  TradeId next_trade_id();
  EventId next_event_id();

  [[nodiscard]] SequenceState snapshot() const;
  void restore(const SequenceState& state);
};
