#include "SequenceGenerator.hpp"

EngineSequence SequenceGenerator::next_sequence(){
  return nextSequence++;
}

TradeId SequenceGenerator::next_trade_id(){
  return nextTradeId++;
}

EventId SequenceGenerator::next_event_id(){
  return nextEventId++;
}

SequenceState SequenceGenerator::snapshot() const{
  return SequenceState{
    nextSequence,
    nextTradeId,
    nextEventId,
  };
}

void SequenceGenerator::restore(const SequenceState& state){
  nextSequence=state.nextSequence;
  nextTradeId=state.nextTradeId;
  nextEventId=state.nextEventId;
}
