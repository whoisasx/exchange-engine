#include "services/WebsocketFanout.hpp"

#include <utility>

namespace services {
namespace {

FanoutEvent account_event(std::uint64_t user_id, const ProtocolMessage& source) {
  return FanoutEvent{
      FanoutRoute{FanoutRouteKind::PrivateUser, user_id, 0},
      "AccountEvent",
      source,
  };
}

FanoutEvent market_event(std::uint64_t market_id, const ProtocolMessage& source) {
  return FanoutEvent{
      FanoutRoute{FanoutRouteKind::Market, 0, market_id},
      "MarketEvent",
      source,
  };
}

}  // namespace

WebsocketFanout::WebsocketFanout(IWebsocketSink& sink) : sink_(sink) {}

WebsocketFanoutResult WebsocketFanout::route(const ProtocolMessage& message) {
  std::vector<FanoutEvent> events;

  if (message.topic == WalletEventsTopic) {
    const auto user_id = payload_u64(message, "user_id");
    if (!user_id.has_value()) {
      return WebsocketFanoutResult{WebsocketFanoutStatus::Invalid,
                                   {},
                                   "wallet.events require payload.user_id"};
    }
    events.push_back(account_event(*user_id, message));
  } else if (message.topic == EngineEventsTopic) {
    const auto user_id = payload_u64(message, "user_id");
    const auto market_id = payload_u64(message, "market_id");
    if (user_id.has_value()) {
      events.push_back(account_event(*user_id, message));
    }
    if (market_id.has_value()) {
      events.push_back(market_event(*market_id, message));
    }
  } else {
    return WebsocketFanoutResult{WebsocketFanoutStatus::Ignored, {}, {}};
  }

  if (events.empty()) {
    return WebsocketFanoutResult{WebsocketFanoutStatus::Ignored, {}, {}};
  }

  for (const auto& event : events) {
    sink_.publish(event);
  }

  return WebsocketFanoutResult{WebsocketFanoutStatus::Routed,
                               std::move(events),
                               {}};
}

}  // namespace services
