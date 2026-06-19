#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "services/ProtocolMessage.hpp"

namespace services {

enum class FanoutRouteKind {
  PrivateUser,
  Market,
};

struct FanoutRoute {
  FanoutRouteKind kind{FanoutRouteKind::Market};
  std::uint64_t user_id{0};
  std::uint64_t market_id{0};
};

struct FanoutEvent {
  FanoutRoute route;
  std::string outbound_type;
  ProtocolMessage source;
};

enum class WebsocketFanoutStatus {
  Ignored,
  Routed,
  Invalid,
};

struct WebsocketFanoutResult {
  WebsocketFanoutStatus status{WebsocketFanoutStatus::Ignored};
  std::vector<FanoutEvent> events;
  std::string reason;
};

class IWebsocketSink {
public:
  virtual ~IWebsocketSink() = default;

  // Concrete websocket servers should translate this route into authenticated
  // socket sessions. This scaffold intentionally does not own sockets.
  virtual void publish(const FanoutEvent& event) = 0;
};

class WebsocketFanout {
public:
  explicit WebsocketFanout(IWebsocketSink& sink);

  // Contract: wallet.events are private account updates routed only by
  // payload.user_id. This includes AccountDeltaApplied and every other wallet
  // event because wallet events are the balance/accounting source of truth.
  [[nodiscard]] WebsocketFanoutResult route(const ProtocolMessage& message);

private:
  IWebsocketSink& sink_;
};

}  // namespace services
