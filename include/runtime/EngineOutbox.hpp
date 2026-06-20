#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "runtime/EngineRuntimeConfig.hpp"

namespace cex::runtime {

struct EnginePublishFailure {
  std::string topic;
  std::string key;
  std::optional<std::int32_t> partition;
  std::string type;
  std::string error;
};

struct EnginePublishResult {
  std::size_t attempted{0};
  std::size_t published{0};
  std::vector<EnginePublishFailure> failures;

  [[nodiscard]] bool ok() const noexcept {
    return failures.empty();
  }
};

class IEnginePublisher {
 public:
  virtual ~IEnginePublisher() = default;

  [[nodiscard]] virtual std::optional<std::string> publish(
      const EngineOutputRecord& record,
      std::string_view serialized_json) = 0;
};

class EngineOutbox {
 public:
  explicit EngineOutbox(IEnginePublisher& publisher);

  [[nodiscard]] EnginePublishResult publish(
      const EngineProcessResult& process_result) const;

 private:
  IEnginePublisher& publisher_;
};

}  // namespace cex::runtime
