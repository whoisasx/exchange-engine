#include "runtime/EngineOutbox.hpp"

#include <string>
#include <utility>
#include <vector>

#include "runtime/EngineOutputSerializer.hpp"

namespace cex::runtime {
namespace {

void publish_records(const std::vector<EngineOutputRecord>& records,
                     IEnginePublisher& publisher,
                     EnginePublishResult& result) {
  for (const auto& record : records) {
    ++result.attempted;
    const std::string serialized_json = serialize_engine_output_record(record);
    if (auto error = publisher.publish(record, serialized_json);
        error.has_value()) {
      result.failures.push_back(EnginePublishFailure{
          .topic = record.topic,
          .key = record.key,
          .partition = record.partition,
          .type = record.type,
          .error = std::move(*error),
      });
      continue;
    }
    ++result.published;
  }
}

}  // namespace

EngineOutbox::EngineOutbox(IEnginePublisher& publisher)
    : publisher_(publisher) {}

EnginePublishResult EngineOutbox::publish(
    const EngineProcessResult& process_result) const {
  EnginePublishResult result;
  if (process_result.status == EngineProcessStatus::Duplicate ||
      process_result.status == EngineProcessStatus::Rejected ||
      process_result.empty()) {
    return result;
  }

  publish_records(process_result.replies, publisher_, result);
  publish_records(process_result.events, publisher_, result);
  return result;
}

}  // namespace cex::runtime
