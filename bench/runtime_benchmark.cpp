#include "BenchSupport.hpp"
#include "Workloads.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "runtime/EngineOutbox.hpp"

namespace {

class CountingPublisher final : public cex::runtime::IEnginePublisher {
 public:
  std::optional<std::string> publish(
      const cex::runtime::EngineOutputRecord&,
      std::string_view serialized_json) override {
    ++records;
    bytes += serialized_json.size();
    return std::nullopt;
  }

  std::uint64_t records{0};
  std::uint64_t bytes{0};
};

[[nodiscard]] std::vector<cex::runtime::InboundEngineRecord> make_records(
    std::vector<std::string> values,
    std::int64_t first_offset) {
  std::vector<cex::runtime::InboundEngineRecord> records;
  records.reserve(values.size());
  std::int64_t offset = first_offset;
  for (auto& value : values) {
    records.push_back(cex::bench::runtime_record(std::move(value), offset));
    ++offset;
  }
  return records;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    auto options = cex::bench::parse_options(
        argc, argv, "runtime", true, false);
    const auto total_commands = options.warmup + options.commands;
    auto workload = cex::bench::make_runtime_json_workload(
        options.scenario, total_commands, options.book_depth, options.seed);
    auto setup_records = make_records(std::move(workload.setup), 0);
    auto measured_records =
        make_records(std::move(workload.measured),
                     static_cast<std::int64_t>(setup_records.size()));

    auto runtime = cex::bench::make_runtime();
    CountingPublisher publisher;
    cex::runtime::EngineOutbox outbox(publisher);

    for (const auto& record : setup_records) {
      auto result = runtime.process(record);
      if (options.include_output_serialization) {
        cex::bench::do_not_optimize(outbox.publish(result));
      }
    }

    for (std::uint64_t i = 0; i < options.warmup; ++i) {
      auto result =
          runtime.process(measured_records[static_cast<std::size_t>(i)]);
      if (options.include_output_serialization) {
        cex::bench::do_not_optimize(outbox.publish(result));
      }
    }

    publisher.records = 0;
    publisher.bytes = 0;
    std::uint64_t output_records = 0;
    std::vector<std::uint64_t> samples_ns;
    samples_ns.reserve(static_cast<std::size_t>(options.commands));
    const auto begin_all = cex::bench::Clock::now();
    for (std::uint64_t i = options.warmup; i < total_commands; ++i) {
      const auto begin = cex::bench::Clock::now();
      auto result =
          runtime.process(measured_records[static_cast<std::size_t>(i)]);
      if (options.include_output_serialization) {
        cex::bench::do_not_optimize(outbox.publish(result));
      }
      const auto end = cex::bench::Clock::now();
      output_records += static_cast<std::uint64_t>(result.replies.size() +
                                                   result.events.size());
      cex::bench::do_not_optimize(result);
      samples_ns.push_back(static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
              .count()));
    }
    const auto end_all = cex::bench::Clock::now();

    cex::bench::BenchmarkReport report{
        .options = options,
        .measured_commands = options.commands,
        .setup_commands = static_cast<std::uint64_t>(setup_records.size()),
        .output_records = output_records,
        .output_bytes = publisher.bytes,
        .duration_ms =
            std::chrono::duration<double, std::milli>(end_all - begin_all)
                .count(),
        .throughput_per_sec = 0.0,
        .latency_ns = cex::bench::summarize_latencies(std::move(samples_ns)),
    };
    report.throughput_per_sec =
        static_cast<double>(options.commands) / (report.duration_ms / 1000.0);
    cex::bench::fill_resource_usage(report);
    cex::bench::print_report_json(report);
  } catch (const std::exception& error) {
    std::cerr << "runtime benchmark failed: " << error.what() << '\n';
    return 1;
  }
  return 0;
}
