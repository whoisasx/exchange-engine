#include "BenchSupport.hpp"
#include "Workloads.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "broker/RedpandaEngineApp.hpp"

namespace {

class EmptyConsumer final : public cex::broker::IEngineInputConsumer {
 public:
  std::optional<cex::broker::ConsumedRecord> poll() override {
    return std::nullopt;
  }

  std::optional<std::string> seek(const std::string&,
                                  std::int32_t,
                                  std::int64_t) override {
    return std::nullopt;
  }

  cex::broker::BrokerWatermarkResult get_watermark(const std::string&,
                                                   std::int32_t) override {
    return cex::broker::BrokerWatermarkResult{
        .offsets = cex::broker::BrokerWatermarkOffsets{.low = 0, .high = 0},
        .error = std::nullopt,
    };
  }
};

class CountingProducer final : public cex::broker::IEngineRecordProducer {
 public:
  std::optional<std::string> produce(
      const cex::broker::ProduceRequest& request) override {
    ++records;
    bytes += request.value.size();
    return std::nullopt;
  }

  std::uint64_t records{0};
  std::uint64_t bytes{0};
};

class CountingCommitter final : public cex::broker::IEngineOffsetCommitter {
 public:
  std::optional<std::string> commit(
      const cex::broker::OffsetCommitRequest&) override {
    ++commits;
    return std::nullopt;
  }

  std::uint64_t commits{0};
};

[[nodiscard]] std::vector<cex::broker::ConsumedRecord> make_records(
    std::vector<std::string> values,
    std::int64_t first_offset) {
  std::vector<cex::broker::ConsumedRecord> records;
  records.reserve(values.size());
  std::int64_t offset = first_offset;
  for (auto& value : values) {
    records.push_back(cex::bench::broker_record(std::move(value), offset));
    ++offset;
  }
  return records;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    auto options = cex::bench::parse_options(
        argc, argv, "broker_loop", false, true);
    options.include_output_serialization = true;
    const auto total_commands = options.warmup + options.commands;
    auto workload = cex::bench::make_runtime_json_workload(
        options.scenario, total_commands, options.book_depth, options.seed);
    auto setup_records = make_records(std::move(workload.setup), 0);
    auto measured_records =
        make_records(std::move(workload.measured),
                     static_cast<std::int64_t>(setup_records.size()));

    auto runtime = cex::bench::make_runtime();
    EmptyConsumer consumer;
    CountingProducer producer;
    CountingCommitter committer;
    cex::broker::EnginePreCommitHook hook;
    if (options.checkpoint_delay_us > 0) {
      hook = [delay = options.checkpoint_delay_us](
                 const cex::broker::ConsumedRecord&,
                 const cex::runtime::EngineRuntime&) -> std::optional<std::string> {
        std::this_thread::sleep_for(std::chrono::microseconds(delay));
        return std::nullopt;
      };
    }

    cex::broker::RedpandaEngineApp app(
        consumer,
        producer,
        committer,
        runtime,
        std::move(hook));

    for (const auto& record : setup_records) {
      auto result = app.consume(record);
      if (!result.ok()) {
        throw std::runtime_error("setup consume failed: " + result.error);
      }
    }

    for (std::uint64_t i = 0; i < options.warmup; ++i) {
      auto result =
          app.consume(measured_records[static_cast<std::size_t>(i)]);
      if (!result.ok()) {
        throw std::runtime_error("warmup consume failed: " + result.error);
      }
    }

    producer.records = 0;
    producer.bytes = 0;
    committer.commits = 0;
    std::vector<std::uint64_t> samples_ns;
    samples_ns.reserve(static_cast<std::size_t>(options.commands));
    const auto begin_all = cex::bench::Clock::now();
    for (std::uint64_t i = options.warmup; i < total_commands; ++i) {
      const auto begin = cex::bench::Clock::now();
      auto result =
          app.consume(measured_records[static_cast<std::size_t>(i)]);
      const auto end = cex::bench::Clock::now();
      if (!result.ok()) {
        throw std::runtime_error("consume failed: " + result.error);
      }
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
        .output_records = producer.records,
        .output_bytes = producer.bytes,
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
    std::cerr << "broker loop benchmark failed: " << error.what() << '\n';
    return 1;
  }
  return 0;
}
