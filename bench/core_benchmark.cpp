#include "BenchSupport.hpp"
#include "Workloads.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <vector>

namespace {

[[nodiscard]] std::uint64_t event_count(
    const std::vector<EngineEvent>& events) {
  return static_cast<std::uint64_t>(events.size());
}

}  // namespace

int main(int argc, char** argv) {
  try {
    auto options = cex::bench::parse_options(argc, argv, "core");
    const auto total_commands = options.warmup + options.commands;
    const auto workload = cex::bench::make_core_workload(
        options.scenario, total_commands, options.book_depth, options.seed);

    EngineCore core;
    core.add_symbol(cex::bench::make_symbol());

    for (const auto& command : workload.setup) {
      cex::bench::do_not_optimize(core.process(command));
    }

    std::uint64_t output_records = 0;
    for (std::uint64_t i = 0; i < options.warmup; ++i) {
      cex::bench::do_not_optimize(
          core.process(workload.measured[static_cast<std::size_t>(i)]));
    }

    output_records = 0;
    std::vector<std::uint64_t> samples_ns;
    samples_ns.reserve(static_cast<std::size_t>(options.commands));
    const auto begin_all = cex::bench::Clock::now();
    for (std::uint64_t i = options.warmup; i < total_commands; ++i) {
      const auto begin = cex::bench::Clock::now();
      const auto events =
          core.process(workload.measured[static_cast<std::size_t>(i)]);
      const auto end = cex::bench::Clock::now();
      output_records += event_count(events);
      cex::bench::do_not_optimize(events);
      samples_ns.push_back(static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
              .count()));
    }
    const auto end_all = cex::bench::Clock::now();

    cex::bench::BenchmarkReport report{
        .options = options,
        .measured_commands = options.commands,
        .setup_commands = static_cast<std::uint64_t>(workload.setup.size()),
        .output_records = output_records,
        .output_bytes = 0,
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
    std::cerr << "core benchmark failed: " << error.what() << '\n';
    return 1;
  }
  return 0;
}
