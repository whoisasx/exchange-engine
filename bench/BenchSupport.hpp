#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/resource.h>
#include <vector>

namespace cex::bench {

using Clock = std::chrono::steady_clock;

struct BenchmarkOptions {
  std::string component;
  std::string scenario{"mixed"};
  std::uint64_t commands{100'000};
  std::uint64_t warmup{5'000};
  std::uint64_t book_depth{10'000};
  std::uint64_t seed{1};
  bool include_output_serialization{false};
  std::uint64_t checkpoint_delay_us{0};
};

struct Percentiles {
  std::uint64_t p50{0};
  std::uint64_t p90{0};
  std::uint64_t p95{0};
  std::uint64_t p99{0};
  std::uint64_t p999{0};
  std::uint64_t max{0};
};

struct BenchmarkReport {
  BenchmarkOptions options;
  std::uint64_t measured_commands{0};
  std::uint64_t setup_commands{0};
  std::uint64_t output_records{0};
  std::uint64_t output_bytes{0};
  double duration_ms{0.0};
  double throughput_per_sec{0.0};
  Percentiles latency_ns;
  long user_cpu_ms{0};
  long system_cpu_ms{0};
  long max_rss_kb{0};
};

[[nodiscard]] inline std::uint64_t parse_u64(std::string_view text,
                                             std::string_view label) {
  if (text.empty()) {
    throw std::invalid_argument(std::string(label) + " must not be empty");
  }

  std::uint64_t value = 0;
  for (const char ch : text) {
    if (ch < '0' || ch > '9') {
      throw std::invalid_argument(std::string(label) +
                                  " must be an unsigned integer");
    }
    const auto digit = static_cast<std::uint64_t>(ch - '0');
    if (value >
        (std::numeric_limits<std::uint64_t>::max() - digit) / 10U) {
      throw std::invalid_argument(std::string(label) + " is too large");
    }
    value = value * 10U + digit;
  }
  return value;
}

[[nodiscard]] inline BenchmarkOptions parse_options(
    int argc,
    char** argv,
    std::string component,
    bool allow_serialization_flag = false,
    bool allow_checkpoint_flag = false) {
  BenchmarkOptions options;
  options.component = std::move(component);

  for (int i = 1; i < argc; ++i) {
    const std::string_view arg(argv[i]);
    const auto require_value = [&](std::string_view name) -> std::string_view {
      if (i + 1 >= argc) {
        throw std::invalid_argument(std::string(name) + " requires a value");
      }
      ++i;
      return std::string_view(argv[i]);
    };

    if (arg == "--scenario") {
      options.scenario = std::string(require_value(arg));
    } else if (arg == "--commands") {
      options.commands = parse_u64(require_value(arg), arg);
    } else if (arg == "--warmup") {
      options.warmup = parse_u64(require_value(arg), arg);
    } else if (arg == "--book-depth") {
      options.book_depth = parse_u64(require_value(arg), arg);
    } else if (arg == "--seed") {
      options.seed = parse_u64(require_value(arg), arg);
    } else if (arg == "--include-output-serialization" &&
               allow_serialization_flag) {
      options.include_output_serialization = true;
    } else if (arg == "--checkpoint-delay-us" && allow_checkpoint_flag) {
      options.checkpoint_delay_us = parse_u64(require_value(arg), arg);
    } else if (arg == "--help" || arg == "-h") {
      std::cout
          << "Usage: " << argv[0]
          << " [--scenario mixed|place_only|match_heavy|cancel_heavy|"
             "reject_path|deep_book]"
          << " [--commands N] [--warmup N] [--book-depth N] [--seed N]";
      if (allow_serialization_flag) {
        std::cout << " [--include-output-serialization]";
      }
      if (allow_checkpoint_flag) {
        std::cout << " [--checkpoint-delay-us N]";
      }
      std::cout << '\n';
      std::exit(0);
    } else {
      throw std::invalid_argument("unknown argument: " + std::string(arg));
    }
  }

  if (options.commands == 0) {
    throw std::invalid_argument("--commands must be greater than zero");
  }
  return options;
}

[[nodiscard]] inline std::string escape_json(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.size() + 8);
  for (const unsigned char ch : value) {
    switch (ch) {
      case '"':
        escaped += "\\\"";
        break;
      case '\\':
        escaped += "\\\\";
        break;
      case '\b':
        escaped += "\\b";
        break;
      case '\f':
        escaped += "\\f";
        break;
      case '\n':
        escaped += "\\n";
        break;
      case '\r':
        escaped += "\\r";
        break;
      case '\t':
        escaped += "\\t";
        break;
      default:
        if (ch < 0x20U) {
          constexpr char hex[] = "0123456789abcdef";
          escaped += "\\u00";
          escaped.push_back(hex[ch >> 4U]);
          escaped.push_back(hex[ch & 0x0FU]);
        } else {
          escaped.push_back(static_cast<char>(ch));
        }
        break;
    }
  }
  return escaped;
}

[[nodiscard]] inline std::uint64_t percentile(
    const std::vector<std::uint64_t>& sorted_samples,
    long double percentile_value) {
  if (sorted_samples.empty()) {
    return 0;
  }

  const long double rank =
      percentile_value *
      static_cast<long double>(sorted_samples.size() - 1U);
  const auto index = static_cast<std::size_t>(rank + 0.5L);
  return sorted_samples[std::min(index, sorted_samples.size() - 1U)];
}

[[nodiscard]] inline Percentiles summarize_latencies(
    std::vector<std::uint64_t> samples) {
  std::sort(samples.begin(), samples.end());
  return Percentiles{
      .p50 = percentile(samples, 0.500L),
      .p90 = percentile(samples, 0.900L),
      .p95 = percentile(samples, 0.950L),
      .p99 = percentile(samples, 0.990L),
      .p999 = percentile(samples, 0.999L),
      .max = samples.empty() ? 0 : samples.back(),
  };
}

[[nodiscard]] inline long timeval_to_ms(const timeval& value) {
  return static_cast<long>(value.tv_sec * 1000L + value.tv_usec / 1000L);
}

[[nodiscard]] inline long current_max_rss_kb(const rusage& usage) {
#if defined(__APPLE__)
  return static_cast<long>(usage.ru_maxrss / 1024L);
#else
  return static_cast<long>(usage.ru_maxrss);
#endif
}

inline void fill_resource_usage(BenchmarkReport& report) {
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) != 0) {
    return;
  }

  report.user_cpu_ms = timeval_to_ms(usage.ru_utime);
  report.system_cpu_ms = timeval_to_ms(usage.ru_stime);
  report.max_rss_kb = current_max_rss_kb(usage);
}

inline void print_report_json(const BenchmarkReport& report) {
  std::cout << std::fixed << std::setprecision(3);
  std::cout << "{\n";
  std::cout << "  \"component\": \"" << escape_json(report.options.component)
            << "\",\n";
  std::cout << "  \"scenario\": \"" << escape_json(report.options.scenario)
            << "\",\n";
  std::cout << "  \"commands\": " << report.measured_commands << ",\n";
  std::cout << "  \"warmup_commands\": " << report.options.warmup << ",\n";
  std::cout << "  \"setup_commands\": " << report.setup_commands << ",\n";
  std::cout << "  \"book_depth\": " << report.options.book_depth << ",\n";
  std::cout << "  \"seed\": " << report.options.seed << ",\n";
  std::cout << "  \"include_output_serialization\": "
            << (report.options.include_output_serialization ? "true"
                                                            : "false")
            << ",\n";
  std::cout << "  \"checkpoint_delay_us\": "
            << report.options.checkpoint_delay_us << ",\n";
  std::cout << "  \"duration_ms\": " << report.duration_ms << ",\n";
  std::cout << "  \"throughput_per_sec\": " << report.throughput_per_sec
            << ",\n";
  std::cout << "  \"output_records\": " << report.output_records << ",\n";
  std::cout << "  \"output_bytes\": " << report.output_bytes << ",\n";
  std::cout << "  \"latency_ns\": {\n";
  std::cout << "    \"p50\": " << report.latency_ns.p50 << ",\n";
  std::cout << "    \"p90\": " << report.latency_ns.p90 << ",\n";
  std::cout << "    \"p95\": " << report.latency_ns.p95 << ",\n";
  std::cout << "    \"p99\": " << report.latency_ns.p99 << ",\n";
  std::cout << "    \"p999\": " << report.latency_ns.p999 << ",\n";
  std::cout << "    \"max\": " << report.latency_ns.max << "\n";
  std::cout << "  },\n";
  std::cout << "  \"process\": {\n";
  std::cout << "    \"user_cpu_ms\": " << report.user_cpu_ms << ",\n";
  std::cout << "    \"system_cpu_ms\": " << report.system_cpu_ms << ",\n";
  std::cout << "    \"max_rss_kb\": " << report.max_rss_kb << "\n";
  std::cout << "  }\n";
  std::cout << "}\n";
}

template <typename Value>
inline void do_not_optimize(const Value& value) {
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" : : "g"(&value) : "memory");
#else
  (void)value;
#endif
}

}  // namespace cex::bench
