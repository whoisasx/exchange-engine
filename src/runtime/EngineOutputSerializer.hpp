#pragma once

#include <string>

#include "runtime/EngineRuntimeConfig.hpp"

namespace cex::runtime {

[[nodiscard]] std::string serialize_engine_output_record(
    const EngineOutputRecord& record);

}  // namespace cex::runtime
