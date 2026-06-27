#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "checkpoint/EngineCheckpoint.hpp"

namespace cex::checkpoint {

inline constexpr std::string_view CheckpointFileExtension = ".checkpoint";

[[nodiscard]] std::string checkpoint_filename_for_id(
    std::string_view checkpoint_id);

[[nodiscard]] std::string serialize_checkpoint(
    const EngineCheckpoint& checkpoint);

[[nodiscard]] EngineCheckpoint deserialize_checkpoint(std::string_view text);

[[nodiscard]] std::optional<EngineCheckpoint> try_deserialize_checkpoint(
    std::string_view text);

}  // namespace cex::checkpoint
