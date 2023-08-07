#pragma once

#include <cstdint>
#include <db/topology/physical_schema.h>
#include <flounder/program.h>
#include <mx/system/cache.h>
#include <optional>
#include <unordered_map>

namespace db::execution::compilation {
class PrefetchCallbackGenerator
{
private:
    static constexpr auto inline MAX_CACHE_LINES = 17U;

public:
    [[nodiscard]] static std::uint8_t produce(flounder::Program &program, const topology::PhysicalSchema &tile_schema);

    [[nodiscard]] static std::uint8_t produce(flounder::Program &program, const topology::PhysicalSchema &tile_schema,
                                              std::unordered_map<std::uint16_t, float> &&prevalent_indices);

private:
    [[nodiscard]] static std::uint8_t produce(
        flounder::Program &program, const topology::PhysicalSchema &tile_schema,
        std::vector<std::tuple<std::uint16_t, std::uint16_t, float>> &&prevalent_indices);

    [[nodiscard]] static std::uint8_t produce(flounder::Program &program, std::vector<std::uint32_t> &&offsets);

    [[nodiscard]] static std::uint16_t cache_lines_to_prefetch(const std::uint16_t type_size,
                                                               const std::uint16_t iterations) noexcept
    {
        const auto bytes = type_size * iterations;
        return std::ceil(bytes / double(mx::system::cache::line_size()));
    }

    [[nodiscard]] static std::uint16_t cache_lines_to_prefetch(
        const std::vector<std::tuple<std::uint16_t, std::uint16_t, float>> &indices,
        const std::uint16_t iterations) noexcept
    {
        return std::accumulate(indices.begin(), indices.end(), 0U, [iterations](const auto sum, const auto &term) {
            return sum + PrefetchCallbackGenerator::cache_lines_to_prefetch(std::get<1>(term), iterations);
        });
    }

    [[nodiscard]] static std::uint16_t iterations_to_prefetch(
        std::uint16_t remaining_cache_lines,
        const std::vector<std::tuple<std::uint16_t, std::uint16_t, float>> &indices) noexcept;

    [[nodiscard]] static bool is_prefetch_entirely(
        const std::vector<std::tuple<std::uint16_t, std::uint16_t, float>> &indices) noexcept;

    [[nodiscard]] static bool is_prefetch_only_prevalent(
        const std::vector<std::tuple<std::uint16_t, std::uint16_t, float>> &indices) noexcept;
};
} // namespace db::execution::compilation