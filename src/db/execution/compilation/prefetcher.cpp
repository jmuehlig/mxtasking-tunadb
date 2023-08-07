#include "prefetcher.h"
#include <db/data/pax_tile.h>

using namespace db::execution::compilation;

std::uint8_t PrefetchCallbackGenerator::produce(flounder::Program &program, const topology::PhysicalSchema &tile_schema)
{
    auto terms = std::vector<std::tuple<std::uint16_t, std::uint16_t, float>>{};
    terms.reserve(tile_schema.size());

    for (auto i = 0U; i < tile_schema.size(); ++i)
    {
        terms.emplace_back(i, tile_schema.type(i).size(), 1.0);
    }

    return PrefetchCallbackGenerator::produce(program, tile_schema, std::move(terms));
}

std::uint8_t PrefetchCallbackGenerator::produce(flounder::Program &program, const topology::PhysicalSchema &tile_schema,
                                                std::unordered_map<std::uint16_t, float> &&prevalent_indices)
{
    auto terms = std::vector<std::tuple<std::uint16_t, std::uint16_t, float>>{};
    std::transform(prevalent_indices.begin(), prevalent_indices.end(), std::back_inserter(terms),
                   [&tile_schema](auto &item) {
                       const auto type = tile_schema.type(std::get<0>(item));
                       return std::make_tuple(std::get<0>(item), type.size(), std::get<1>(item));
                   });

    return PrefetchCallbackGenerator::produce(program, tile_schema, std::move(terms));
}

std::uint8_t PrefetchCallbackGenerator::produce(
    flounder::Program &program, const topology::PhysicalSchema &tile_schema,
    std::vector<std::tuple<std::uint16_t, std::uint16_t, float>> &&prevalent_indices)
{
    auto offsets_to_prefetch = std::vector<std::uint32_t>{};
    offsets_to_prefetch.reserve(32U);

    /// Prefetch the header.
    offsets_to_prefetch.push_back(0U);

    /// Check if we can prefetch the entire tile.
    if (PrefetchCallbackGenerator::is_prefetch_entirely(prevalent_indices))
    {
        for (const auto &term : prevalent_indices)
        {
            const auto count_cache_lines =
                PrefetchCallbackGenerator::cache_lines_to_prefetch(std::get<1>(term), config::tuples_per_tile());

            const auto column_offset = tile_schema.pax_offset(std::get<0>(term)) + sizeof(data::PaxTile);
            for (auto i = 0U; i < count_cache_lines; ++i)
            {
                offsets_to_prefetch.emplace_back(column_offset + (i * mx::system::cache::line_size()));
            }
        }

        return PrefetchCallbackGenerator::produce(program, std::move(offsets_to_prefetch));
    }

    /// Prefetch only prevalent attributes entirely.
    std::sort(prevalent_indices.begin(), prevalent_indices.end(),
              [](const auto &left, const auto &right) { return std::get<2>(left) < std::get<2>(right); });

    /// Prefetch all but the prevalent attribute.
    if (std::get<2>(prevalent_indices.front()) < 1.0F)
    {
        const auto most_frequent_predicate = prevalent_indices.front();
        prevalent_indices.erase(prevalent_indices.begin());

        if (PrefetchCallbackGenerator::is_prefetch_entirely(prevalent_indices))
        {
            for (const auto &term : prevalent_indices)
            {
                const auto count_cache_lines =
                    PrefetchCallbackGenerator::cache_lines_to_prefetch(std::get<1>(term), config::tuples_per_tile());

                const auto column_offset = tile_schema.pax_offset(std::get<0>(term)) + sizeof(data::PaxTile);
                for (auto i = 0U; i < count_cache_lines; ++i)
                {
                    offsets_to_prefetch.emplace_back(column_offset + (i * mx::system::cache::line_size()));
                }
            }

            return PrefetchCallbackGenerator::produce(program, std::move(offsets_to_prefetch));
        }

        /// Prefetching only filtered accesses did not work.
        prevalent_indices.insert(prevalent_indices.begin(), most_frequent_predicate);
    }

    /// If there is at least one prevalent attribute, prefetch that.
    if (PrefetchCallbackGenerator::is_prefetch_only_prevalent(prevalent_indices))
    {
        auto prefetched_prevalend_indices = std::unordered_set<std::uint16_t>{};
        prefetched_prevalend_indices.reserve(prevalent_indices.size());

        auto count = 0U;
        for (const auto &term : prevalent_indices)
        {
            const auto count_cache_lines =
                PrefetchCallbackGenerator::cache_lines_to_prefetch(std::get<1>(term), config::tuples_per_tile());
            if (count + count_cache_lines <= PrefetchCallbackGenerator::MAX_CACHE_LINES)
            {
                const auto column_offset = tile_schema.pax_offset(std::get<0>(term)) + sizeof(data::PaxTile);
                for (auto i = 0U; i < count_cache_lines; ++i)
                {
                    offsets_to_prefetch.emplace_back(column_offset + (i * mx::system::cache::line_size()));
                }

                prefetched_prevalend_indices.insert(std::get<0>(term));
            }
            else
            {
                /// We do not want to prefetch the second attribute but not the first.
                break;
            }

            count += count_cache_lines;

            if (count >= PrefetchCallbackGenerator::MAX_CACHE_LINES)
            {
                return PrefetchCallbackGenerator::produce(program, std::move(offsets_to_prefetch));
            }
        }

        /// Remove all prefetched indices.
        prevalent_indices.erase(std::remove_if(prevalent_indices.begin(), prevalent_indices.end(),
                                               [&prefetched_prevalend_indices](const auto &term) {
                                                   return prefetched_prevalend_indices.find(std::get<0>(term)) !=
                                                          prefetched_prevalend_indices.end();
                                               }),
                                prevalent_indices.end());
    }

    /// If there is space to prefetch some more iterations, go for it.
    if (offsets_to_prefetch.size() < PrefetchCallbackGenerator::MAX_CACHE_LINES)
    {
        const auto iterations_to_prefetch = PrefetchCallbackGenerator::iterations_to_prefetch(
            PrefetchCallbackGenerator::MAX_CACHE_LINES - offsets_to_prefetch.size(), prevalent_indices);
        if (iterations_to_prefetch > 0U)
        {
            /// Prefetch all attributes partly.
            for (const auto &term : prevalent_indices)
            {
                const auto cache_lines_to_prefetch =
                    PrefetchCallbackGenerator::cache_lines_to_prefetch(std::get<1>(term), iterations_to_prefetch);
                const auto column_offset = tile_schema.pax_offset(std::get<0>(term)) + sizeof(data::PaxTile);

                for (auto i = 0U; i < cache_lines_to_prefetch; ++i)
                {
                    offsets_to_prefetch.emplace_back(column_offset + (i * mx::system::cache::line_size()));
                }
            }
        }
    }

    return PrefetchCallbackGenerator::produce(program, std::move(offsets_to_prefetch));
}

std::uint8_t PrefetchCallbackGenerator::produce(flounder::Program &program, std::vector<std::uint32_t> &&offsets)
{
    std::sort(offsets.begin(), offsets.end());

    /// Emit prefetch code.
    {
        auto prefetch_context_guard = flounder::ContextGuard{program, "Prefetch"};

        auto resource_address_vreg = program.vreg("resource_addr");
        program << program.request_vreg64(resource_address_vreg) << program.get_arg0(resource_address_vreg);

        for (const auto offset : offsets)
        {
            program << program.prefetch(program.mem(resource_address_vreg, offset, flounder::RegisterWidth::r64));
        }

        program << program.clear(resource_address_vreg);
    }

    return offsets.size();
}

std::uint16_t PrefetchCallbackGenerator::iterations_to_prefetch(
    const std::uint16_t remaining_cache_lines,
    const std::vector<std::tuple<std::uint16_t, std::uint16_t, float>> &indices) noexcept
{
    const auto max_element = *std::max_element(indices.begin(), indices.end(), [](const auto &left, const auto &right) {
        return std::get<1>(left) < std::get<1>(right);
    });

    const auto iteration_steps = std::uint16_t(mx::system::cache::line_size() / std::get<1>(max_element));
    auto iterations = 0U;

    while (iterations < config::tuples_per_tile())
    {
        const auto cache_lines =
            PrefetchCallbackGenerator::cache_lines_to_prefetch(indices, iterations + iteration_steps);
        if (cache_lines == 0U || cache_lines >= remaining_cache_lines)
        {
            return iterations;
        }

        iterations += iteration_steps;
    }

    return config::tuples_per_tile();
}

bool PrefetchCallbackGenerator::is_prefetch_entirely(
    const std::vector<std::tuple<std::uint16_t, std::uint16_t, float>> &indices) noexcept
{
    const auto count_total_cache_lines =
        PrefetchCallbackGenerator::cache_lines_to_prefetch(indices, config::tuples_per_tile());

    return count_total_cache_lines < PrefetchCallbackGenerator::MAX_CACHE_LINES;
}

bool PrefetchCallbackGenerator::is_prefetch_only_prevalent(
    const std::vector<std::tuple<std::uint16_t, std::uint16_t, float>> &indices) noexcept
{
    if constexpr (config::is_prefer_prevalent_for_prefetching())
    {
        return std::get<2>(indices.front()) < .12F;
    }
    else
    {
        return false;
    }
}