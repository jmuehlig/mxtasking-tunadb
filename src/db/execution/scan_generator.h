#pragma once

#include "record_token.h"
#include <db/data/row_tile.h>
#include <db/topology/table.h>
#include <mx/tasking/annotation.h>
#include <mx/tasking/dataflow/token_generator.h>
#include <mx/tasking/prefetch_descriptor.h>
#include <ranges>

namespace db::execution {
class ScanGenerator final : public mx::tasking::dataflow::TokenGenerator<RecordSet>
{
public:
    constexpr explicit ScanGenerator(const topology::Table &table) noexcept : _scanned_table(table) {}

    constexpr ScanGenerator(const topology::Table &table, mx::tasking::PrefetchDescriptor prefetch_descriptor) noexcept
        : _prefetch_descriptor(prefetch_descriptor), _scanned_table(table)
    {
    }

    ~ScanGenerator() noexcept override = default;

    void prefetch(const mx::tasking::PrefetchDescriptor descriptor) noexcept
    {
        this->_prefetch_descriptor = descriptor;
    }

    [[nodiscard]] std::vector<RecordToken> generate(const std::uint16_t worker_id) override
    {
        auto tokens = std::vector<RecordToken>{};

        const auto &tile_index = _scanned_table.tiles_index();
        if (auto iterator = tile_index.find(worker_id); iterator != tile_index.end())
        {
            const auto &tiles = iterator->second;
            tokens.reserve(tiles.size());

            for (const auto tile : tiles)
            {
                const auto annotation = mx::tasking::annotation{mx::tasking::annotation::access_intention::readonly,
                                                                tile, _prefetch_descriptor};

                /// For compilation, we do not need the initial tile size.
                tokens.emplace_back(RecordSet{tile}, annotation);
            }
        }

        return tokens;
    }

    [[nodiscard]] std::uint64_t count() override { return _scanned_table.tiles().size(); }

    //    [[nodiscard]] mx::tasking::PrefetchMask &prefetch_mask() noexcept { return _prefetch_mask; }

private:
    mx::tasking::PrefetchDescriptor _prefetch_descriptor;
    const topology::Table &_scanned_table;
};

class DisponsableGenerator final : public mx::tasking::dataflow::TokenGenerator<RecordSet>
{
public:
    constexpr explicit DisponsableGenerator() noexcept = default;

    ~DisponsableGenerator() noexcept override = default;

    [[nodiscard]] std::vector<RecordToken> generate(const std::uint16_t /*worker_id*/) override
    {
        auto tokens = std::vector<RecordToken>{};
        tokens.emplace_back(make_empty_token());
        return tokens;
    }

    [[nodiscard]] std::uint64_t count() override { return 1U; }
};
} // namespace db::execution