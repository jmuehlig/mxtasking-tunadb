#pragma once

#include "table.h"
#include <db/udf/descriptor.h>
#include <db/util/tile_sample.h>
#include <mx/util/core_set.h>
#include <perf/counter.h>
#include <perf/sample.h>
#include <string>
#include <unordered_map>

namespace db::topology {
class Database
{
public:
    Database() noexcept : _profiling_counter(perf::CounterDescription::CYCLE_ACTIVITY_STALLS_MEM_ANY)
    {
        //        _profiling_counter.open();
        //        _profiling_counter.start();
    }
    ~Database()
    {
        //        _profiling_counter.stop();
        //        _profiling_counter.close();
    }

    [[nodiscard]] bool is_table(const std::string &table_name) const { return _tables.contains(table_name); }
    [[nodiscard]] bool is_user_defined_function(const std::string &udf_name) const
    {
        return _user_defined_functions.contains(udf_name);
    }

    const Table &operator[](const std::string &table_name) const { return _tables.at(table_name); }
    Table &operator[](const std::string &table_name) { return _tables.at(table_name); }

    Table &insert(std::string &&table_name, PhysicalSchema &&schema) noexcept
    {
        auto table = Table{std::string(table_name), std::move(schema)};
        auto [table_iterator, _] = _tables.insert(std::make_pair(std::move(table_name), std::move(table)));

        /// Add the first storage tile.
        table_iterator->second.initialize();

        return table_iterator->second;
    }

    void insert(udf::Descriptor &&function)
    {
        _user_defined_functions.insert(std::make_pair(std::string{function.name()}, std::move(function)));
    }

    [[nodiscard]] const udf::Descriptor &user_defined_function(const std::string &function_name) const
    {
        return _user_defined_functions.at(function_name);
    }

    [[nodiscard]] const std::unordered_map<std::string, Table> &tables() const noexcept { return _tables; };
    [[nodiscard]] const perf::Counter &profiling_counter() const noexcept { return _profiling_counter; }

    void update_core_mapping(const mx::util::core_set &new_core_set)
    {
        for (auto &[_, table] : _tables)
        {
            table.update_core_mapping(new_core_set);
        }
    }

    [[nodiscard]] std::unordered_map<std::string, util::TileSample> map_to_tiles(
        const perf::AggregatedSamples &samples) const
    {
        /**
         * Tile Name (e.g., table name): std::string
         * columns:
         *      cache lines: std::uint64_t
         *
         * example:
         *   lineitem:
         *   0: [1,2,3]
         *   1: [1,0,0]
         *
         */

        /// Map from tile name to tile size in byte
        auto table_tile_sizes = std::unordered_map<std::string, std::uint64_t>{};
        for (const auto &[name, table] : _tables)
        {
            table_tile_sizes.insert(std::make_pair(name, data::PaxTile::size(table.schema())));
        }

        auto tile_samples = std::unordered_map<std::string, util::TileSample>{};
        for (const auto &[addr, count] : samples.samples())
        {
            for (const auto &[name, table] : _tables)
            {
                const auto tile_size = table_tile_sizes.at(name);

                for (const auto tile : table.tiles())
                {
                    const auto tile_ptr = std::uintptr_t(tile.get());

                    if (addr >= tile_ptr && addr <= (tile_ptr + tile_size))
                    {
                        if (tile_samples.contains(name) == false)
                        {
                            tile_samples.insert(std::make_pair(name, util::TileSample{table.schema()}));
                        }

                        if (addr < (tile_ptr + sizeof(data::PaxTile)))
                        {
                            tile_samples.at(name).increment();
                        }
                        else
                        {
                            const auto offset = addr - tile_ptr - sizeof(data::PaxTile);

                            auto column = 0U;
                            for (; column < table.schema().size() - 1U; ++column)
                            {
                                if (offset >= table.schema().pax_offset(column) &&
                                    offset < table.schema().pax_offset(column + 1U))
                                {
                                    break;
                                }
                            }

                            //                            const auto cache_line = (offset -
                            //                            table.schema().pax_offset(column)) /
                            //                            mx::system::cache::line_size();
                            //                            ++tables.at(name)[column][cache_line];
                            tile_samples.at(name).columns()[column].increment(offset -
                                                                              table.schema().pax_offset(column));
                        }

                        goto next_addr;
                    }
                }
            }
        next_addr:
            continue;
        }

        return tile_samples;
    }

private:
    std::unordered_map<std::string, Table> _tables;
    std::unordered_map<std::string, udf::Descriptor> _user_defined_functions;
    perf::Counter _profiling_counter;
};
} // namespace db::topology