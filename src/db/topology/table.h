#pragma once
#include "physical_schema.h"
#include <db/config.h>
#include <db/data/pax_tile.h>
#include <db/statistic/statistics.h>
#include <mx/system/cache.h>
#include <mx/tasking/runtime.h>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace db::topology {
class Table
{
public:
    Table(std::string &&name, PhysicalSchema &&schema) noexcept
        : _name(std::move(name)), _schema(std::move(schema)), _statistics(_schema.size())
    {
    }

    Table(Table &&other) noexcept
        : _name(std::move(other._name)), _schema(std::move(other._schema)), _statistics(std::move(other._statistics)),
          _tiles(std::move(other._tiles)), _tile_index(std::move(other._tile_index)),
          _next_worker_id(other._next_worker_id.load())
    {
    }

    Table(const Table &) = delete;

    ~Table() = default;

    [[nodiscard]] const std::string &name() const noexcept { return _name; }
    [[nodiscard]] const PhysicalSchema &schema() const noexcept { return _schema; }

    [[nodiscard]] const statistic::Statistics &statistics() const noexcept { return _statistics; }
    [[nodiscard]] statistic::Statistics &statistics() noexcept { return _statistics; }

    [[nodiscard]] const std::vector<mx::resource::ptr> &tiles() const noexcept { return _tiles; }
    [[nodiscard]] const std::unordered_map<std::uint16_t, std::vector<mx::resource::ptr>> &tiles_index() const noexcept
    {
        return _tile_index;
    }

    void initialize() { _tiles.emplace_back(make_tile()); }

    void emplace_back(data::PaxTile *tile)
    {
        if (mx::resource::ptr_cast<data::PaxTile>(_tiles.back())->full())
        {
            _tiles.emplace_back(make_tile());
        }

        const auto count_records = tile->size();
        auto inserted = 0U;
        while (inserted < count_records)
        {
            auto *persistent_tile = mx::resource::ptr_cast<data::PaxTile>(_tiles.back());
            inserted += persistent_tile->emplace_back(tile, inserted);
            if (persistent_tile->full())
            {
                _tiles.emplace_back(make_tile());
            }
        }
    }

    void update_core_mapping(const mx::util::core_set &new_core_set)
    {
        _tile_index.clear();
        auto next_worker_id = 0UL;
        for (auto &tile_ptr : _tiles)
        {
            const auto mapped_worker_id = next_worker_id++ % new_core_set.count_cores();

            auto info = tile_ptr.info();
            info.worker_id(mapped_worker_id);
            tile_ptr.reset(info);

            /// Rebuild tile index.
            auto iterator = _tile_index.find(mapped_worker_id);
            if (iterator == _tile_index.end())
            {
                auto tiles = std::vector<mx::resource::ptr>{};
                tiles.reserve(1024U);
                iterator = _tile_index.insert(std::make_pair(mapped_worker_id, std::move(tiles))).first;
            }
            iterator->second.emplace_back(tile_ptr);
        }
        _next_worker_id.store(next_worker_id);
    }

private:
    /// Name of the table.
    std::string _name;

    /// Schema of the table.
    PhysicalSchema _schema;

    /// Statistics per column.
    statistic::Statistics _statistics;

    /// List of tiles.
    std::vector<mx::resource::ptr> _tiles;

    /// Index for tiles indexed by worker id.
    std::unordered_map<std::uint16_t, std::vector<mx::resource::ptr>> _tile_index;

    /// Incrementable int to distribute tiles round robin around all workers.
    alignas(mx::system::cache::line_size()) std::atomic_uint64_t _next_worker_id{0U};

    [[nodiscard]] mx::resource::ptr make_tile()
    {
        /// Map tiles round robin around channels.
        const auto mapping_id = std::uint16_t(_next_worker_id.fetch_add(1U) % mx::tasking::runtime::workers());

        auto tile = data::PaxTile::make(_schema, false, mapping_id);

        auto iterator = _tile_index.find(mapping_id);
        if (iterator == _tile_index.end())
        {
            auto tiles = std::vector<mx::resource::ptr>{};
            tiles.reserve(1024U);
            iterator = _tile_index.insert(std::make_pair(mapping_id, std::move(tiles))).first;
        }
        iterator->second.emplace_back(tile);

        return tile;
    }
};
} // namespace db::topology