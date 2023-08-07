#pragma once

#include <cstdint>
#include <db/config.h>
#include <db/data/row_record_view.h>
#include <db/data/row_tile.h>
#include <db/execution/compilation/record_vector.h>
#include <db/topology/physical_schema.h>
#include <mx/resource/ptr.h>
#include <mx/tasking/runtime.h>
#include <unordered_map>

namespace db::execution::compilation {
class LocalAggregationResult
{
public:
    LocalAggregationResult(topology::PhysicalSchema &&schema, const std::uint16_t partitions)
        : _schema(std::move(schema)), _tile(data::RowTile::make(_schema, true, 0U))
    {
        auto *tile = _tile.get<data::RowTile>();
        for (auto i = 0U; i < partitions; ++i)
        {
            std::ignore = tile->allocate();
        }
    }

    ~LocalAggregationResult()
    {
        if (_tile != nullptr)
        {
            mx::tasking::runtime::delete_resource<data::RowTile>(_tile);
        }
    }

    [[nodiscard]] data::RowRecordView at(const std::uint16_t index) const noexcept
    {
        return _tile.get<data::RowTile>()->record(index);
    }

    [[nodiscard]] data::RowTile *tile() const noexcept { return _tile.get<data::RowTile>(); }
    [[nodiscard]] std::size_t size() const noexcept { return _tile.get<data::RowTile>()->size(); }

    [[nodiscard]] std::size_t size_in_bytes() const noexcept
    {
        return _tile.get<data::RowTile>()->size() * _schema.row_size();
    }

private:
    /// Schema of the aggregation.
    topology::PhysicalSchema _schema;

    /// Storage for local aggregations. Each core/channel uses it's own slot in the channel.
    mx::resource::ptr _tile;
};
} // namespace db::execution::compilation