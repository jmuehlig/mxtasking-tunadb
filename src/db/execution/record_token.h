#pragma once

#include "tile_mask.h"
#include <cstdint>
#include <db/data/pax_tile.h>
#include <db/data/row_tile.h>
#include <db/topology/physical_schema.h>
#include <mx/resource/ptr.h>
#include <mx/tasking/dataflow/token.h>
#include <mx/tasking/runtime.h>

namespace db::execution {
class RecordSet
{
public:
    [[nodiscard]] static RecordSet make_record_set(const topology::PhysicalSchema &schema,
                                                   const std::uint16_t worker_id)
    {
        return RecordSet{data::PaxTile::make(schema, true, worker_id)};
    }

    [[nodiscard]] static RecordSet make_empty() { return RecordSet{}; }

    [[nodiscard]] static RecordSet make_client_record_set(const topology::PhysicalSchema &schema)
    {
        return RecordSet{data::PaxTile::make_for_client(schema)};
    }

    explicit RecordSet(const mx::resource::ptr tile) noexcept : _tile(tile) {}

    RecordSet(RecordSet &&other) noexcept
        : _tile(std::exchange(other._tile, nullptr)), _secondary_input(other._secondary_input)
    {
    }

    ~RecordSet()
    {
        if (_tile != nullptr)
        {
            if (_tile.get<data::PaxTile>()->is_client_tile()) [[unlikely]]
            {
                std::free(_tile.get());
            }
            else if (_tile.get<data::PaxTile>()->is_temporary())
            {
                mx::tasking::runtime::delete_resource<data::PaxTile>(_tile);
            }
        }
    }

    RecordSet &operator=(RecordSet &&other) noexcept
    {
        _tile = std::exchange(other._tile, nullptr);
        _secondary_input = other._secondary_input;
        return *this;
    }

    [[nodiscard]] mx::resource::ptr tile() const noexcept { return _tile; }
    [[nodiscard]] mx::resource::ptr secondary_input() const noexcept { return _secondary_input; }
    void secondary_input(const mx::resource::ptr hash_table) noexcept { _secondary_input = hash_table; }

    static auto tile_offset() { return offsetof(RecordSet, _tile); }

private:
    RecordSet() noexcept = default;

    /// The tile where the records are stored.
    mx::resource::ptr _tile;

    /// Secondary input (e.g., hash table).
    mx::resource::ptr _secondary_input{nullptr};
};

using RecordToken = mx::tasking::dataflow::Token<RecordSet>;

[[nodiscard]] static inline RecordToken make_empty_token()
{
    return RecordToken{RecordSet::make_empty(), mx::tasking::annotation{mx::tasking::annotation::local}};
}
} // namespace db::execution