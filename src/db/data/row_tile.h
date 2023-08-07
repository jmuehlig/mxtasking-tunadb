#pragma once
#include "row_record_view.h"
#include "tile_type.h"
#include <cstdint>
#include <cstdlib>
#include <db/config.h>
#include <db/topology/physical_schema.h>
#include <mx/system/cache.h>
#include <mx/tasking/runtime.h>
#include <optional>

namespace db::data {
class alignas(mx::system::cache::line_size()) RowTile
{
public:
    [[nodiscard]] static mx::resource::ptr make(const topology::PhysicalSchema &schema, const bool is_temporary,
                                                const std::uint16_t worker_id)
    {
        /// Size for tile object + size for records.
        const auto tile_size = sizeof(RowTile) + config::tuples_per_tile() * schema.row_size();

        return mx::tasking::runtime::new_resource<data::RowTile>(
            tile_size,
            mx::resource::annotation{worker_id, mx::synchronization::isolation_level::Exclusive,
                                     mx::synchronization::protocol::Queue},
            static_cast<AllocationType>(is_temporary), schema);
    }

    [[nodiscard]] static mx::resource::ptr make_for_client(const topology::PhysicalSchema &schema)
    {
        /// Size for tile object + size for records.
        const auto tile_size = sizeof(RowTile) + config::tuples_per_tile() * schema.row_size();

        auto *tile = new (std::malloc(tile_size)) RowTile(AllocationType::TemporaryForClient, schema);

        return mx::resource::ptr(tile);
    }

    constexpr RowTile(const AllocationType allocation_type, const topology::PhysicalSchema &schema) noexcept
        : _record_size(schema.row_size()), _schema(schema), _allocation_type(allocation_type)
    {
    }

    [[nodiscard]] void *begin() noexcept { return static_cast<void *>(this + 1U); }
    [[nodiscard]] void *end() noexcept
    {
        return reinterpret_cast<void *>(std::uintptr_t(begin()) + (_size * _record_size));
    }
    [[nodiscard]] void *at(const std::uint32_t index) const noexcept
    {
        return reinterpret_cast<void *>(std::uintptr_t(this + 1U) + (index * _record_size));
    }
    [[nodiscard]] bool is_temporary() const noexcept { return static_cast<std::uint8_t>(_allocation_type) & 0b1; }
    [[nodiscard]] bool is_client_tile() const noexcept
    {
        return _allocation_type == AllocationType::TemporaryForClient;
    }
    [[nodiscard]] const topology::PhysicalSchema &schema() const noexcept { return _schema; }
    [[nodiscard]] std::uint64_t size() const noexcept { return _size; }
    [[nodiscard]] bool empty() const noexcept { return _size == 0U; }
    [[nodiscard]] bool full() const noexcept { return _size >= config::tuples_per_tile(); }

    void emplace_back(RowRecordView &&record)
    {
        const auto record_index = _size++;
        std::memcpy(this->at(record_index), record.data(), _record_size);
    }

    [[nodiscard]] std::uint64_t emplace_back(RowTile *other, const std::uint64_t from_index)
    {
        const auto count = std::min(other->size() - from_index, config::tuples_per_tile() - this->size());
        std::memcpy(this->at(_size), other->at(from_index), _record_size * count);
        _size += count;
        return count;
    }

    [[nodiscard]] RowRecordView record(const std::uint64_t index) const { return RowRecordView{_schema, at(index)}; }
    [[nodiscard]] std::optional<RowRecordView> allocate() noexcept
    {
        if (full())
        {
            return std::nullopt;
        }

        return std::make_optional(record(_size++));
    }

    static auto size_offset() { return offsetof(RowTile, _size); }

private:
    /// Size of the tile.
    std::uint64_t _size{0U};

    /// Size of the records stored within this tile.
    /// Since the size would be also available using the schema,
    /// holding a copy avoids following the pointer to the schema
    /// and reduces memory stalls.
    const std::uint16_t _record_size;

    /// Schema of the records within the tile.
    const topology::PhysicalSchema &_schema;

    /// Tiles can be temporary, when needed only for
    /// query execution (e.g., persisting join results),
    /// specifically for clients (implicit temporary),
    /// or persisted resources (when used as store for a table).
    const AllocationType _allocation_type;
};
} // namespace db::data