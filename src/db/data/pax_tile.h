#pragma once
#include "pax_record_view.h"
#include "tile_type.h"
#include <cstdint>
#include <cstdlib>
#include <db/config.h>
#include <db/topology/physical_schema.h>
#include <mx/memory/alignment_helper.h>
#include <mx/system/cache.h>
#include <mx/tasking/runtime.h>
#include <optional>

namespace db::data {
class alignas(mx::system::cache::line_size()) PaxTile
{
public:
    /**
     * Calculates the size of the pax with aligned columns.
     * Each column will start at its own cache line.
     *
     * @param schema Schema to store.
     * @return The size of the pax for max tuples.
     */
    [[nodiscard]] static std::uint64_t size(const topology::PhysicalSchema &schema) noexcept
    {
        const auto last_offset = schema.pax_offset(schema.size() - 1U);
        const auto last_column_size = schema.types().back().size() * config::tuples_per_tile();

        return last_offset + last_column_size;
    }

    [[nodiscard]] static mx::resource::ptr make(const topology::PhysicalSchema &schema, const bool is_temporary,
                                                const std::uint16_t worker_id)
    {
        /// Size for tile object + size for records.
        const auto tile_size = sizeof(PaxTile) + PaxTile::size(schema);

        return mx::tasking::runtime::new_resource<data::PaxTile>(
            tile_size,
            mx::resource::annotation{worker_id, mx::synchronization::isolation_level::Exclusive,
                                     mx::synchronization::protocol::Queue},
            static_cast<AllocationType>(is_temporary), schema);
    }

    [[nodiscard]] static mx::resource::ptr make_for_client(const topology::PhysicalSchema &schema)
    {
        /// Size for tile object + size for records.
        const auto tile_size = sizeof(PaxTile) + PaxTile::size(schema);

        auto *tile = new (std::malloc(tile_size)) PaxTile(AllocationType::TemporaryForClient, schema);

        return mx::resource::ptr(tile);
    }

    constexpr PaxTile(const AllocationType allocation_type, const topology::PhysicalSchema &schema) noexcept
        : _schema(schema), _allocation_type(allocation_type)
    {
    }

    void size(const std::uint32_t size) noexcept { _size = size; }

    [[nodiscard]] void *begin() noexcept { return static_cast<void *>(this + 1U); }

    [[nodiscard]] bool is_temporary() const noexcept { return static_cast<std::uint8_t>(_allocation_type) & 0b1; }
    [[nodiscard]] bool is_client_tile() const noexcept
    {
        return _allocation_type == AllocationType::TemporaryForClient;
    }
    [[nodiscard]] const topology::PhysicalSchema &schema() const noexcept { return _schema; }
    [[nodiscard]] std::uint64_t size() const noexcept { return _size; }
    [[nodiscard]] bool empty() const noexcept { return _size == 0U; }
    [[nodiscard]] bool full() const noexcept { return _size >= config::tuples_per_tile(); }

    [[nodiscard]] std::uint64_t emplace_back(PaxTile *other, const std::uint64_t from_index)
    {
        const auto count = std::min(other->size() - from_index, config::tuples_per_tile() - this->size());

        for (auto column_id = 0U; column_id < _schema.size(); ++column_id)
        {
            const auto column_offset = _schema.pax_offset(column_id);
            const auto type_size = _schema.type(column_id).size();
            const auto offset = column_offset + _size * type_size;
            const auto from_offset = column_offset + from_index * type_size;
            std::memcpy(reinterpret_cast<void *>(std::uintptr_t(this->begin()) + offset),
                        reinterpret_cast<void *>(std::uintptr_t(other->begin()) + from_offset), count * type_size);
        }

        _size += count;

        return count;
    }

    [[nodiscard]] std::optional<PaxRecordView> allocate() noexcept
    {
        if (full())
        {
            return std::nullopt;
        }

        return std::make_optional(PaxRecordView{_schema, this->begin(), _size++});
    }

    [[nodiscard]] std::pair<std::uint64_t, std::uint64_t> allocate(const std::uint64_t count) noexcept
    {
        const auto index = _size;
        const auto count_allocated = std::min<std::uint64_t>(count, config::tuples_per_tile() - _size);
        _size += count_allocated;

        return std::make_pair(index, count_allocated);
    }

    [[nodiscard]] PaxRecordView view(const std::uint32_t index) noexcept
    {
        return PaxRecordView{_schema, this->begin(), index};
    }

    static auto size_offset() { return offsetof(PaxTile, _size); }

private:
    /// Size of the tile.
    std::uint64_t _size{0U};

    /// Schema of the records within the tile.
    const topology::PhysicalSchema &_schema;

    /// Tiles can be temporary, when needed only for
    /// query execution (e.g., persisting join results),
    /// specifically for clients (implicit temporary),
    /// or persisted resources (when used as store for a table).
    const AllocationType _allocation_type;
};
} // namespace db::data