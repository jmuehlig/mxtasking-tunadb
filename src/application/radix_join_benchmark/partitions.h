#pragma once

#include "config.h"
#include "tuple.h"
#include <atomic>
#include <cstdlib>
#include <mx/memory/global_heap.h>
#include <mx/system/cache.h>
#include <mx/tasking/runtime.h>

namespace application::rjbenchmark {
class alignas(mx::system::cache::line_size()) LocalPartitions
{
public:
    LocalPartitions(const std::uint64_t count_tuples, std::vector<mx::resource::ptr> &partition_squads,
                    const std::uint8_t numa_node_id)
        : _partition_squads(partition_squads)
    {
        const auto count_partitions = partition_squads.size();

        _tile_offsets =
            new (std::aligned_alloc(64U, sizeof(std::uint64_t) * count_partitions)) std::uint64_t[count_partitions];
        for (auto i = 0U; i < count_partitions; ++i)
        {
            _tile_offsets[i] = (_next_tile_index++) * config::tuples_per_tile();
        }

        _allocated_size = sizeof(Tuple) * (count_tuples + (count_partitions * config::tuples_per_tile()));
        _materialized_tuples =
            reinterpret_cast<Tuple *>(mx::memory::GlobalHeap::allocate(numa_node_id, _allocated_size));
    }

    ~LocalPartitions()
    {
        std::free(_tile_offsets);
        mx::memory::GlobalHeap::free(_materialized_tuples, _allocated_size);
    }

    [[nodiscard]] std::uint64_t tile_offset(const std::uint64_t partition_id) const noexcept
    {
        return _tile_offsets[partition_id];
    }

    [[nodiscard]] std::uint64_t *tile_offsets() noexcept { return _tile_offsets; }
    [[nodiscard]] Tuple *tuples() noexcept { return _materialized_tuples; }
    [[nodiscard]] Tuple *from(const std::uint64_t tuple_index) noexcept { return &_materialized_tuples[tuple_index]; }

    [[nodiscard]] std::uint64_t &next_tile_index() noexcept { return _next_tile_index; }
    [[nodiscard]] std::uint64_t next_tile_index() const noexcept { return _next_tile_index; }

    [[nodiscard]] std::uint64_t next_tile_index_inc() noexcept { return _next_tile_index++; }

    [[nodiscard]] mx::resource::ptr squad(const std::uint64_t partition_id) const noexcept
    {
        return _partition_squads[partition_id];
    }

private:
    std::uint64_t _next_tile_index{0U};
    std::uint64_t *_tile_offsets;
    Tuple *_materialized_tuples;
    std::vector<mx::resource::ptr> &_partition_squads;
    std::uint64_t _allocated_size;
};
} // namespace application::rjbenchmark