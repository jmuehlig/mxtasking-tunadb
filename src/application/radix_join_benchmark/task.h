#pragma once
#include "config.h"
#include "partitions.h"
#include "tuple.h"
#include <atomic>
#include <cstdint>
#include <functional>
#include <mx/tasking/runtime.h>
#include <mx/tasking/task.h>

namespace application::rjbenchmark {
template <class F> class FinalizePartitionTask final : public mx::tasking::TaskInterface
{
public:
    FinalizePartitionTask(LocalPartitions &partitions) noexcept : _partitions(partitions) {}

    ~FinalizePartitionTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t worker_id) override
    {
        const auto count_partitions = std::uint64_t(std::pow(2, config::radix_bits()));

        auto *tile_offsets = _partitions.tile_offsets();

        for (auto partition_id = 0U; partition_id < count_partitions; ++partition_id)
        {
            const auto offset = tile_offsets[partition_id];
            const auto size = offset & (config::tuples_per_tile() - 1U);
            if (size > 0U)
            {
                /// Emit the tile to the partition.
                auto *task = mx::tasking::runtime::new_task<F>(worker_id, _partitions.from(offset - size), size);
                task->annotate(_partitions.squad(partition_id));
                mx::tasking::runtime::spawn(*task, worker_id);
            }
        }

        // TODO: If is last finalize task, flush all squads

        return mx::tasking::TaskResult::make_remove();
    }

private:
    LocalPartitions &_partitions;
};

template <class S, class F> class GenerateScanTask final : public mx::tasking::TaskInterface
{
public:
    GenerateScanTask(const std::uint64_t start_index, const std::uint64_t count_tuples, Tuple *data,
                     LocalPartitions &partitions, mx::tasking::TaskInterface *finish_task) noexcept
        : _start_index(start_index), _count_tuples(count_tuples), _data(data), _partitions(partitions),
          _finish_task(finish_task)
    {
    }

    ~GenerateScanTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t worker_id) override
    {
        for (auto i = 0U; i < _count_tuples; i += config::tuples_per_tile())
        {
            auto *start = &_data[_start_index + i];
            const auto size = std::min<std::uint64_t>(config::tuples_per_tile(), _count_tuples - i);

            auto *task = mx::tasking::runtime::new_task<S>(worker_id, start, size, _partitions);
            /// TODO: Make MxTasking Prefetching work without resource
            /// TODO: Make interface to annotate the prefetch hint only
            /// TODO: Annotate worker and prefetch hint
            task->annotate(
                mx::resource::ptr{start,
                                  mx::resource::information{worker_id, mx::synchronization::primitive::ScheduleAll}},
                config::prefetch_size());
            mx::tasking::runtime::spawn(*task, worker_id);
        }

        /// Finalize the partitions.
        auto *finalize_partitions_task =
            mx::tasking::runtime::new_task<FinalizePartitionTask<F>>(worker_id, _partitions);
        finalize_partitions_task->annotate(worker_id);
        mx::tasking::runtime::spawn(*finalize_partitions_task, worker_id);

        /// Start next operator.
        mx::tasking::runtime::spawn(*_finish_task, worker_id);

        return mx::tasking::TaskResult::make_remove();
    }

private:
    const std::uint64_t _start_index;
    const std::uint64_t _count_tuples;
    Tuple *_data;
    LocalPartitions &_partitions;
    mx::tasking::TaskInterface *_finish_task;
};

template <class F> class ScanAndPartitionTask final : public mx::tasking::TaskInterface
{
public:
    ScanAndPartitionTask(Tuple *data, const std::uint32_t count_tuples, LocalPartitions &partitions) noexcept
        : _data(data), _count_tuples(count_tuples), _partitions(partitions)
    {
    }

    ~ScanAndPartitionTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t worker_id) override
    {
        const auto mask = std::uint64_t(std::pow(2, config::radix_bits())) - 1U;
        constexpr auto tile_mask = config::tuples_per_tile() - 1U;

        auto *tile_offsets = _partitions.tile_offsets();

        for (auto i = 0U; i < _count_tuples; ++i)
        {
            const auto key = _data[i].key;
            const auto partition_id = key & mask;
            const auto offset = tile_offsets[partition_id]++;

            /// Offset % tuples_per_tile == 0U
            const auto is_packed = ((offset + 1U) & tile_mask) == 0U;

            if (is_packed)
            {
                /// Emit the tile to the partition.
                auto *task = mx::tasking::runtime::new_task<F>(worker_id, _partitions.from(offset - tile_mask),
                                                               config::tuples_per_tile());
                task->annotate(_partitions.squad(partition_id));
                mx::tasking::runtime::spawn(*task, worker_id);

                /// Get the next offset
                tile_offsets[partition_id] = _partitions.next_tile_index() * config::tuples_per_tile();
            }

            /// Increment next tile when full.
            _partitions.next_tile_index() += is_packed;

            /// Materialize data.
            _partitions.tuples()[offset] = _data[i];
        }

        return mx::tasking::TaskResult::make_remove();
    }

private:
    Tuple *_data;
    const std::uint32_t _count_tuples;
    LocalPartitions &_partitions;
};

template <class F> class ScanAndPartitionAllTask final : public mx::tasking::TaskInterface
{
public:
    ScanAndPartitionAllTask(Tuple *data, const std::uint64_t from, const std::uint64_t to, LocalPartitions &partitions,
                            mx::tasking::TaskInterface *finish_task) noexcept
        : _data(data), _from(from), _to(to), _partitions(partitions), _finish_task(finish_task)
    {
    }

    ~ScanAndPartitionAllTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t worker_id) override
    {
        const auto count_partitions = std::uint64_t(std::pow(2, config::radix_bits()));
        const auto mask = count_partitions - 1U;
        constexpr auto tile_mask = config::tuples_per_tile() - 1U;

        auto *tile_offsets = _partitions.tile_offsets();

        /// Scan and Partition
        for (auto i = _from; i < _to; ++i)
        {
            const auto key = _data[i].key;
            const auto partition_id = key & mask;
            const auto offset = tile_offsets[partition_id];

            /// Materialize data.
            _partitions.tuples()[offset] = _data[i];

            /// Next offset for that tile.
            ++tile_offsets[partition_id];

            /// Offset % tuples_per_tile == 0U
            const auto is_packed = ((offset + 1U) & tile_mask) == 0U;
            if (is_packed)
            {
                /// Emit the tile to the partition.
                auto *task = mx::tasking::runtime::new_task<F>(worker_id, _partitions.from(offset - tile_mask),
                                                               config::tuples_per_tile());
                task->annotate(_partitions.squad(partition_id));
                mx::tasking::runtime::spawn(*task, worker_id);

                /// Get the next offset
                tile_offsets[partition_id] = _partitions.next_tile_index_inc() * config::tuples_per_tile();
            }
        }

        /// Finalize
        for (auto partition_id = 0U; partition_id < count_partitions; ++partition_id)
        {
            const auto offset = tile_offsets[partition_id];
            const auto size = offset & (config::tuples_per_tile() - 1U);
            if (size > 0U)
            {
                /// Emit the tile to the partition.
                auto *task = mx::tasking::runtime::new_task<F>(worker_id, _partitions.from(offset - size), size);
                task->annotate(_partitions.squad(partition_id));
                mx::tasking::runtime::spawn(*task, worker_id);
            }
        }

        return mx::tasking::TaskResult::make_succeed_and_remove(_finish_task);
    }

private:
    Tuple *_data;
    const std::uint64_t _from;
    const std::uint64_t _to;
    LocalPartitions &_partitions;
    mx::tasking::TaskInterface *_finish_task;
};

class BuildHTTask final : public mx::tasking::TaskInterface
{
public:
    BuildHTTask(Tuple *data, const std::uint32_t count_tuples) noexcept : _data(data), _count_tuples(count_tuples) {}

    ~BuildHTTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t /*worker_id*/) override
    {
        auto hashtable = this->annotation().resource();

        for (auto i = 0U; i < _count_tuples; ++i)
        {
            /// TODO: Insert into HT
        }

        return mx::tasking::TaskResult::make_remove();
    }

private:
    Tuple *_data;
    const std::uint32_t _count_tuples;
};

class ProbeHTTask final : public mx::tasking::TaskInterface
{
public:
    ProbeHTTask(Tuple *data, const std::uint32_t count_tuples) noexcept : _data(data), _count_tuples(count_tuples) {}

    ~ProbeHTTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t /*worker_id*/) override
    {
        auto hashtable = this->annotation().resource();

        for (auto i = 0U; i < _count_tuples; ++i)
        {
            /// TODO: Probe HT
        }

        return mx::tasking::TaskResult::make_remove();
    }

private:
    Tuple *_data;
    const std::uint32_t _count_tuples;
};

class SumKeysTask final : public mx::tasking::TaskInterface
{
public:
    SumKeysTask(Tuple *data, const std::uint32_t count_tuples) noexcept : _data(data), _count_tuples(count_tuples) {}

    ~SumKeysTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t /*worker_id*/) override
    {
        for (auto i = 0U; i < _count_tuples; ++i)
        {
            const auto key = _data[i].key;
            this->_sum += key;
        }

        return mx::tasking::TaskResult::make_remove();
    }

    [[nodiscard]] std::int64_t sum() const noexcept { return _sum; }
    [[nodiscard]] std::uint64_t count_tuples() const noexcept { return _count_tuples; }

private:
    Tuple *_data;
    const std::uint32_t _count_tuples;
    std::int64_t _sum{0U};
};

class SynchronizeWorkerTask final : public mx::tasking::TaskInterface
{
public:
    SynchronizeWorkerTask(std::atomic_uint16_t &pending_counter, std::function<void(void)> callback) noexcept
        : _pending_counter(pending_counter), _callback(std::move(callback))
    {
    }

    ~SynchronizeWorkerTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t /*worker_id*/) override
    {
        if (_pending_counter.fetch_sub(1U) == 1U)
        {
            _callback();
        }

        return mx::tasking::TaskResult::make_remove();
    }

private:
    std::atomic_uint16_t &_pending_counter;
    std::function<void(void)> _callback;
};

} // namespace application::rjbenchmark