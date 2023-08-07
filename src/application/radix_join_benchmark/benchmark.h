#pragma once

#include "partitions.h"
#include "tuple.h"
#include <array>
#include <atomic>
#include <benchmark/chronometer.h>
#include <benchmark/cores.h>
#include <cstdint>
#include <memory>
#include <mx/system/cache.h>
#include <mx/util/core_set.h>
#include <string>
#include <vector>

namespace application::rjbenchmark {
/**
 * Benchmark executing the task-based Radix Join.
 */
class Benchmark
{
public:
    Benchmark(benchmark::Cores &&, std::uint16_t iterations, std::string &&build_side_file,
              std::string &&probe_side_file, bool use_performance_counter, std::string &&result_file_name);

    ~Benchmark() noexcept = default;

    /**
     * @return Core set the benchmark should run in the current iteration.
     */
    const mx::util::core_set &core_set();

    /**
     * Starts the benchmark after initialization.
     */
    void start();

    void finished();

private:
    /// Collection of cores the benchmark should run on.
    benchmark::Cores _cores;

    /// Number of iterations the benchmark should use.
    const std::uint16_t _iterations;

    /// Current iteration within the actual core set.
    std::uint16_t _current_iteration = std::numeric_limits<std::uint16_t>::max();

    /// Name of the file to print results to.
    const std::string _result_file_name;

    /// Build side data.
    std::pair<std::uint64_t, Tuple *> _build_relation;

    std::uint64_t _build_relation_key_sum{0U};

    /// Build materialized data.
    std::vector<LocalPartitions> _build_local_partitions;

    /// Probe side data.
    std::pair<std::uint64_t, Tuple *> _probe_relation;

    std::uint64_t _probe_relation_key_sum{0U};

    /// Build materialized data.
    std::vector<LocalPartitions> _probe_local_partitions;

    /// Global Partitions.
    std::vector<mx::resource::ptr> _partition_squads;

    alignas(mx::system::cache::line_size()) std::atomic_uint16_t _pending_worker_counter;

    /// Chronometer for starting/stopping time and performance counter.
    alignas(mx::system::cache::line_size()) benchmark::Chronometer<std::uint16_t> _chronometer;

    [[nodiscard]] static std::pair<std::uint64_t, Tuple *> read_tuples(std::string &&file_name);

    /**
     * Breaks up the entire partition into one relation per worker.
     *
     * @param tuples Number of tuples within the relation.
     * @param workers Number of workers.
     * @return A pair (start index, count tuples) per worker.
     */
    [[nodiscard]] static std::vector<std::pair<std::uint64_t, std::uint64_t>> calculate_worker_relation_boundaries(
        std::uint64_t tuples, std::uint64_t workers);
};
} // namespace application::rjbenchmark