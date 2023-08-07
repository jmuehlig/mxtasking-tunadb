#pragma once

namespace db {
class config
{
public:
    /**
     * @return Name of the DB (primary for console).
     */
    [[nodiscard]] static constexpr auto name() { return "tunadb"; }

    /**
     * @return Capacity of how many tuples are stored in one tile.
     */
    [[nodiscard]] static constexpr auto tuples_per_tile() { return 256U; }

    /**
     * @return Number of iterations to prefetch.
     */
    [[nodiscard]] static constexpr auto prefetch_iterations() { return 16U; }

    /**
     * @return True, if prevalent attributes should be preferred for prefetching.
     */
    [[nodiscard]] static constexpr auto is_prefer_prevalent_for_prefetching() { return true; }

    /**
     * @return True, when all columns should be materialized during partitioning (i.e.,
     *  for radix join or radix aggregation). This is a costly aspect and is only used
     *  for benchmarks to ensure comparability between specific applications (i.e., the ETH radix join).
     */
    [[nodiscard]] static constexpr auto is_materialize_all_columns_on_partitioning() { return false; }

    /**
     * @return True, when a hash should be used for calculating the partition id during
     *  partitioning (i.e., for radix join or radix aggregation). This evenly spreads key
     *  to partitions. However, this is used for benchmarks to ensure comparability between
     *  specific applications (i.e., the ETH radix join).
     */
    [[nodiscard]] static constexpr auto is_use_hash_for_partitioning() { return true; }

    [[nodiscard]] static constexpr auto is_relocate_radix_join() { return false; }

    /**
     * @return True, when the flounder compiler should write a jit map used by perf record to track symbols.
     */
    [[nodiscard]] static constexpr auto emit_flounder_code_to_perf() { return false; }

    /**
     * @return True, when the flounder compiler should be made visible for vtune.
     */
    [[nodiscard]] static constexpr auto emit_flounder_code_to_vtune() { return true; }

    /**
     * @return Trace id of the planning task.
     */
    [[nodiscard]] static constexpr auto task_id_planning() { return (1U << 8U) | 2U; }

    /**
     * @return Trace id of the hash table memset task.
     */
    [[nodiscard]] static constexpr auto task_id_hash_table_memset() { return (1U << 8U) | 4U; }

    /**
     * @return Default sample frequency 1x per ms.
     */
    [[nodiscard]] static constexpr auto default_sample_frequency() { return 1000; }
};
} // namespace db
