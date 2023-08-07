#pragma once

#include <db/execution/compilation/hashtable/descriptor.h>
#include <db/execution/compilation/operator/operator_interface.h>
#include <db/plan/logical/node/join_node.h>
#include <db/topology/database.h>
#include <memory>
#include <mx/tasking/task.h>
#include <vector>

namespace db::plan::physical::compilation {
class JoinPlanner
{
public:
    [[nodiscard]] static std::unique_ptr<execution::compilation::OperatorInterface> build(
        const topology::Database &database, logical::JoinNode *logical_join_node,
        topology::LogicalSchema &&logical_build_schema,
        std::unique_ptr<execution::compilation::OperatorInterface> &&build_child,
        topology::LogicalSchema &&logical_probe_schema,
        std::unique_ptr<execution::compilation::OperatorInterface> &&probe_child,
        std::uint64_t expected_build_cardinality, std::vector<mx::tasking::TaskInterface *> &preparatory_tasks);

    /**
     * Builds a set of hash tables according to the given radix partition bits.
     * The hash tables will be aligned to a power of two of the expected cardinality.
     *
     * @param count_partitions Number of partitions.
     * @param count_worker Number of worker.
     * @param hash_table_descriptor Descriptor of the hash table.
     * @param preparatory_tasks List to add zero-out tasks.
     *
     * @return List of hash tables.
     */
    [[nodiscard]] static std::vector<mx::resource::ptr> create_hash_tables(
        std::uint32_t count_partitions, std::uint16_t count_worker,
        const execution::compilation::hashtable::Descriptor &hash_table_descriptor,
        std::vector<mx::tasking::TaskInterface *> &preparatory_tasks);

private:
    constexpr auto inline static HASH_TABLE_TYPE = execution::compilation::hashtable::Descriptor::Type::Chained;
    constexpr auto inline static MULTI_SLOT_ENTRY_COUNT = 1U;
    constexpr auto inline static BLOOM_FILTER_BYTES_PER_BLOCK = 8U; // 64bit per bloom filter block.

    [[nodiscard]] static std::unique_ptr<execution::compilation::OperatorInterface> build_radix_join(
        const topology::Database &database, logical::JoinNode *logical_join_node,
        topology::LogicalSchema &&logical_build_schema,
        std::unique_ptr<execution::compilation::OperatorInterface> &&build_child,
        topology::LogicalSchema &&logical_probe_schema,
        std::unique_ptr<execution::compilation::OperatorInterface> &&probe_child,
        std::uint64_t expected_build_cardinality, std::vector<mx::tasking::TaskInterface *> &preparatory_tasks);
    [[nodiscard]] static std::unique_ptr<execution::compilation::OperatorInterface> build_hash_join(
        const topology::Database &database, logical::JoinNode *logical_join_node,
        topology::LogicalSchema &&logical_build_schema,
        std::unique_ptr<execution::compilation::OperatorInterface> &&build_child,
        topology::LogicalSchema &&logical_probe_schema,
        std::unique_ptr<execution::compilation::OperatorInterface> &&probe_child,
        std::uint64_t expected_build_cardinality, std::vector<mx::tasking::TaskInterface *> &preparatory_tasks);
    [[nodiscard]] static std::unique_ptr<execution::compilation::OperatorInterface> build_nested_loops_join(
        const topology::Database &database, logical::JoinNode *logical_join_node,
        topology::LogicalSchema &&logical_build_schema,
        std::unique_ptr<execution::compilation::OperatorInterface> &&build_child,
        topology::LogicalSchema &&logical_probe_schema,
        std::unique_ptr<execution::compilation::OperatorInterface> &&probe_child,
        std::uint64_t expected_build_cardinality, std::vector<mx::tasking::TaskInterface *> &preparatory_tasks);

    /**
     * Extract the join predicate terms (build or probe side) fromt the given predicate.
     *
     * @param predicate Join predicate.
     * @param is_build Flag if this is the build or probe side (build = true).
     * @return List of predicate terms.
     */
    [[nodiscard]] static std::vector<expression::Term> extract_predicate_terms(
        const std::unique_ptr<expression::Operation> &predicate, const bool is_build)
    {
        auto terms = std::vector<expression::Term>{};
        JoinPlanner::extract_predicate_terms(predicate, is_build, terms);
        return terms;
    }

    /**
     * Extract the join predicate terms (build or probe side) fromt the given predicate.
     *
     * @param predicate Join predicate.
     * @param is_build Flag if this is the build or probe side (build = true).
     * @param terms List of terms to insert.
     */
    static void extract_predicate_terms(const std::unique_ptr<expression::Operation> &predicate, bool is_build,
                                        std::vector<expression::Term> &terms);

    /**
     * Examines if all predicates are primary keys.
     * If yes, we do not need multiple entries per hash bucket slot.
     *
     * @param database Database to examine tables and their priamry keys.
     * @param build_side_schema Schema of the build side.
     * @param build_keys Build keys.
     * @return True, if we build the hash table with primary keys.
     */
    [[nodiscard]] static bool are_keys_primary(const topology::Database &database,
                                               const topology::PhysicalSchema &build_side_schema,
                                               const std::vector<expression::Term> &build_keys);

    /**
     * Creates a list of radix bits, so that each pass fits into the (S)TLB cache
     * and each partition fits into the L2 cache.
     *
     * @param ht_type Type of the hash table.
     * @param count_workers Number of workers (= least number of partitions to load balance)
     * @param expected_build_cardinality Expected number of tuples stored in each hash table.
     * @param keys_size Size of the keys stored in the hash table.
     * @param record_size Size of the record stored in the hash table.
     * @param entries_per_slot Number of slots stored in each bucket.
     *
     * @return List of partition passes with the number of used bits per pass.
     */
    [[nodiscard]] static std::vector<std::uint8_t> calculate_partition_pass_bits(
        execution::compilation::hashtable::Descriptor::Type ht_type, std::uint16_t count_workers,
        std::uint64_t expected_build_cardinality, std::uint32_t keys_size, std::uint32_t record_size,
        std::uint8_t entries_per_slot);

    /**
     * Calculates the number of partitions for a specific phase specified
     * by the given radix bits.
     *
     * @param radix_bits Bits used for partitioning.
     * @param pass Pass to calculate the number of partitions for.
     * @return Number of partitions.
     */
    [[nodiscard]] static std::uint16_t count_partitions(const std::vector<std::uint8_t> &radix_bits,
                                                        const std::uint8_t pass) noexcept
    {
        if (pass == 0U)
        {
            return std::pow(2U, radix_bits.front());
        }

        return std::pow(2U, radix_bits[pass]) * count_partitions(radix_bits, pass - 1U);
    }

    /**
     * Calculates the number of partitions for all passes
     * by the given radix bits.
     *
     * @param radix_bits Bits used for partitioning.
     * @return Number of partitions.
     */
    [[nodiscard]] static std::uint16_t count_partitions(const std::vector<std::uint8_t> &radix_bits) noexcept
    {
        return count_partitions(radix_bits, radix_bits.size() - 1U);
    }

    /**
     * Checks if the partitions identified by the given parameters fits into the cache.
     *
     * @param ht_type Type of the table.
     * @param l2_cache_size Size of the cache.
     * @param radix_bits List of radix partitioning passes.
     * @param expected_build_cardinality Expected cardinality.
     * @param key_size Size of the keys stored in the hash table.
     * @param record_size Size of the records stored in the hash table.
     * @param entries_per_slot Number of entries per hash table slot.
     *
     * @return True, if the partitions fit into the cache.
     */
    [[nodiscard]] static bool fits_into_cache(execution::compilation::hashtable::Descriptor::Type ht_type,
                                              std::uint64_t l2_cache_size, const std::vector<std::uint8_t> &radix_bits,
                                              std::uint64_t expected_build_cardinality, std::uint32_t key_size,
                                              std::uint32_t record_size, std::uint8_t entries_per_slot);

    /**
     * Allocated memory for the partition bloom filter and creates zero-out tasks.
     *
     * @param expected_build_cardinality Expected cardinality of the build side.
     * @param count_partitions Number of radix partitions.
     * @param preparatory_tasks Lists of tasks to append zero-out tasks.
     *
     * @return Pointer with bloom filter memory and number of blocks per partition.
     */
    [[nodiscard]] static std::pair<std::unique_ptr<std::byte>, std::uint64_t> create_partition_bloom_filter(
        std::uint64_t expected_build_cardinality, std::uint32_t count_partitions, std::uint16_t count_worker,
        std::vector<mx::tasking::TaskInterface *> &preparatory_tasks);

    /**
     * Calculates the size of the bloom filter.
     *
     * @param expected_cardinality Expected number of keys to insert.
     * @param count_partitions Partitions.
     * @return Number of blocks per partition.
     */
    [[nodiscard]] static std::uint64_t calculate_bloom_filter_blocks_per_partition(std::uint64_t expected_cardinality,
                                                                                   std::uint32_t count_partitions);

    /**
     * Builds a set of partitions, aligned to the given radix bits and the given pass.
     *
     * @param radix_bits List of partition passes.
     * @param pass Pass to build partitions for.
     * @param count_worker Number of workers.
     *
     * @return List of partitions.
     */
    [[nodiscard]] static std::vector<mx::resource::ptr> create_partitions(const std::vector<std::uint8_t> &radix_bits,
                                                                          std::uint8_t pass,
                                                                          std::uint16_t count_worker);

    [[nodiscard]] static topology::PhysicalSchema create_partition_schema(
        topology::PhysicalSchema &&optimized_schema,
        const std::unique_ptr<execution::compilation::OperatorInterface> &child);
};
} // namespace db::plan::physical::compilation
