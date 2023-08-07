#include "join_planner.h"
#include <db/config.h>
#include <db/execution/compilation/bloom_filter.h>
#include <db/execution/compilation/hashtable/table_proxy.h>
#include <db/execution/compilation/operator/buffer_operator.h>
#include <db/execution/compilation/operator/hash_join_operator.h>
#include <db/execution/compilation/operator/nested_loops_join_operator.h>
#include <db/execution/compilation/operator/partition_filter_operator.h>
#include <db/execution/compilation/operator/partition_operator.h>
#include <db/execution/compilation/operator/radix_join_operator.h>
#include <mx/tasking/runtime.h>

using namespace db::plan::physical::compilation;

std::unique_ptr<db::execution::compilation::OperatorInterface> JoinPlanner::build(
    const topology::Database &database, logical::JoinNode *logical_join_node,
    topology::LogicalSchema &&logical_build_schema,
    std::unique_ptr<execution::compilation::OperatorInterface> &&build_child,
    topology::LogicalSchema &&logical_probe_schema,
    std::unique_ptr<execution::compilation::OperatorInterface> &&probe_child,
    const std::uint64_t expected_build_cardinality, std::vector<mx::tasking::TaskInterface *> &preparatory_tasks)
{
    if (logical_join_node->method() == logical::JoinNode::RadixJoin ||
        logical_join_node->method() == logical::JoinNode::FilteredRadixJoin)
    {
        return JoinPlanner::build_radix_join(database, logical_join_node, std::move(logical_build_schema),
                                             std::move(build_child), std::move(logical_probe_schema),
                                             std::move(probe_child), expected_build_cardinality, preparatory_tasks);
    }

    if (logical_join_node->method() == logical::JoinNode::HashJoin)
    {
        return JoinPlanner::build_hash_join(database, logical_join_node, std::move(logical_build_schema),
                                            std::move(build_child), std::move(logical_probe_schema),
                                            std::move(probe_child), expected_build_cardinality, preparatory_tasks);
    }

    if (logical_join_node->method() == logical::JoinNode::NestedLoopsJoin)
    {
        return JoinPlanner::build_nested_loops_join(
            database, logical_join_node, std::move(logical_build_schema), std::move(build_child),
            std::move(logical_probe_schema), std::move(probe_child), expected_build_cardinality, preparatory_tasks);
    }

    return nullptr;
}

std::unique_ptr<db::execution::compilation::OperatorInterface> JoinPlanner::build_radix_join(
    const topology::Database &database, logical::JoinNode *logical_join_node,
    topology::LogicalSchema &&logical_build_schema,
    std::unique_ptr<execution::compilation::OperatorInterface> &&build_child,
    topology::LogicalSchema &&logical_probe_schema,
    std::unique_ptr<execution::compilation::OperatorInterface> &&probe_child,
    const std::uint64_t expected_build_cardinality, std::vector<mx::tasking::TaskInterface *> &preparatory_tasks)
{
    const auto count_worker = mx::tasking::runtime::workers();
    const auto is_filter = logical_join_node->method() == logical::JoinNode::Method::FilteredRadixJoin;

    /// Predicate terms.
    auto probe_predicate_terms = JoinPlanner::extract_predicate_terms(logical_join_node->predicate(), false);
    auto build_predicate_terms = JoinPlanner::extract_predicate_terms(logical_join_node->predicate(), true);

    /// Schema stored in the hash table, derived from the build-side child.
    auto build_key_schema = topology::PhysicalSchema::from_logical(logical_build_schema, build_predicate_terms, true);
    auto build_entry_schema =
        topology::PhysicalSchema::from_logical(logical_build_schema, build_predicate_terms, false);

    /// Check if all keys are primary. If yes, we do not need to store multiple entries per key.
    const auto is_key_unique = JoinPlanner::are_keys_primary(database, build_child->schema(), build_predicate_terms);
    const auto entries_per_hashtable_slot =
        (is_key_unique &&
         JoinPlanner::HASH_TABLE_TYPE == execution::compilation::hashtable::Descriptor::Type::LinearProbing)
            ? 1U
            : JoinPlanner::MULTI_SLOT_ENTRY_COUNT;

    /// Create set of radix bits, one entry per pass.
    const auto radix_bits = JoinPlanner::calculate_partition_pass_bits(
        JoinPlanner::HASH_TABLE_TYPE, count_worker, expected_build_cardinality, build_key_schema.row_size(),
        build_entry_schema.row_size(), entries_per_hashtable_slot);
    const auto count_partitions = JoinPlanner::count_partitions(radix_bits);
    const auto count_radix_bits = std::uint8_t(std::popcount(count_partitions - 1U));

    /// Create the bloom filter.
    auto bloom_filter = std::unique_ptr<std::byte>{nullptr};
    auto blocks_per_partition = 0ULL;
    if (is_filter)
    {
        std::tie(bloom_filter, blocks_per_partition) = JoinPlanner::create_partition_bloom_filter(
            expected_build_cardinality, count_partitions, count_worker, preparatory_tasks);
    }
    auto bloom_filter_descriptor =
        execution::compilation::BloomFilterDescriptor{bloom_filter.get(), blocks_per_partition};

    /// Create the descriptor for the hash tables and the hash tables..
    const auto capacity_per_table = execution::compilation::hashtable::TableProxy::allocation_capacity(
        expected_build_cardinality / count_partitions, JoinPlanner::HASH_TABLE_TYPE);

    auto hash_table_descriptor = execution::compilation::hashtable::Descriptor{
        JoinPlanner::HASH_TABLE_TYPE,  capacity_per_table,     build_key_schema.row_size(),
        build_entry_schema.row_size(), is_key_unique == false, entries_per_hashtable_slot};
    auto hash_tables =
        JoinPlanner::create_hash_tables(count_partitions, count_worker, hash_table_descriptor, preparatory_tasks);

    /** Build Side **/
    /// Build the left side partitioning (left -> |partition -> build|).
    for (auto partition_pass = 0U; partition_pass < radix_bits.size(); ++partition_pass)
    {
        /// Create the output schema of the partition operator.
        /// This is a dirty hack: Since the ETH code materializes all columns,
        /// we may be comparable; in that case we materialize all, too.
        auto partition_schema = JoinPlanner::create_partition_schema(
            topology::PhysicalSchema::make_combination(build_key_schema, build_entry_schema), build_child);

        const auto is_last_pass = partition_pass == radix_bits.size() - 1U;
        if (is_last_pass == false)
        {
            /// Create the partition squads that are needed for all but the last pass.
            auto partitions = JoinPlanner::create_partitions(radix_bits, partition_pass, count_worker);

            /// Add the partition hash if there are passes following up.
            partition_schema.emplace_back(
                expression::Term{execution::compilation::PartitionOperator::partition_hash_term},
                type::Type::make_bigint());

            /// Operator that materializes the partition.
            auto materialize_partition_operator =
                std::make_unique<execution::compilation::MaterializePartitionOperator>(
                    topology::PhysicalSchema{partition_schema}, std::move(partitions), false, true);

            /// Operator that partitions.
            auto build_partition_operator = std::make_unique<execution::compilation::PartitionOperator>(
                std::move(partition_schema), build_predicate_terms, radix_bits, partition_pass);
            build_partition_operator->child(std::move(build_child));

            materialize_partition_operator->child(std::move(build_partition_operator));
            build_child = std::move(materialize_partition_operator);
        }
        else
        {
            /// Add the partition id if the filter needs it.
            if (is_filter)
            {
                partition_schema.emplace_back(
                    expression::Term{execution::compilation::PartitionOperator::partition_id_term},
                    type::Type::make_bigint());
            }

            /// Operator that materializes the partition.
            auto build_partition_operator = std::make_unique<execution::compilation::PartitionOperator>(
                topology::PhysicalSchema{partition_schema}, build_predicate_terms, radix_bits, partition_pass);
            build_partition_operator->child(std::move(build_child));
            build_child = std::move(build_partition_operator);

            /// Operator that materializes the partition.
            auto materialize_partition_operator =
                std::make_unique<execution::compilation::MaterializePartitionOperator>(
                    std::move(partition_schema), hash_tables, true,
                    config::is_relocate_radix_join() ? is_filter : true);

            materialize_partition_operator->child(std::move(build_child));
            build_child = std::move(materialize_partition_operator);
        }
    }

    /// The hash table build operator.
    auto build_operator = std::make_unique<execution::compilation::RadixJoinBuildOperator>(
        std::move(build_key_schema), std::move(build_entry_schema), hash_tables, hash_table_descriptor,
        count_radix_bits);
    build_operator->child(std::move(build_child));

    /// Remember hash table schemas (keys and entries) for the probe operator.
    const auto &hash_table_keys_schema = build_operator->keys_schema();
    const auto &hash_table_entries_schema = build_operator->entries_schema();

    build_child = std::move(build_operator);

    /// When the build side is selective, build a partition filter operator.
    if (is_filter)
    {
        const auto &build_term = hash_table_keys_schema.terms().front();
        const auto build_term_type = hash_table_keys_schema.types().front();
        auto build_partition_filter_operator = std::make_unique<execution::compilation::PartitionFilterBuildOperator>(
            build_term, build_term_type, bloom_filter_descriptor);
        build_partition_filter_operator->child(std::move(build_child));
        build_child = std::move(build_partition_filter_operator);
    }

    /** Probe Side **/
    /// This is a dirty hack: Since the ETH code materializes all columns,
    /// we may be comparable; in that case we materialize all, too.
    auto probe_side_schema =
        JoinPlanner::create_partition_schema(topology::PhysicalSchema::from_logical(logical_probe_schema), probe_child);

    /// Build the right side (right -> |partition|).
    for (auto partition_pass = 0U; partition_pass < radix_bits.size(); ++partition_pass)
    {
        const auto is_last_pass = partition_pass == radix_bits.size() - 1U;
        const auto is_first_pass = partition_pass == 0U;

        if (is_last_pass == false)
        {
            /// Create the partition squads that are needed for all but the last pass.
            auto partitions = JoinPlanner::create_partitions(radix_bits, partition_pass, count_worker);

            /// Add the partition hash if there are passes following up.
            auto partition_schema = topology::PhysicalSchema{probe_side_schema};
            partition_schema.emplace_back(
                expression::Term{execution::compilation::PartitionOperator::partition_hash_term},
                type::Type::make_bigint());

            /// Operator that partitions.
            auto probe_partition_operator = std::make_unique<execution::compilation::PartitionOperator>(
                topology::PhysicalSchema{partition_schema}, probe_predicate_terms, radix_bits, partition_pass);
            probe_partition_operator->child(std::move(probe_child));
            probe_child = std::move(probe_partition_operator);

            /// When the build side is selective, probe bloom filter before materializing.
            if (is_first_pass && is_filter)
            {
                auto probe_partition_filter_operator =
                    std::make_unique<execution::compilation::PartitionFilterProbeOperator>(
                        topology::PhysicalSchema{probe_side_schema}, probe_predicate_terms.front(), radix_bits,
                        bloom_filter_descriptor);
                probe_partition_filter_operator->child(std::move(probe_child));
                probe_child = std::move(probe_partition_filter_operator);
            }

            /// Operator that materializes the partition.
            auto materialize_partition_operator =
                std::make_unique<execution::compilation::MaterializePartitionOperator>(
                    std::move(partition_schema), std::move(partitions), false, true);

            materialize_partition_operator->child(std::move(probe_child));
            probe_child = std::move(materialize_partition_operator);
        }
        else
        {
            auto probe_partition_operator = std::make_unique<execution::compilation::PartitionOperator>(
                topology::PhysicalSchema{probe_side_schema}, probe_predicate_terms, radix_bits, partition_pass);
            probe_partition_operator->child(std::move(probe_child));
            probe_child = std::move(probe_partition_operator);

            /// When the build side is selective, probe bloom filter before partitioning.
            if (is_first_pass && is_filter)
            {
                auto probe_partition_filter_operator =
                    std::make_unique<execution::compilation::PartitionFilterProbeOperator>(
                        topology::PhysicalSchema{probe_side_schema}, probe_predicate_terms.front(), radix_bits,
                        bloom_filter_descriptor);
                probe_partition_filter_operator->child(std::move(probe_child));
                probe_child = std::move(probe_partition_filter_operator);
            }

            auto materialize_partition_operator =
                std::make_unique<execution::compilation::MaterializePartitionOperator>(
                    std::move(probe_side_schema), std::move(hash_tables), true, true, std::move(bloom_filter));
            materialize_partition_operator->child(std::move(probe_child));
            probe_child = std::move(materialize_partition_operator);
        }
    }

    /// Build the join operator (left -> partition -> build -> |probe| <- partition <- right).#
    auto join_schema = topology::PhysicalSchema::from_logical(logical_join_node->relation().schema());
    auto probe_operator = std::make_unique<execution::compilation::RadixJoinProbeOperator>(
        std::move(join_schema), hash_table_keys_schema, hash_table_entries_schema, hash_table_descriptor,
        std::move(probe_predicate_terms), count_radix_bits);
    probe_operator->left_child(std::move(build_child));
    probe_operator->right_child(std::move(probe_child));

    return probe_operator;
}

std::unique_ptr<db::execution::compilation::OperatorInterface> JoinPlanner::build_hash_join(
    const topology::Database &database, logical::JoinNode *logical_join_node,
    topology::LogicalSchema &&logical_build_schema,
    std::unique_ptr<execution::compilation::OperatorInterface> &&build_child,
    topology::LogicalSchema && /*logical_probe_schema*/,
    std::unique_ptr<execution::compilation::OperatorInterface> &&probe_child,
    const std::uint64_t expected_build_cardinality, std::vector<mx::tasking::TaskInterface *> &preparatory_tasks)
{
    const auto hash_table_buckets = execution::compilation::hashtable::TableProxy::allocation_capacity(
        expected_build_cardinality, JoinPlanner::HASH_TABLE_TYPE);

    /// Build and probe terms.
    auto probe_predicate_terms = JoinPlanner::extract_predicate_terms(logical_join_node->predicate(), false);
    auto build_predicate_terms = JoinPlanner::extract_predicate_terms(logical_join_node->predicate(), true);

    /// Schema stored in the hash table.
    auto build_key_schema = topology::PhysicalSchema::from_logical(logical_build_schema, build_predicate_terms, true);
    auto build_entry_schema =
        topology::PhysicalSchema::from_logical(logical_build_schema, build_predicate_terms, false);

    /// May have multiple pks, but we only get the one in the schema. (l_orderkey, l_linenumber)
    const auto is_key_unique = JoinPlanner::are_keys_primary(database, build_child->schema(), build_predicate_terms);
    const auto entries_per_hashtable_slot =
        (is_key_unique &&
         JoinPlanner::HASH_TABLE_TYPE == execution::compilation::hashtable::Descriptor::Type::LinearProbing)
            ? 1U
            : JoinPlanner::MULTI_SLOT_ENTRY_COUNT;

    /// Descriptor of the hash table.
    auto hash_table_descriptor = execution::compilation::hashtable::Descriptor{
        JoinPlanner::HASH_TABLE_TYPE,  hash_table_buckets,     build_entry_schema.row_size(),
        build_entry_schema.row_size(), is_key_unique == false, entries_per_hashtable_slot};

    /// Build the hash table.
    const auto hash_table_size = execution::compilation::hashtable::TableProxy::size(hash_table_descriptor);
    const auto local_worker_id = mx::tasking::runtime::worker_id();

    auto hash_table = mx::resource::ptr{};

    if (hash_table_descriptor.table_type() == execution::compilation::hashtable::Descriptor::LinearProbing)
    {
        hash_table = mx::tasking::runtime::new_squad<execution::compilation::hashtable::LinearProbingTable>(
            hash_table_size, 0U, hash_table_descriptor);
    }
    else if (hash_table_descriptor.table_type() == execution::compilation::hashtable::Descriptor::Chained)
    {
        hash_table = mx::tasking::runtime::new_squad<execution::compilation::hashtable::ChainedTable>(
            hash_table_size, 0U, hash_table_descriptor);
    }

    auto *zero_out_task = mx::tasking::runtime::new_task<execution::compilation::hashtable::InitializeTableTask>(
        local_worker_id, hash_table.get<execution::compilation::hashtable::AbstractTable>());
    zero_out_task->annotate(std::uint16_t(0U));
    preparatory_tasks.emplace_back(zero_out_task);

    /// Build side.
    auto hash_join_build_operator = std::make_unique<execution::compilation::HashJoinBuildOperator>(
        std::move(build_key_schema), std::move(build_entry_schema), hash_table, hash_table_descriptor);
    hash_join_build_operator->child(std::move(build_child));

    /// Probe side.
    auto probe_schema = topology::PhysicalSchema::from_logical(logical_join_node->relation().schema());
    auto hash_join_probe_operator = std::make_unique<execution::compilation::HashJoinProbeOperator>(
        std::move(probe_schema), hash_join_build_operator->keys_schema(), hash_join_build_operator->entries_schema(),
        hash_table, hash_table_descriptor, std::move(probe_predicate_terms));
    hash_join_probe_operator->left_child(std::move(hash_join_build_operator));
    hash_join_probe_operator->right_child(std::move(probe_child));

    return hash_join_probe_operator;
}

std::unique_ptr<db::execution::compilation::OperatorInterface> JoinPlanner::build_nested_loops_join(
    const topology::Database & /*database*/, logical::JoinNode *logical_join_node,
    topology::LogicalSchema &&logical_build_schema,
    std::unique_ptr<execution::compilation::OperatorInterface> &&build_child,
    topology::LogicalSchema && /*logical_probe_schema*/,
    std::unique_ptr<execution::compilation::OperatorInterface> &&probe_child,
    const std::uint64_t expected_build_cardinality, std::vector<mx::tasking::TaskInterface *> & /*preparatory_tasks*/)
{
    auto build_schema = topology::PhysicalSchema::from_logical(logical_build_schema);
    auto *buffer = execution::compilation::RowRecordBuffer::make(
        build_schema, mx::memory::alignment_helper::next_power_of_two(std::max(32UL, expected_build_cardinality)));

    /// Build side.
    auto buffer_operator = std::make_unique<execution::compilation::BufferOperator>(std::move(build_schema), buffer);
    buffer_operator->child(std::move(build_child));

    /// Probe side.
    auto nested_loops_join_operator = std::make_unique<execution::compilation::NestedLoopsJoinOperator>(
        topology::PhysicalSchema::from_logical(logical_join_node->relation().schema()), buffer_operator->schema(),
        buffer, std::move(logical_join_node->predicate()));
    nested_loops_join_operator->left_child(std::move(buffer_operator));
    nested_loops_join_operator->right_child(std::move(probe_child));

    return nested_loops_join_operator;
}

void JoinPlanner::extract_predicate_terms(const std::unique_ptr<expression::Operation> &predicate, const bool is_build,
                                          std::vector<expression::Term> &terms)
{
    if (predicate->is_binary() == false)
    {
        return;
    }

    auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());

    if (predicate->is_comparison())
    {
        if (is_build)
        {
            terms.emplace_back(binary_operation->left_child()->result().value());
        }
        else
        {
            terms.emplace_back(binary_operation->right_child()->result().value());
        }
    }
    else
    {
        JoinPlanner::extract_predicate_terms(binary_operation->left_child(), is_build, terms);
        JoinPlanner::extract_predicate_terms(binary_operation->right_child(), is_build, terms);
    }
}

bool JoinPlanner::are_keys_primary(const topology::Database &database,
                                   const topology::PhysicalSchema &build_side_schema,
                                   const std::vector<expression::Term> &build_keys)
{
    auto count_primary_keys = 0U;
    for (const auto &term : build_side_schema.terms())
    {
        if (term.is_attribute() && std::find(build_keys.begin(), build_keys.end(), term) != build_keys.end())
        {
            const auto &attribute = term.get<expression::Attribute>();
            if (attribute.source().has_value())
            {
                if (database.is_table(attribute.source().value().name()))
                {
                    const auto &table = database[attribute.source().value().name()];
                    for (auto i = 0U; i < table.schema().size(); ++i)
                    {
                        if (table.schema().is_primary_key(i))
                        {
                            ++count_primary_keys;
                            if (std::find(build_keys.begin(), build_keys.end(), table.schema().term(i)) ==
                                build_keys.end())
                            {
                                return false;
                            }
                        }
                    }
                }
            }
        }
    }

    return build_keys.size() == count_primary_keys;
}

std::vector<std::uint8_t> JoinPlanner::calculate_partition_pass_bits(
    execution::compilation::hashtable::Descriptor::Type ht_type, std::uint16_t count_workers,
    std::uint64_t expected_build_cardinality, std::uint32_t keys_size, std::uint32_t record_size,
    std::uint8_t entries_per_slot)
{
    constexpr auto min_radix_bits = 3U;
    constexpr auto max_radix_bits = 12U;

    /// Size of the L2 cache; each partition should fit into the L2 cache.
    const auto l2_cache_in_bytes = std::uint64_t(mx::system::cache::size<mx::system::cache::L2>() * .75);

    /// Try to use only a single partition step from 3 to 5 bits.
    /// If that breaks hash table into cache-fitting-sizes and utilizes all workers; we are done.
    auto radix_bits = std::vector<std::uint8_t>{min_radix_bits};
    for (auto i = 0U; i <= (max_radix_bits - min_radix_bits); ++i)
    {
        const auto fits_into_cache =
            JoinPlanner::fits_into_cache(ht_type, l2_cache_in_bytes, radix_bits, expected_build_cardinality, keys_size,
                                         record_size, entries_per_slot);
        const auto utilizes_all_workers = JoinPlanner::count_partitions(radix_bits) >= count_workers;

        if (fits_into_cache && utilizes_all_workers)
        {
            return radix_bits;
        }

        radix_bits.back() += 1U;
    }

    /// If a single partition step is not enough, use a second one.
    radix_bits = {min_radix_bits, min_radix_bits};
    for (auto i = 0U; i < (max_radix_bits - min_radix_bits); ++i)
    {
        const auto fits_into_cache =
            JoinPlanner::fits_into_cache(ht_type, l2_cache_in_bytes, radix_bits, expected_build_cardinality, keys_size,
                                         record_size, entries_per_slot);
        const auto utilizes_all_workers = JoinPlanner::count_partitions(radix_bits) >= count_workers;

        if (fits_into_cache && utilizes_all_workers)
        {
            return radix_bits;
        }

        radix_bits[0U] += 1U;
        radix_bits[1U] += 1U;
    }

    return radix_bits;
}

bool JoinPlanner::fits_into_cache(execution::compilation::hashtable::Descriptor::Type ht_type,
                                  std::uint64_t l2_cache_size, const std::vector<std::uint8_t> &radix_bits,
                                  std::uint64_t expected_build_cardinality, std::uint32_t key_size,
                                  std::uint32_t record_size, std::uint8_t entries_per_slot)
{
    const auto count_partitions = JoinPlanner::count_partitions(radix_bits);
    const auto expected_records_per_partition = std::uint64_t(expected_build_cardinality / count_partitions);
    const auto allocation_capacity =
        execution::compilation::hashtable::TableProxy::allocation_capacity(expected_records_per_partition, ht_type);

    const auto size_in_bytes_per_ht =
        execution::compilation::hashtable::TableProxy::size(execution::compilation::hashtable::Descriptor{
            ht_type, allocation_capacity, key_size, record_size, entries_per_slot > 1U, entries_per_slot});

    return size_in_bytes_per_ht <= l2_cache_size;
}

std::pair<std::unique_ptr<std::byte>, std::uint64_t> JoinPlanner::create_partition_bloom_filter(
    const std::uint64_t expected_build_cardinality, const std::uint32_t count_partitions,
    const std::uint16_t count_worker, std::vector<mx::tasking::TaskInterface *> &preparatory_tasks)
{
    auto bloom_filter = std::unique_ptr<std::byte>{nullptr};
    const auto bloom_filter_blocks_per_partition =
        JoinPlanner::calculate_bloom_filter_blocks_per_partition(expected_build_cardinality, count_partitions);

    if (bloom_filter_blocks_per_partition > 0U)
    {
        const auto bloom_filter_size =
            bloom_filter_blocks_per_partition * count_partitions * JoinPlanner::BLOOM_FILTER_BYTES_PER_BLOCK;
        bloom_filter.reset(reinterpret_cast<std::byte *>(std::aligned_alloc(64U, bloom_filter_size)));

        /// Memset the bloom filter in parallel.
        const auto local_worker_id = mx::tasking::runtime::worker_id();
        const auto memset_bytes_per_worker =
            mx::memory::alignment_helper::next_multiple<std::size_t>(bloom_filter_size / count_worker, 8U);
        auto already_set = 0ULL;
        for (auto worker_id = std::uint16_t(0U); worker_id < count_worker; ++worker_id)
        {
            const auto zero_out_size = std::min<std::size_t>(memset_bytes_per_worker, bloom_filter_size - already_set);
            auto *zero_out_begin = reinterpret_cast<void *>(std::uintptr_t(bloom_filter.get()) + already_set);
            already_set += zero_out_size;

            auto *memset_bloom_filter_task =
                mx::tasking::runtime::new_task<execution::compilation::ZeroOutBloomFilterTask>(
                    local_worker_id, zero_out_begin, zero_out_size);
            memset_bloom_filter_task->annotate(worker_id);
            preparatory_tasks.emplace_back(memset_bloom_filter_task);
        }
    }

    return std::make_pair(std::move(bloom_filter), bloom_filter_blocks_per_partition);
}

std::uint64_t JoinPlanner::calculate_bloom_filter_blocks_per_partition(const std::uint64_t expected_cardinality,
                                                                       const std::uint32_t count_partitions)
{
    /// See https://hur.st/bloomfilter

    /// 6 items per 64bit block
    /// (actually we want 4, but aligning the number of blocks
    /// will increase the number of blocks and reduce the items
    /// per block).
    const auto needed_bits = expected_cardinality * 16U;

    /// 64bits per block.
    const auto needed_blocks = needed_bits / 64U;

    /// Align for easy mod (& n-1). Because this will result in
    /// more blocks, we increased items before (6 instead of 4).
    const auto needed_blocks_per_partition =
        mx::memory::alignment_helper::next_power_of_two(needed_blocks / count_partitions);

    return std::max<std::uint64_t>(8U, needed_blocks_per_partition);
}

std::vector<mx::resource::ptr> JoinPlanner::create_hash_tables(
    const std::uint32_t count_partitions, const std::uint16_t count_worker,
    const execution::compilation::hashtable::Descriptor &hash_table_descriptor,
    std::vector<mx::tasking::TaskInterface *> &preparatory_tasks)
{
    const auto hash_table_size = execution::compilation::hashtable::TableProxy::size(hash_table_descriptor);

    auto hash_tables = std::vector<mx::resource::ptr>{};
    hash_tables.reserve(count_partitions);

    const auto local_worker_id = mx::tasking::runtime::worker_id();

    for (auto hash_table_id = 0U; hash_table_id < count_partitions; ++hash_table_id)
    {
        const auto mapped_worker_id = hash_table_id % count_worker;

        auto hash_table = mx::resource::ptr{};

        if (hash_table_descriptor.table_type() == execution::compilation::hashtable::Descriptor::Type::LinearProbing)
        {
            hash_table = mx::tasking::runtime::new_squad<execution::compilation::hashtable::LinearProbingTable>(
                hash_table_size, mapped_worker_id, hash_table_descriptor);
        }
        else if (hash_table_descriptor.table_type() == execution::compilation::hashtable::Descriptor::Chained)
        {
            hash_table = mx::tasking::runtime::new_squad<execution::compilation::hashtable::ChainedTable>(
                hash_table_size, mapped_worker_id, hash_table_descriptor);
        }
        hash_tables.emplace_back(hash_table);

        auto *zero_out_task = mx::tasking::runtime::new_task<execution::compilation::hashtable::InitializeTableTask>(
            local_worker_id, hash_table.get<execution::compilation::hashtable::AbstractTable>());
        zero_out_task->annotate(std::uint16_t(mapped_worker_id));
        preparatory_tasks.emplace_back(zero_out_task);
    }

    return hash_tables;
}

std::vector<mx::resource::ptr> JoinPlanner::create_partitions(const std::vector<std::uint8_t> &radix_bits,
                                                              const std::uint8_t pass, const std::uint16_t count_worker)
{
    auto count = JoinPlanner::count_partitions(radix_bits, pass);

    auto partitions = std::vector<mx::resource::ptr>{};
    partitions.reserve(count * count_worker);

    for (auto worker_id = 0U; worker_id < count_worker; ++worker_id)
    {
        for (auto partition_id = 0U; partition_id < count; ++partition_id)
        {
            partitions.emplace_back(mx::tasking::runtime::new_squad(worker_id));
        }
    }

    return partitions;
}

db::topology::PhysicalSchema JoinPlanner::create_partition_schema(
    topology::PhysicalSchema &&optimized_schema,
    const std::unique_ptr<execution::compilation::OperatorInterface> &child)
{
    if constexpr (config::is_materialize_all_columns_on_partitioning() == false)
    {
        return std::move(optimized_schema);
    }
    else
    {
        return topology::PhysicalSchema{child->schema()};
    }
}