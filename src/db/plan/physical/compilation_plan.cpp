#include "compilation_plan.h"
#include "radix_bit_calculator.h"
#include <db/exception/execution_exception.h>
#include <db/execution/compilation/bloom_filter.h>
#include <db/execution/compilation/hashtable/chained_table.h>
#include <db/execution/compilation/hashtable/linear_probing_table.h>
#include <db/execution/compilation/operator/aggregation_operator.h>
#include <db/execution/compilation/operator/arithmetic_operator.h>
#include <db/execution/compilation/operator/buffer_operator.h>
#include <db/execution/compilation/operator/grouped_aggregation_operator.h>
#include <db/execution/compilation/operator/hash_join_operator.h>
#include <db/execution/compilation/operator/limit_operator.h>
#include <db/execution/compilation/operator/materialize_operator.h>
#include <db/execution/compilation/operator/nested_loops_join_operator.h>
#include <db/execution/compilation/operator/partition_filter_operator.h>
#include <db/execution/compilation/operator/partition_operator.h>
#include <db/execution/compilation/operator/radix_aggregation_operator.h>
#include <db/execution/compilation/operator/radix_join_operator.h>
#include <db/execution/compilation/operator/scan_operator.h>
#include <db/execution/compilation/operator/selection_operator.h>
#include <db/execution/compilation/operator/user_defined_operator.h>
#include <db/plan/logical/node/aggregation_node.h>
#include <db/plan/logical/node/arithmetic_node.h>
#include <db/plan/logical/node/join_node.h>
#include <db/plan/logical/node/limit_node.h>
#include <db/plan/logical/node/materialize_node.h>
#include <db/plan/logical/node/selection_node.h>
#include <db/plan/logical/node/table_node.h>
#include <db/plan/logical/node/table_selection_node.h>
#include <db/plan/logical/node/user_defined_node.h>
#include <db/plan/physical/compilation/join_planner.h>

using namespace db::plan::physical;

CompilationPlan CompilationPlan::build(const topology::Database &database, logical::Plan &&logical_plan)
{
    auto preparatory_tasks = std::vector<mx::tasking::TaskInterface *>{};
    preparatory_tasks.reserve(1024U);

    auto root_operator =
        CompilationPlan::build_operator(database, std::move(logical_plan.root_node()), preparatory_tasks);

    return CompilationPlan{std::move(root_operator), std::move(preparatory_tasks)};
}

std::unique_ptr<db::execution::compilation::OperatorInterface> CompilationPlan::build_operator(
    const topology::Database &database, std::unique_ptr<logical::NodeInterface> &&logical_node,
    std::vector<mx::tasking::TaskInterface *> &preparatory_tasks)
{
    auto *node = logical_node.get();

    if (typeid(*node) == typeid(logical::ExplainNode))
    {
        return CompilationPlan::build_operator(
            database, std::move(reinterpret_cast<logical::ExplainNode *>(node)->child()), preparatory_tasks);
    }

    if (typeid(*node) == typeid(logical::SampleNode))
    {
        return CompilationPlan::build_operator(
            database, std::move(reinterpret_cast<logical::SampleNode *>(node)->child()), preparatory_tasks);
    }

    if (typeid(*node) == typeid(logical::ProjectionNode))
    {
        return CompilationPlan::build_operator(
            database, std::move(reinterpret_cast<logical::ProjectionNode *>(node)->child()), preparatory_tasks);
    }

    if (typeid(*node) == typeid(logical::MaterializeNode))
    {
        auto child = CompilationPlan::build_operator(
            database, std::move(reinterpret_cast<logical::MaterializeNode *>(node)->child()), preparatory_tasks);
        auto materialize_operator = std::make_unique<execution::compilation::MaterializeOperator>(
            topology::PhysicalSchema::from_logical(node->relation().schema()));
        materialize_operator->child(std::move(child));

        return materialize_operator;
    }

    if (typeid(*node) == typeid(logical::TableNode))
    {
        const auto &table_reference = reinterpret_cast<logical::TableNode *>(node)->table();
        const auto &table = database[table_reference.name()];
        return std::make_unique<execution::compilation::ScanOperator>(
            table, topology::PhysicalSchema::from_logical(node->relation().schema()));
    }

    if (typeid(*node) == typeid(logical::TableSelectionNode))
    {
        auto *table_selection_node = reinterpret_cast<logical::TableSelectionNode *>(node);
        const auto &table_reference = table_selection_node->table();
        const auto &table = database[table_reference.name()];
        return std::make_unique<execution::compilation::ScanOperator>(
            table, topology::PhysicalSchema::from_logical(node->relation().schema()),
            std::move(table_selection_node->predicate()));
    }

    if (typeid(*node) == typeid(logical::JoinNode))
    {
        auto *join_node = reinterpret_cast<logical::JoinNode *>(node);

        const auto expected_build_cardinality = join_node->left_child()->relation().cardinality();
        auto left_child_schema = join_node->left_child()->relation().schema();
        auto right_child_schema = join_node->right_child()->relation().schema();

        auto build_child =
            CompilationPlan::build_operator(database, std::move(join_node->left_child()), preparatory_tasks);
        auto probe_child =
            CompilationPlan::build_operator(database, std::move(join_node->right_child()), preparatory_tasks);

        return compilation::JoinPlanner::build(database, join_node, std::move(left_child_schema),
                                               std::move(build_child), std::move(right_child_schema),
                                               std::move(probe_child), expected_build_cardinality, preparatory_tasks);
    }

    if (typeid(*node) == typeid(logical::SelectionNode))
    {
        auto *selection_node = reinterpret_cast<logical::SelectionNode *>(node);
        auto child = CompilationPlan::build_operator(database, std::move(selection_node->child()), preparatory_tasks);

        auto selection_operator = std::make_unique<execution::compilation::SelectionOperator>(
            topology::PhysicalSchema::from_logical(node->relation().schema()), std::move(selection_node->predicate()));
        selection_operator->child(std::move(child));

        return selection_operator;
    }

    if (typeid(*node) == typeid(logical::ArithmeticNode))
    {
        auto *arithmetic_node = reinterpret_cast<logical::ArithmeticNode *>(node);

        auto child = CompilationPlan::build_operator(database, std::move(arithmetic_node->child()), preparatory_tasks);

        auto arithmetic_operator = std::make_unique<execution::compilation::ArithmeticOperator>(
            topology::PhysicalSchema::from_logical(arithmetic_node->relation().schema()),
            std::move(arithmetic_node->arithmetic_operations()));

        arithmetic_operator->child(std::move(child));

        return arithmetic_operator;
    }

    if (typeid(*node) == typeid(logical::AggregationNode))
    {
        auto *aggregation_node = reinterpret_cast<logical::AggregationNode *>(node);
        auto child = CompilationPlan::build_operator(
            database, std::move(reinterpret_cast<logical::AggregationNode *>(node)->child()), preparatory_tasks);

        /// Full schema of the operator.
        auto schema = topology::PhysicalSchema::from_logical(aggregation_node->relation().schema());

        /// Schema only for aggregations.
        auto aggregation_schema = execution::compilation::AbstractAggregationOperator::make_aggregation_schema(
            schema, aggregation_node->aggregation_operations());

        if (aggregation_node->groups().has_value())
        {
            const auto count_workers = mx::tasking::runtime::workers();
            auto group_schema = execution::compilation::AbstractAggregationOperator::make_group_schema(
                child->schema(), std::move(aggregation_node->groups().value()));

            const auto expected_cardinality = aggregation_node->relation().cardinality();

            /// Type of the hash table.
            const auto hash_table_type = execution::compilation::hashtable::Descriptor::Type::LinearProbing;

            /// Use radix aggregation to build small hash tables.
            //            if (expected_cardinality > 1000U)
            if (aggregation_node->method() == logical::AggregationNode::Method::RadixAggregation)
            {
                /// Create radix bits.
                auto radix_bits = RadixBitCalculator::calculate(hash_table_type, count_workers, expected_cardinality,
                                                                aggregation_schema, group_schema.row_size(), 1U);
                const auto count_partitions = RadixBitCalculator::count_partitions(radix_bits);

                /// Build descriptor for the hash table.
                const auto capacity_per_table = execution::compilation::hashtable::TableProxy::allocation_capacity(
                    expected_cardinality / count_partitions, hash_table_type);

                auto hash_table_descriptor = execution::compilation::hashtable::Descriptor{
                    hash_table_type, capacity_per_table, group_schema.row_size(), aggregation_schema.row_size()};

                /// Hash tables are shared partitions.
                auto hash_tables = compilation::JoinPlanner::create_hash_tables(
                    count_partitions, count_workers, hash_table_descriptor, preparatory_tasks);

                /// Create partitions.
                const auto &incoming_schema = child->schema();
                for (auto partition_pass = 0U; partition_pass < radix_bits.size(); ++partition_pass)
                {
                    const auto is_last_pass = partition_pass == radix_bits.size() - 1U;
                    if (is_last_pass == false)
                    {
                        auto partitions =
                            CompilationPlan::build_radix_partitions(radix_bits, partition_pass, count_workers);
                        auto partition_schema = topology::PhysicalSchema{incoming_schema};
                        partition_schema.emplace_back(
                            expression::Term{execution::compilation::PartitionOperator::partition_hash_term},
                            type::Type::make_bigint());

                        auto materialize_partition_operator =
                            std::make_unique<execution::compilation::MaterializePartitionOperator>(
                                topology::PhysicalSchema{partition_schema}, std::move(partitions), false, true);

                        auto partition_operator = std::make_unique<execution::compilation::PartitionOperator>(
                            std::move(partition_schema), group_schema.terms(), radix_bits, partition_pass);
                        partition_operator->child(std::move(child));

                        materialize_partition_operator->child(std::move(partition_operator));
                        child = std::move(materialize_partition_operator);
                    }
                    else
                    {
                        auto partition_operator = std::make_unique<execution::compilation::PartitionOperator>(
                            topology::PhysicalSchema{incoming_schema}, group_schema.terms(), radix_bits,
                            partition_pass);
                        partition_operator->child(std::move(child));
                        child = std::move(partition_operator);

                        auto materialize_partition_operator =
                            std::make_unique<execution::compilation::MaterializePartitionOperator>(
                                topology::PhysicalSchema{incoming_schema}, hash_tables, true, true);
                        materialize_partition_operator->child(std::move(child));
                        child = std::move(materialize_partition_operator);
                    }
                }

                /// Create Radix Aggregation Operator
                auto aggregation_operator = std::make_unique<execution::compilation::RadixAggregationOperator>(
                    std::move(schema), std::move(group_schema), std::move(aggregation_schema), child->schema(),
                    std::move(aggregation_node->aggregation_operations()), std::move(hash_tables),
                    hash_table_descriptor);
                aggregation_operator->child(std::move(child));
                return aggregation_operator;
            }

            /// Let all workers aggregate locally and merge afterwards.
            /// Build hash tables for every worker.
            const auto hash_table_capacity = execution::compilation::hashtable::TableProxy::allocation_capacity(
                expected_cardinality, hash_table_type);

            auto hash_table_descriptor = execution::compilation::hashtable::Descriptor{
                hash_table_type, hash_table_capacity, group_schema.row_size(), aggregation_schema.row_size()};
            auto hash_tables =
                CompilationPlan::build_aggregation_hash_tables(count_workers, hash_table_descriptor, preparatory_tasks);

            auto aggregation_operator = std::make_unique<execution::compilation::GroupedAggregationOperator>(
                std::move(schema), std::move(group_schema), std::move(aggregation_schema), child->schema(),
                std::move(aggregation_node->aggregation_operations()), std::move(hash_tables), hash_table_descriptor);
            aggregation_operator->child(std::move(child));

            return aggregation_operator;
        }

        /// Aggregation without groups.
        auto aggregation_operator = std::make_unique<execution::compilation::AggregationOperator>(
            std::move(schema), std::move(aggregation_schema), child->schema(),
            std::move(aggregation_node->aggregation_operations()));
        aggregation_operator->child(std::move(child));

        return aggregation_operator;
    }

    if (typeid(*node) == typeid(logical::LimitNode))
    {
        auto *limit_node = reinterpret_cast<logical::LimitNode *>(node);
        auto child = CompilationPlan::build_operator(database, std::move(limit_node->child()), preparatory_tasks);

        auto limit_operator = std::make_unique<execution::compilation::LimitOperator>(
            topology::PhysicalSchema::from_logical(node->relation().schema()), limit_node->limit());
        limit_operator->child(std::move(child));

        return limit_operator;
    }

    if (typeid(*node) == typeid(logical::UserDefinedNode))
    {
        auto *user_defined_function_node = reinterpret_cast<logical::UserDefinedNode *>(node);
        auto child = CompilationPlan::build_operator(database, std::move(user_defined_function_node->child()),
                                                     preparatory_tasks);

        auto user_defined_function_operator = std::make_unique<execution::compilation::UserDefinedOperator>(
            topology::PhysicalSchema::from_logical(node->relation().schema()),
            std::move(user_defined_function_node->user_defined_functions()));
        user_defined_function_operator->child(std::move(child));

        return user_defined_function_operator;
    }

    throw exception::ExecutionException{"Could not create compilation plan from logical plan. Missing logical node to "
                                        "compilation operator transformation."};
}

std::vector<db::execution::compilation::hashtable::AbstractTable *> CompilationPlan::build_aggregation_hash_tables(
    const std::uint16_t count_partitions, const execution::compilation::hashtable::Descriptor &hash_table_descriptor,
    std::vector<mx::tasking::TaskInterface *> &preparatory_tasks)
{
    /// Create the hash tables.
    auto hash_tables = std::vector<db::execution::compilation::hashtable::AbstractTable *>{};
    hash_tables.reserve(count_partitions);

    const auto local_worker_id = mx::tasking::runtime::worker_id();
    const auto worker_local_ht_size = execution::compilation::hashtable::TableProxy::size(hash_table_descriptor);
    for (auto worker_id = 0U; worker_id < count_partitions; ++worker_id)
    {
        auto *hash_table_data =
            mx::memory::GlobalHeap::allocate(mx::tasking::runtime::numa_node_id(worker_id), worker_local_ht_size);

        execution::compilation::hashtable::AbstractTable *hash_table;

        if (hash_table_descriptor.table_type() == execution::compilation::hashtable::Descriptor::LinearProbing)
        {
            hash_table =
                new (hash_table_data) execution::compilation::hashtable::LinearProbingTable(hash_table_descriptor);
        }
        else if (hash_table_descriptor.table_type() == execution::compilation::hashtable::Descriptor::Chained)
        {
            hash_table = new (hash_table_data) execution::compilation::hashtable::ChainedTable(hash_table_descriptor);
        }

        hash_tables.emplace_back(hash_table);

        auto *zero_out_task = mx::tasking::runtime::new_task<execution::compilation::hashtable::InitializeTableTask>(
            local_worker_id, hash_table);
        zero_out_task->annotate(std::uint16_t(worker_id));
        preparatory_tasks.emplace_back(zero_out_task);
    }

    return hash_tables;
}

std::vector<mx::resource::ptr> CompilationPlan::build_radix_partitions(const std::vector<std::uint8_t> &radix_bits,
                                                                       const std::uint8_t pass,
                                                                       const std::uint16_t count_worker)
{
    auto count = RadixBitCalculator::count_partitions(radix_bits, pass);

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
