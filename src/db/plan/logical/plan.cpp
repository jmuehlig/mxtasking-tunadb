#include "plan.h"
#include "adjuster.h"
#include "adjustments/add_arithmetic_node_for_aggregation_node_adjustment.h"
#include "adjustments/explicit_cast_adjustment.h"
#include "adjustments/predicate_value_right_of_attribute_adjustment.h"
#include "adjustments/resolve_predicate_source_adjustment.h"
#include "adjustments/resolve_user_defined_function_adjustment.h"
#include "node/aggregation_node.h"
#include "node/arithmetic_node.h"
#include "node/copy_node.h"
#include "node/create_table_node.h"
#include "node/cross_product_node.h"
#include "node/explain_node.h"
#include "node/insert_node.h"
#include "node/join_node.h"
#include "node/limit_node.h"
#include "node/materialize_node.h"
#include "node/order_by_node.h"
#include "node/projection_node.h"
#include "node/selection_node.h"
#include "node/table_node.h"
#include "node/user_defined_node.h"
#include "table.h"
#include <db/exception/plan_exception.h>
#include <db/parser/node.h>

using namespace db::plan::logical;

Plan Plan::build(const topology::Database &database, std::unique_ptr<parser::NodeInterface> &&abstract_syntax_tree)
{
    auto root_node = Plan::build_node(std::move(abstract_syntax_tree));

    /// Emit the schema.
    auto child_iterator = TreeNodeChildIterator{};
    std::ignore = root_node->emit_relation(database, child_iterator, false);

    auto plan = Plan{std::move(root_node)};

    /// Adjust the plan for select queries.
    if (plan.is_select_query())
    {
        auto adjuster = Adjuster{};

        /// Move the value right to the attribute in WHERE statements (1 > id -> id < 1).
        adjuster.add(std::make_unique<PredicateValueRightOfAttributeAdjustment>());

        /// Resolve predicate sources (e.g., WHERE x = 1 -> WHERE foo.x = 1).
        adjuster.add(std::make_unique<ResolvePredicateSourceAdjustment>());

        /// Add CAST expressions for expressions and predicates (id - 1.0 -> cast(id, DECIMAL)).
        adjuster.add(std::make_unique<ExplicitCastAdjustment>());

        /// For compiled queries: Arithmetics in aggregations are calculated separately.
        adjuster.add(std::make_unique<AddArithmeticNodeForAggregationNodeAdjustment>());

        /// Resolve user defined functions from names.
        adjuster.add(std::make_unique<ResolveUserDefinedFunctionAdjustment>(database));

        /// Apply adjustments.
        adjuster.adjust(plan.root_node());

        /// Rebuild schema.
        std::ignore = plan.root_node()->emit_relation(database, child_iterator, false);
    }

    return plan;
}

std::unique_ptr<NodeInterface> Plan::build_node(std::unique_ptr<parser::NodeInterface> &&parser_node)
{
    auto *node = parser_node.get();
    if (typeid(*node) == typeid(parser::CreateStatement))
    {
        auto *create_statement = reinterpret_cast<parser::CreateStatement *>(node);
        return std::make_unique<CreateTableNode>(std::move(create_statement->table_name()),
                                                 std::move(create_statement->schema()),
                                                 create_statement->if_not_exists());
    }

    if (typeid(*node) == typeid(parser::InsertStatement))
    {
        auto *insert_statement = reinterpret_cast<parser::InsertStatement *>(node);
        return std::make_unique<InsertNode>(std::move(insert_statement->table_name()),
                                            std::move(insert_statement->column_names()),
                                            std::move(insert_statement->values()));
    }

    if (typeid(*node) == typeid(parser::CopyStatement))
    {
        auto *copy_statement = reinterpret_cast<parser::CopyStatement *>(node);
        return std::make_unique<CopyNode>(std::move(copy_statement->table_name()), std::move(copy_statement->file()),
                                          std::move(copy_statement->separator()));
    }

    if (typeid(*node) == typeid(parser::SelectQuery))
    {
        return Plan::build_select_query(std::move(parser_node));
    }

    if (typeid(*node) == typeid(parser::StopCommand))
    {
        return std::make_unique<StopNode>();
    }

    if (typeid(*node) == typeid(parser::ShowTablesCommand))
    {
        return std::make_unique<ShowTablesNode>();
    }

    if (typeid(*node) == typeid(parser::DescribeTableCommand))
    {
        auto *describe_table_command = reinterpret_cast<parser::DescribeTableCommand *>(node);
        return std::make_unique<DescribeTableNode>(std::move(describe_table_command->table_name()));
    }

    if (typeid(*node) == typeid(parser::LoadFileCommand))
    {
        auto *load_file_command = reinterpret_cast<parser::LoadFileCommand *>(node);
        return std::make_unique<LoadFileNode>(std::move(load_file_command->file()));
    }

    if (typeid(*node) == typeid(parser::StoreCommand))
    {
        auto *store_command = reinterpret_cast<parser::StoreCommand *>(node);
        return std::make_unique<StoreNode>(std::move(store_command->file_name()));
    }

    if (typeid(*node) == typeid(parser::RestoreCommand))
    {
        auto *restore_command = reinterpret_cast<parser::RestoreCommand *>(node);
        return std::make_unique<RestoreNode>(std::move(restore_command->file_name()));
    }

    if (typeid(*node) == typeid(parser::GetConfigurationCommand))
    {
        return std::make_unique<GetConfigurationNode>();
    }

    if (typeid(*node) == typeid(parser::SetCoresCommand))
    {
        auto *set_cores_command = reinterpret_cast<parser::SetCoresCommand *>(node);
        return std::make_unique<SetCoresNode>(set_cores_command->count_cores());
    }

    if (typeid(*node) == typeid(parser::UpdateStatisticsCommand))
    {
        auto *update_statistics_command = reinterpret_cast<parser::UpdateStatisticsCommand *>(node);
        return std::make_unique<UpdateStatisticsNode>(std::move(update_statistics_command->table_name()));
    }

    throw exception::PlanningException{"Logical Builder can not build plan / unknown node in AST."};
}

std::unique_ptr<NodeInterface> Plan::build_select_query(std::unique_ptr<parser::NodeInterface> &&parser_node)
{
    auto *select_node = reinterpret_cast<parser::SelectQuery *>(parser_node.get());

    /// Attributes
    auto &attributes = select_node->attributes();
    auto projection_terms = std::vector<expression::Term>{};
    projection_terms.reserve(attributes.size());
    for (auto &attribute : attributes)
    {
        if (attribute->is_user_defined_function())
        {
            auto *user_defined_function = reinterpret_cast<expression::UserDefinedFunctionOperation *>(attribute.get());
            for (auto &child : user_defined_function->children())
            {
                projection_terms.emplace_back(child->result().value());
            }
        }
        else
        {
            projection_terms.emplace_back(attribute->result().value());
        }
    }

    auto table_references = std::move(select_node->from());
    auto join_references = std::move(select_node->join());

    /// Extract where nodes, these may contain EXISTS which will be
    /// un-nested at first.
    auto where_parts = std::optional<std::vector<std::unique_ptr<expression::Operation>>>{std::nullopt};
    if (select_node->where() != nullptr)
    {
        where_parts = Plan::split_logical_and(std::move(select_node->where()));
        Plan::unnest_exists(table_references, join_references, where_parts.value());
    }

    /// This "top" node will be changed by adding
    /// further nodes on top.
    std::unique_ptr<NodeInterface> top;

    /// FROM and JOIN
    top = Plan::build_from(std::move(table_references), std::move(join_references));

    /// WHERE
    if (where_parts.has_value())
    {
        /// AND operations (r.id > 7 AND s.year < 2020) are split into multiple nodes
        /// in terms of push-down optimization. The optimizer will merge them after
        /// push-down, when possible.
        for (auto &where_part : where_parts.value())
        {
            auto selection = std::make_unique<SelectionNode>(std::move(where_part));
            selection->child(std::move(top));
            top = std::move(selection);
        }
    }

    /// ARITHMETIC and AGGREGATION
    auto aggregations = std::vector<std::unique_ptr<expression::Operation>>{};
    auto arithmetics = std::vector<std::unique_ptr<expression::Operation>>{};
    auto user_defined_functions = std::vector<std::unique_ptr<expression::UserDefinedFunctionOperation>>{};
    aggregations.reserve(attributes.size());
    arithmetics.reserve(attributes.size());
    user_defined_functions.reserve(attributes.size());
    for (auto &attribute : attributes)
    {
        if (attribute->is_aggregation())
        {
            aggregations.emplace_back(std::move(attribute));
        }
        else if (attribute->is_arithmetic())
        {
            Plan::extract_aggregation_from_arithmetic(attribute, aggregations);
            arithmetics.emplace_back(std::move(attribute));
        }
        else if (attribute->is_user_defined_function())
        {
            auto user_defined_function = std::unique_ptr<expression::UserDefinedFunctionOperation>(
                reinterpret_cast<expression::UserDefinedFunctionOperation *>(attribute.release()));
            user_defined_functions.emplace_back(std::move(user_defined_function));
        }
    }
    attributes.clear();

    if (aggregations.empty() == false)
    {
        auto aggregation = std::make_unique<AggregationNode>(std::move(aggregations), select_node->group_by());
        aggregation->child(std::move(top));
        top = std::move(aggregation);
    }

    if (arithmetics.empty() == false)
    {
        auto arithmetic = std::make_unique<ArithmeticNode>(std::move(arithmetics));
        arithmetic->child(std::move(top));
        top = std::move(arithmetic);
    }

    /// PROJECTION
    auto projection = std::make_unique<ProjectionNode>(std::move(projection_terms));
    projection->child(std::move(top));
    top = std::move(projection);

    /// ORDER BY
    if (select_node->order_by().has_value())
    {
        auto order_by = std::make_unique<OrderByNode>(std::move(select_node->order_by().value()));
        order_by->child(std::move(top));
        top = std::move(order_by);
    }

    /// LIMIT
    if (select_node->limit().has_value())
    {
        auto limit = std::make_unique<LimitNode>(select_node->limit().value());
        limit->child(std::move(top));
        top = std::move(limit);
    }

    /// Materialize
    auto materialize = std::make_unique<MaterializeNode>();
    materialize->child(std::move(top));
    top = std::move(materialize);

    /// User defined function.
    if (user_defined_functions.empty() == false)
    {
        auto user_defined_function = std::make_unique<UserDefinedNode>(std::move(user_defined_functions));
        user_defined_function->child(std::move(top));

        /// Materialize
        materialize = std::make_unique<MaterializeNode>();
        materialize->child(std::move(user_defined_function));
        top = std::move(materialize);
    }

    /// EXPLAIN or SAMPLE
    if (select_node->explain_level().has_value())
    {
        const auto level = Plan::extract_explain_level(select_node->explain_level().value());
        auto explain = std::make_unique<ExplainNode>(level);
        explain->child(std::move(top));
        top = std::move(explain);
    }
    else if (select_node->sample_counter_type().has_value())
    {
        const auto [level, counter_type] = Plan::extract_sample_level_type(select_node->sample_level().value(),
                                                                           select_node->sample_counter_type().value());
        auto sample = std::make_unique<SampleNode>(level, counter_type, select_node->sample_frequency());
        sample->child(std::move(top));
        top = std::move(sample);
    }

    return top;
}

std::unique_ptr<NodeInterface> Plan::build_from(std::vector<plan::logical::TableReference> &&from,
                                                std::optional<std::vector<plan::logical::JoinReference>> &&join)
{
    if (from.empty())
    {
        throw exception::PlanningException{"Missing FROM."};
    }

    if (join.has_value() && join->empty() == false)
    {
        /// (1) Build SCAN from "JOIN X" part.
        auto join_descriptor = std::move(join.value().front());
        auto left_node = std::make_unique<TableNode>(std::move(join_descriptor.join_table()));

        /// (2) Build other FROM parts.
        join.value().erase(join->begin());
        auto right_node = Plan::build_from(std::move(from), std::move(join));

        //        auto materialize = std::make_unique<MaterializeNode>();
        //        materialize->child(std::move(right_node));

        /// Join (1) and (2).
        return std::make_unique<JoinNode>(std::move(join_descriptor.predicate()), std::move(left_node),
                                          std::move(right_node));
    }

    if (from.size() > 1U)
    {
        /// (1) Build SCAN from "FROM X,..." part.
        auto left_node = std::make_unique<TableNode>(std::move(from.front()));

        /// (2) Build other FROM parts.
        from.erase(from.begin());
        auto right_node = Plan::build_from(std::move(from), std::nullopt);
        //        auto materialize = std::make_unique<MaterializeNode>();
        //        materialize->child(std::move(right_node));

        /// Join (1) and (2).
        return std::make_unique<CrossProductNode>(std::move(left_node), std::move(right_node));
    }

    /// No joins, just a single "FROM".
    return std::make_unique<TableNode>(std::move(from.front()));
}

bool Plan::is_select_query() const noexcept
{
    auto *top = this->_root_node.get();

    /// Select query can have an "EXPLAIN" or "SAMPLE".
    if (typeid(*top) == typeid(ExplainNode))
    {
        top = reinterpret_cast<ExplainNode *>(top)->child().get();
    }
    else if (typeid(*top) == typeid(SampleNode))
    {
        top = reinterpret_cast<SampleNode *>(top)->child().get();
    }

    /// Each SELECT query starts with an Projection node
    /// (at least before optimization).
    return top->query_type() == NodeInterface::QueryType::SELECT;
}

std::vector<std::unique_ptr<db::expression::Operation>> Plan::split_logical_and(
    std::unique_ptr<expression::Operation> &&operation)
{
    auto container = std::vector<std::unique_ptr<db::expression::Operation>>{};
    Plan::split_logical_and(std::move(operation), container);
    return container;
}

void Plan::split_logical_and(std::unique_ptr<expression::Operation> &&operation,
                             std::vector<std::unique_ptr<expression::Operation>> &container)
{
    if (operation->id() == expression::Operation::Id::And)
    {
        auto *and_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());
        Plan::split_logical_and(std::move(and_operation->left_child()), container);
        Plan::split_logical_and(std::move(and_operation->right_child()), container);
    }
    else
    {
        container.emplace_back(std::move(operation));
    }
}

void Plan::unnest_exists(std::vector<TableReference> &table_references,
                         std::optional<std::vector<JoinReference>> &join_references,
                         std::vector<std::unique_ptr<expression::Operation>> &where_parts)
{
    /**
     * TODO: This un-nests EXISTS to JOINs but what we need is a SEMI JOIN.
     *  The results are wrong.
     */

    auto additional_where_parts = std::vector<std::unique_ptr<expression::Operation>>{};

    for (auto &where_part : where_parts)
    {
        if (where_part->id() == expression::Operation::Exists)
        {
            auto &sub_query = reinterpret_cast<expression::ExistsOperation *>(where_part.get())->sub_query();
            auto *select_query = reinterpret_cast<parser::SelectQuery *>(sub_query.get());

            /// Add the table references.
            std::move(select_query->from().begin(), select_query->from().end(), std::back_inserter(table_references));

            /// Add the joins references.
            if (select_query->join().has_value() && select_query->join()->empty() == false)
            {
                if (join_references.has_value() == false)
                {
                    join_references.emplace(std::move(select_query->join().value()));
                }
                else
                {
                    std::move(select_query->join()->begin(), select_query->join()->end(),
                              std::back_inserter(join_references.value()));
                }
            }

            /// Split the where clauses and add later, because we are iterating over them.
            if (select_query->where() != nullptr)
            {
                Plan::split_logical_and(std::move(select_query->where()), additional_where_parts);
            }
        }
    }

    /// Remove the un-nested exists.
    where_parts.erase(
        std::remove_if(where_parts.begin(), where_parts.end(),
                       [](const auto &where) { return where->id() == expression::Operation::Id::Exists; }),
        where_parts.end());

    /// Add the where clauses.
    if (additional_where_parts.empty() == false)
    {
        std::move(additional_where_parts.begin(), additional_where_parts.end(), std::back_inserter(where_parts));
    }
}

void Plan::extract_aggregation_from_arithmetic(std::unique_ptr<expression::Operation> &arithmetic,
                                               std::vector<std::unique_ptr<expression::Operation>> &aggregations)
{
    if (arithmetic->is_binary())
    {
        auto *binary_arithmetic = reinterpret_cast<expression::BinaryOperation *>(arithmetic.get());

        /// Arithmetic could contain aggregation (i.e., SUM(a) * SUM(b))
        if (binary_arithmetic->left_child()->is_aggregation())
        {
            auto aggregated_attribute =
                Plan::replace_aggregation_by_attribute(std::move(binary_arithmetic->left_child()), aggregations);
            binary_arithmetic->left_child(std::move(aggregated_attribute));
        }
        else if (binary_arithmetic->left_child()->is_arithmetic())
        {
            Plan::extract_aggregation_from_arithmetic(binary_arithmetic->left_child(), aggregations);
        }

        if (binary_arithmetic->right_child()->is_aggregation())
        {
            auto aggregated_attribute =
                Plan::replace_aggregation_by_attribute(std::move(binary_arithmetic->right_child()), aggregations);
            binary_arithmetic->right_child(std::move(aggregated_attribute));
        }
        else if (binary_arithmetic->right_child()->is_arithmetic())
        {
            Plan::extract_aggregation_from_arithmetic(binary_arithmetic->right_child(), aggregations);
        }
    }
    else if (arithmetic->is_unary())
    {
        auto *unary_arithmetic = reinterpret_cast<expression::UnaryOperation *>(arithmetic.get());

        /// Arithmetic could contain aggregation (i.e., SUM(a) * SUM(b))
        if (unary_arithmetic->child()->is_aggregation())
        {
            auto aggregated_attribute =
                Plan::replace_aggregation_by_attribute(std::move(unary_arithmetic->child()), aggregations);
            unary_arithmetic->child(std::move(aggregated_attribute));
        }
        else if (unary_arithmetic->is_arithmetic())
        {
            Plan::extract_aggregation_from_arithmetic(unary_arithmetic->child(), aggregations);
        }
    }
}

std::unique_ptr<db::expression::Operation> Plan::replace_aggregation_by_attribute(
    std::unique_ptr<expression::Operation> &&aggregation,
    std::vector<std::unique_ptr<expression::Operation>> &aggregations)
{
    /// Let arithmetic work on the result of the aggregation.
    auto attribute = std::make_unique<expression::NullaryOperation>(expression::Term{aggregation->result().value()});

    /// Push aggregation before.
    aggregations.emplace_back(std::move(aggregation));

    return attribute;
}

ExplainNode::Level Plan::extract_explain_level(const parser::SelectQuery::ExplainLevel explain_level) noexcept
{
    switch (explain_level)
    {
    case parser::SelectQuery::ExplainLevel::Performance:
        return ExplainNode::Level::Performance;
    case parser::SelectQuery::ExplainLevel::TaskGraph:
        return ExplainNode::Level::TaskGraph;
    case parser::SelectQuery::ExplainLevel::DataFlowGraph:
        return ExplainNode::Level::DataFlowGraph;
    case parser::SelectQuery::ExplainLevel::TaskLoad:
        return ExplainNode::Level::TaskLoad;
    case parser::SelectQuery::ExplainLevel::TaskTraces:
        return ExplainNode::Level::TaskTraces;
    case parser::SelectQuery::ExplainLevel::Flounder:
        return ExplainNode::Level::Flounder;
    case parser::SelectQuery::ExplainLevel::Assembly:
        return ExplainNode::Level::Assembly;
    case parser::SelectQuery::ExplainLevel::Plan:
        return ExplainNode::Level::Plan;
    case parser::SelectQuery::ExplainLevel::DRAMBandwidth:
        return ExplainNode::Level::DRAMBandwidth;
    case parser::SelectQuery::ExplainLevel::Times:
        return ExplainNode::Level::Times;
    }
}

std::pair<SampleNode::Level, SampleNode::CounterType> Plan::extract_sample_level_type(
    const parser::SelectQuery::SampleLevel sample_level,
    const parser::SelectQuery::SampleCounterType sample_counter_type) noexcept
{
    auto level = SampleNode::Level::Assembly;
    if (sample_level == parser::SelectQuery::SampleLevel::Operators)
    {
        level = SampleNode::Level::Operators;
    }
    else if (sample_level == parser::SelectQuery::SampleLevel::Memory)
    {
        level = SampleNode::Level::Memory;
    }
    else if (sample_level == parser::SelectQuery::SampleLevel::HistoricalMemory)
    {
        level = SampleNode::Level::HistoricalMemory;
    }

    switch (sample_counter_type)
    {
    case parser::SelectQuery::SampleCounterType::Branches:
        return std::make_pair(level, SampleNode::CounterType::Branches);
    case parser::SelectQuery::SampleCounterType::BranchMisses:
        return std::make_pair(level, SampleNode::CounterType::BranchMisses);
    case parser::SelectQuery::SampleCounterType::Cycles:
        return std::make_pair(level, SampleNode::CounterType::Cycles);
    case parser::SelectQuery::SampleCounterType::Instructions:
        return std::make_pair(level, SampleNode::CounterType::Instructions);
    case parser::SelectQuery::SampleCounterType::CacheMisses:
        return std::make_pair(level, SampleNode::CounterType::CacheMisses);
    case parser::SelectQuery::SampleCounterType::CacheReferences:
        return std::make_pair(level, SampleNode::CounterType::CacheReferences);
    case parser::SelectQuery::SampleCounterType::StallsMemAny:
        return std::make_pair(level, SampleNode::CounterType::StallsMemAny);
    case parser::SelectQuery::SampleCounterType::StallsL3Miss:
        return std::make_pair(level, SampleNode::CounterType::StallsL3Miss);
    case parser::SelectQuery::SampleCounterType::StallsL2Miss:
        return std::make_pair(level, SampleNode::CounterType::StallsL2Miss);
    case parser::SelectQuery::SampleCounterType::StallsL1DMiss:
        return std::make_pair(level, SampleNode::CounterType::StallsL1DMiss);
    case parser::SelectQuery::SampleCounterType::CyclesL3Miss:
        return std::make_pair(level, SampleNode::CounterType::CyclesL3Miss);
    case parser::SelectQuery::SampleCounterType::DTLBMiss:
        return std::make_pair(level, SampleNode::CounterType::DTLBMiss);
    case parser::SelectQuery::SampleCounterType::L3MissRemote:
        return std::make_pair(level, SampleNode::CounterType::L3MissRemote);
    case parser::SelectQuery::SampleCounterType::FillBufferFull:
        return std::make_pair(level, SampleNode::CounterType::FillBufferFull);
    case parser::SelectQuery::SampleCounterType::LoadHitL1DFillBuffer:
        return std::make_pair(level, SampleNode::LoadHitL1DFillBuffer);
    case parser::SelectQuery::SampleCounterType::MemRetiredLoads:
        return std::make_pair(level, SampleNode::MemRetiredLoads);
    case parser::SelectQuery::SampleCounterType::MemRetiredStores:
        return std::make_pair(level, SampleNode::MemRetiredStores);
    case parser::SelectQuery::SampleCounterType::MemRetiredLoadL1Miss:
        return std::make_pair(level, SampleNode::MemRetiredLoadL1Miss);
    case parser::SelectQuery::SampleCounterType::MemRetiredLoadL2Miss:
        return std::make_pair(level, SampleNode::MemRetiredLoadL2Miss);
    case parser::SelectQuery::SampleCounterType::MemRetiredLoadL3Miss:
        return std::make_pair(level, SampleNode::MemRetiredLoadL3Miss);
    case parser::SelectQuery::SampleCounterType::BAClearsAny:
        return std::make_pair(level, SampleNode::CounterType::BAClearsAny);
    }
}