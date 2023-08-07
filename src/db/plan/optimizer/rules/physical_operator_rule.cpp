#include "physical_operator_rule.h"
#include <db/plan/logical/node/selection_node.h>
#include <db/plan/logical/node/table_selection_node.h>

using namespace db::plan::optimizer;

bool PhysicalOperatorRule::apply(db::plan::optimizer::PlanView &plan)
{
    auto is_optimized = false;
    for (auto *node : plan.extract_nodes())
    {
        /// Choose JOIN method.
        if (typeid(*node) == typeid(logical::JoinNode))
        {
            auto *join_node = reinterpret_cast<logical::JoinNode *>(node);
            const auto best_join_method = PhysicalOperatorRule::choose_method(plan, join_node);
            if (join_node->method() != best_join_method)
            {
                join_node->method(best_join_method);
                is_optimized = true;
            }
        }

        /// Choose GROUPED AGGREGATION method.
        else if (typeid(*node) == typeid(logical::AggregationNode))
        {
            auto *aggregation_node = reinterpret_cast<logical::AggregationNode *>(node);
            if (aggregation_node->groups().has_value())
            {
                const auto best_aggregation_method = PhysicalOperatorRule::choose_method(aggregation_node);
                if (aggregation_node->method() != best_aggregation_method)
                {
                    aggregation_node->method(best_aggregation_method);
                    is_optimized = true;
                }
            }
        }

        /// Choose ORDER BY method.
        else if (typeid(*node) == typeid(logical::OrderByNode))
        {
            auto *order_by_node = reinterpret_cast<logical::OrderByNode *>(node);
            const auto best_order_method = PhysicalOperatorRule::choose_method(plan, order_by_node);
            if (order_by_node->method() != best_order_method)
            {
                order_by_node->method(best_order_method);
                is_optimized = true;
            }
        }
    }

    return is_optimized;
}

db::plan::logical::JoinNode::Method PhysicalOperatorRule::choose_method(const db::plan::optimizer::PlanView &plan,
                                                                        logical::JoinNode *join_node)
{
    const auto &children = plan.children(join_node);
    if (std::get<0>(children)->relation().cardinality() < 256U)
    {
        if (std::get<1>(children)->relation().cardinality() < 256U)
        {
            return logical::JoinNode::Method::NestedLoopsJoin;
        }
        return logical::JoinNode::Method::HashJoin;
    }

    if (PhysicalOperatorRule::has_only_equal_comparison(join_node->predicate()))
    {
        if (PhysicalOperatorRule::is_selective(plan, std::get<0>(children)))
        {
            return logical::JoinNode::Method::FilteredRadixJoin;
        }

        return logical::JoinNode::Method::RadixJoin;
    }

    return logical::JoinNode::Method::NestedLoopsJoin;
}

db::plan::logical::OrderByNode::Method PhysicalOperatorRule::choose_method(const db::plan::optimizer::PlanView &plan,
                                                                           logical::OrderByNode *order_by_node)
{
    const auto &children = plan.children(order_by_node);

    if (std::get<0>(children)->relation().cardinality() > 100000U)
    {
        return logical::OrderByNode::Method::Parallel;
    }

    return logical::OrderByNode::Method::Sequential;
}

bool PhysicalOperatorRule::has_only_equal_comparison(const std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->id() == expression::Operation::Id::And || predicate->id() == expression::Operation::Id::Or)
    {
        auto *binary_predicate = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        return PhysicalOperatorRule::has_only_equal_comparison(binary_predicate->left_child()) &&
               PhysicalOperatorRule::has_only_equal_comparison(binary_predicate->right_child());
    }

    return predicate->id() == expression::Operation::Id::Equals;
}

bool PhysicalOperatorRule::is_selective(const db::plan::optimizer::PlanView &plan, logical::NodeInterface *node)
{
    if (node->is_binary())
    {
        const auto &children = plan.children(node);
        return PhysicalOperatorRule::is_selective(plan, std::get<0>(children)) ||
               PhysicalOperatorRule::is_selective(plan, std::get<1>(children));
    }

    if (node->is_unary())
    {
        if (typeid(*node) == typeid(logical::SelectionNode))
        {
            return true;
        }

        return PhysicalOperatorRule::is_selective(plan, std::get<0>(plan.children(node)));
    }

    if (node->is_nullary() && typeid(*node) == typeid(logical::TableSelectionNode))
    {
        return true;
    }

    return false;
}
