#include "add_arithmetic_node_for_aggregation_node_adjustment.h"
#include <db/plan/logical/node/aggregation_node.h>
#include <db/plan/logical/node/arithmetic_node.h>
#include <db/plan/logical/node/selection_node.h>

using namespace db::plan::logical;

void AddArithmeticNodeForAggregationNodeAdjustment::apply(std::unique_ptr<NodeInterface> &node)
{
    if (node->is_unary())
    {
        if (typeid(*node) == typeid(AggregationNode))
        {
            auto arithmetic_operations = std::vector<std::unique_ptr<expression::Operation>>{};

            auto *aggregation_node = reinterpret_cast<AggregationNode *>(node.get());
            for (auto &aggregation : aggregation_node->aggregation_operations())
            {
                auto *aggregation_operation = reinterpret_cast<expression::UnaryOperation *>(aggregation.get());
                auto &child = aggregation_operation->child();

                /// Replace the aggregations arithmetic child by an attribute operation
                /// and put arithmetic aside.
                if (child->is_arithmetic())
                {
                    auto attribute_operation = std::make_unique<expression::NullaryOperation>(
                        expression::Term::make_attribute(child->result()->to_string()));
                    arithmetic_operations.emplace_back(std::move(child));
                    aggregation_operation->child(std::move(attribute_operation));
                }
            }

            /// Insert the a new arithmetic node as a child of the aggregation node.
            if (arithmetic_operations.empty() == false)
            {
                auto arithmetic_node = std::make_unique<ArithmeticNode>(std::move(arithmetic_operations));
                arithmetic_node->child(std::move(aggregation_node->child()));
                aggregation_node->child(std::move(arithmetic_node));
            }
        }
        else if (typeid(*node) == typeid(SelectionNode))
        {
            auto *selection_node = reinterpret_cast<SelectionNode *>(node.get());
            auto arithmetic_operations = std::vector<std::unique_ptr<expression::Operation>>{};
            AddArithmeticNodeForAggregationNodeAdjustment::extract_arithmetic(selection_node->predicate(),
                                                                              arithmetic_operations);

            /// Insert the a new arithmetic node as a child of the aggregation node.
            if (arithmetic_operations.empty() == false)
            {
                auto arithmetic_node = std::make_unique<ArithmeticNode>(std::move(arithmetic_operations));
                arithmetic_node->child(std::move(selection_node->child()));
                selection_node->child(std::move(arithmetic_node));
            }
        }
        else
        {
            this->apply(reinterpret_cast<UnaryNode *>(node.get())->child());
        }
    }
    else if (node->is_binary())
    {
        this->apply(reinterpret_cast<BinaryNode *>(node.get())->left_child());
        this->apply(reinterpret_cast<BinaryNode *>(node.get())->right_child());
    }
}

void AddArithmeticNodeForAggregationNodeAdjustment::extract_arithmetic(
    std::unique_ptr<expression::Operation> &predicate, std::vector<std::unique_ptr<expression::Operation>> &arithmetics)
{
    if (predicate->is_arithmetic())
    {
        auto *binary_expression = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        if (binary_expression->left_child()->result()->is_value() == false &&
            binary_expression->right_child()->result()->is_value() == false)
        {
            auto nullary = std::make_unique<expression::NullaryOperation>(
                expression::Term::make_attribute(predicate->to_string(), true));
            arithmetics.emplace_back(std::move(predicate));
            predicate = std::move(nullary);
        }
    }
    else if (predicate->is_binary())
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        AddArithmeticNodeForAggregationNodeAdjustment::extract_arithmetic(binary_operation->left_child(), arithmetics);
        AddArithmeticNodeForAggregationNodeAdjustment::extract_arithmetic(binary_operation->right_child(), arithmetics);
    }
    else if (predicate->is_unary())
    {
        AddArithmeticNodeForAggregationNodeAdjustment::extract_arithmetic(
            reinterpret_cast<expression::UnaryOperation *>(predicate.get())->child(), arithmetics);
    }
}