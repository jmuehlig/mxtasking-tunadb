#include "explicit_cast_adjustment.h"
#include <db/plan/logical/node/aggregation_node.h>
#include <db/plan/logical/node/arithmetic_node.h>
#include <db/plan/logical/node/selection_node.h>

using namespace db::plan::logical;

void ExplicitCastAdjustment::apply(std::unique_ptr<NodeInterface> &node)
{
    if (node->is_unary())
    {
        if (typeid(*node) == typeid(ArithmeticNode))
        {
            auto *arithmetic_node = reinterpret_cast<ArithmeticNode *>(node.get());
            for (auto &operation : arithmetic_node->arithmetic_operations())
            {
                ExplicitCastAdjustment::apply(node->relation().schema(), operation);
            }
        }
        else if (typeid(*node) == typeid(AggregationNode))
        {
            auto *aggregation_node = reinterpret_cast<AggregationNode *>(node.get());
            const auto &child_schema = aggregation_node->child()->relation().schema();
            for (auto &operation : aggregation_node->aggregation_operations())
            {
                ExplicitCastAdjustment::apply(child_schema,
                                              reinterpret_cast<expression::UnaryOperation *>(operation.get())->child());
            }
        }
        else if (typeid(*node) == typeid(SelectionNode))
        {
            auto *selection_node = reinterpret_cast<SelectionNode *>(node.get());
            ExplicitCastAdjustment::apply(node->relation().schema(), selection_node->predicate());
        }

        this->apply(reinterpret_cast<UnaryNode *>(node.get())->child());
    }
    else if (node->is_binary())
    {
        this->apply(reinterpret_cast<BinaryNode *>(node.get())->left_child());
        this->apply(reinterpret_cast<BinaryNode *>(node.get())->right_child());
    }
}

void ExplicitCastAdjustment::apply(const topology::LogicalSchema &schema,
                                   std::unique_ptr<expression::Operation> &operation)
{
    if (operation->is_unary())
    {
        ExplicitCastAdjustment::apply(schema, reinterpret_cast<expression::UnaryOperation *>(operation.get())->child());
    }
    else if (operation->is_binary())
    {
        /// Attribute may be unknown in schema, because schema may be aggregation.
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());

        auto &left_child = binary_operation->left_child();
        auto &right_child = binary_operation->right_child();

        ExplicitCastAdjustment::apply(schema, left_child);
        ExplicitCastAdjustment::apply(schema, right_child);

        const auto left_type = left_child->type(schema);
        const auto right_type = right_child->type(schema);

        if (left_type != right_type)
        {
            if (right_type < left_type)
            {
                /// We try to optimize the special case, where we cast the value (right of the attribute)
                /// first. If that does not work, we need to cast the attribute (which may be cost intensive).
                if (left_child->result()->is_attribute() && right_child->result()->is_value())
                {
                    auto &value = right_child->result()->get<data::Value>();
                    if (value.is_lossless_convertible(left_type))
                    {
                        auto alias = right_child->result()->to_string();
                        auto casted_value_nullary = std::make_unique<expression::NullaryOperation>(
                            expression::Term{std::move(value.as(left_type)), std::move(alias)});
                        binary_operation->right_child(std::move(casted_value_nullary));

                        return;
                    }
                }
                else if (right_child->result()->is_attribute() && left_child->result()->is_value())
                {
                    auto &value = left_child->result()->get<data::Value>();
                    if (value.is_lossless_convertible(right_type))
                    {
                        auto alias = left_child->result()->to_string();
                        auto casted_value_nullary = std::make_unique<expression::NullaryOperation>(
                            expression::Term{std::move(value.as(right_type)), std::move(alias)});
                        binary_operation->left_child(std::move(casted_value_nullary));

                        return;
                    }
                }

                binary_operation->left_child(
                    std::make_unique<expression::CastOperation>(std::move(left_child), right_type));
            }
            else
            {
                if (right_child->result()->is_value())
                {
                    /// Values are casted directly.
                    auto alias = right_child->result()->to_string();
                    auto &value = right_child->result()->get<data::Value>();
                    auto casted_value_nullary = std::make_unique<expression::NullaryOperation>(
                        expression::Term{std::move(value.as(left_type)), std::move(alias)});
                    binary_operation->right_child(std::move(casted_value_nullary));
                }
                else
                {
                    binary_operation->right_child(
                        std::make_unique<expression::CastOperation>(std::move(right_child), left_type));
                }
            }
        }
    }
    else if (operation->is_case())
    {
        auto *case_operation = reinterpret_cast<expression::ListOperation *>(operation.get());

        for (auto &child : case_operation->children())
        {
            if (child->is_binary())
            {
                auto *case_child = reinterpret_cast<expression::BinaryOperation *>(child.get());
                ExplicitCastAdjustment::apply(schema, case_child->left_child());
                ExplicitCastAdjustment::apply(schema, case_child->right_child());
            }
            else
            {
                ExplicitCastAdjustment::apply(schema, child);
            }
        }
    }
}