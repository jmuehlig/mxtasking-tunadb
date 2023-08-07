#include "remove_fixed_value_cast_rule.h"
#include <db/plan/logical/node/aggregation_node.h>
#include <db/plan/logical/node/arithmetic_node.h>
#include <db/plan/logical/node/selection_node.h>

using namespace db::plan::optimizer;

bool RemoveFixedValueCastRule::apply(PlanView &plan)
{
    auto is_optimized = false;

    for (auto *node : plan.extract_nodes())
    {
        if (typeid(*node) == typeid(logical::SelectionNode))
        {
            auto *selection = reinterpret_cast<logical::SelectionNode *>(node);
            if (RemoveFixedValueCastRule::has_fixed_value_cast(selection->predicate()))
            {
                RemoveFixedValueCastRule::apply(selection->predicate());
                is_optimized |= true;
            }
        }
        else if (typeid(*node) == typeid(logical::AggregationNode))
        {
            auto *aggregation = reinterpret_cast<logical::AggregationNode *>(node);
            for (auto &aggregation_operation : aggregation->aggregation_operations())
            {
                if (RemoveFixedValueCastRule::has_fixed_value_cast(aggregation_operation))
                {
                    RemoveFixedValueCastRule::apply(aggregation_operation);
                    is_optimized |= true;
                }
            }
        }
        else if (typeid(*node) == typeid(logical::ArithmeticNode))
        {
            auto *arithmetic = reinterpret_cast<logical::ArithmeticNode *>(node);
            for (auto &arithmetic_operation : arithmetic->arithmetic_operations())
            {
                if (RemoveFixedValueCastRule::has_fixed_value_cast(arithmetic_operation))
                {
                    RemoveFixedValueCastRule::apply(arithmetic_operation);
                    is_optimized |= true;
                }
            }
        }
    }

    return is_optimized;
}

bool RemoveFixedValueCastRule::has_fixed_value_cast(const std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->is_cast())
    {
        const auto &child = reinterpret_cast<expression::CastOperation *>(predicate.get())->child();
        if (child->is_nullary())
        {
            if (child->result().has_value() && child->result()->is_value())
            {
                return true;
            }
        }
    }

    if (predicate->is_unary())
    {
        return RemoveFixedValueCastRule::has_fixed_value_cast(
            reinterpret_cast<expression::UnaryOperation *>(predicate.get())->child());
    }

    if (predicate->is_binary())
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        return RemoveFixedValueCastRule::has_fixed_value_cast(binary_operation->left_child()) ||
               RemoveFixedValueCastRule::has_fixed_value_cast(binary_operation->right_child());
    }

    return false;
}

void RemoveFixedValueCastRule::apply(std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->is_cast())
    {
        auto *cast_operation = reinterpret_cast<expression::CastOperation *>(predicate.get());
        const auto &child = cast_operation->child();
        if (child->is_nullary())
        {
            if (child->result().has_value() && child->result()->is_value())
            {
                predicate = std::make_unique<expression::NullaryOperation>(std::move(cast_operation->result().value()));
                return;
            }
        }
    }

    if (predicate->is_unary())
    {
        RemoveFixedValueCastRule::apply(reinterpret_cast<expression::UnaryOperation *>(predicate.get())->child());
    }

    if (predicate->is_binary())
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        RemoveFixedValueCastRule::apply(binary_operation->left_child());
        RemoveFixedValueCastRule::apply(binary_operation->right_child());
    }
}