#include "evaluate_predicate_rule.h"
#include <db/expression/operation_builder.h>
#include <db/plan/logical/node/selection_node.h>

using namespace db::plan::optimizer;

bool EvaluatePredicateRule::apply(PlanView &plan)
{
    auto is_optimized = false;

    for (auto *node : plan.extract_nodes())
    {
        if (typeid(*node) == typeid(logical::SelectionNode))
        {
            auto *selection = reinterpret_cast<logical::SelectionNode *>(node);
            if (EvaluatePredicateRule::is_evaluable(selection->predicate()))
            {
                selection->predicate() = EvaluatePredicateRule::evaluate(std::move(selection->predicate()));
                is_optimized = true;
            }
        }
    }

    return is_optimized;
}

bool EvaluatePredicateRule::is_evaluable(std::unique_ptr<expression::Operation> &predicate) noexcept
{
    if (predicate->is_binary())
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        if (predicate->is_arithmetic())
        {
            return binary_operation->left_child()->is_nullary() &&
                   binary_operation->left_child()->result()->is_value() &&
                   binary_operation->right_child()->is_nullary() &&
                   binary_operation->right_child()->result()->is_value();
        }

        return EvaluatePredicateRule::is_evaluable(binary_operation->left_child()) ||
               EvaluatePredicateRule::is_evaluable(binary_operation->right_child());
    }

    if (predicate->is_unary())
    {
        auto *unary_operation = reinterpret_cast<expression::UnaryOperation *>(predicate.get());
        return EvaluatePredicateRule::is_evaluable(unary_operation->child());
    }

    return false;
}

std::unique_ptr<db::expression::Operation> EvaluatePredicateRule::evaluate(
    std::unique_ptr<expression::Operation> &&predicate)
{
    if (predicate->is_binary())
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());

        if (predicate->is_arithmetic())
        {
            if (binary_operation->left_child()->is_nullary() && binary_operation->left_child()->result()->is_value() &&
                binary_operation->right_child()->is_nullary() && binary_operation->right_child()->result()->is_value())
            {
                auto &left_value = binary_operation->left_child()->result()->get<data::Value>();
                auto &right_value = binary_operation->right_child()->result()->get<data::Value>().as(left_value.type());

                if (binary_operation->id() == expression::Operation::Id::Add)
                {
                    left_value = left_value + right_value;
                }
                else if (binary_operation->id() == expression::Operation::Id::Sub)
                {
                    left_value = left_value - right_value;
                }
                else if (binary_operation->id() == expression::Operation::Id::Multiply)
                {
                    left_value = left_value * right_value;
                }
                else if (binary_operation->id() == expression::Operation::Id::Divide)
                {
                    left_value = left_value / right_value;
                }

                return std::make_unique<expression::NullaryOperation>(expression::Term{std::move(left_value)});
            }
        }

        auto left_child = EvaluatePredicateRule::evaluate(std::move(binary_operation->left_child()));
        binary_operation->left_child(std::move(left_child));

        auto right_child = EvaluatePredicateRule::evaluate(std::move(binary_operation->right_child()));
        binary_operation->right_child(std::move(right_child));
    }

    else if (predicate->is_unary())
    {
        auto *unary_operation = reinterpret_cast<expression::UnaryOperation *>(predicate.get());
        auto child = EvaluatePredicateRule::evaluate(std::move(unary_operation->child()));
        unary_operation->child(std::move(child));

        if (predicate->is_cast())
        {
            if (unary_operation->child()->is_nullary() && unary_operation->child()->result()->is_value())
            {
                const auto cast_type = reinterpret_cast<expression::CastOperation *>(unary_operation)->type();
                auto casted_value = unary_operation->child()->result()->get<data::Value>().as(cast_type);
                return expression::OperationBuilder::make_value(std::move(casted_value));
            }

            if (unary_operation->child()->id() == expression::Operation::Id::BetweenOperands)
            {
                auto *between_operands_operation =
                    reinterpret_cast<expression::BinaryOperation *>(unary_operation->child().get());

                auto &left_operand = between_operands_operation->left_child();
                auto &right_operand = between_operands_operation->right_child();

                if (left_operand->is_nullary() && left_operand->result()->is_value() && right_operand->is_nullary() &&
                    right_operand->result()->is_value())
                {
                    const auto cast_type = reinterpret_cast<expression::CastOperation *>(unary_operation)->type();

                    auto left_casted_value = left_operand->result()->get<data::Value>().as(cast_type);
                    auto new_left_value = expression::OperationBuilder::make_value(std::move(left_casted_value));

                    auto right_casted_value = right_operand->result()->get<data::Value>().as(cast_type);
                    auto new_right_value = expression::OperationBuilder::make_value(std::move(right_casted_value));

                    return std::make_unique<expression::BinaryOperation>(expression::Operation::Id::BetweenOperands,
                                                                         std::move(new_left_value),
                                                                         std::move(new_right_value));
                }
            }
        }
    }

    return std::move(predicate);
}