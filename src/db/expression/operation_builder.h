#pragma once

#include "operation.h"
#include <db/exception/plan_exception.h>
#include <db/parser/node_interface.h>

namespace db::expression {
class OperationBuilder
{
public:
    [[nodiscard]] static std::unique_ptr<Operation> make_value(data::Value &&value)
    {
        return std::make_unique<NullaryOperation>(Term{std::move(value)});
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_attribute(std::string &&name)
    {
        return std::make_unique<NullaryOperation>(Term::make_attribute(std::move(name)));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_attribute(std::string &&source, std::string &&name)
    {
        return std::make_unique<NullaryOperation>(Term::make_attribute(std::move(source), std::move(name)));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_and(std::unique_ptr<Operation> &&left,
                                                             std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::And, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_or(std::unique_ptr<Operation> &&left,
                                                            std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::Or, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_eq(std::unique_ptr<Operation> &&left,
                                                            std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::Equals, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_neq(std::unique_ptr<Operation> &&left,
                                                             std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::NotEquals, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_lt(std::unique_ptr<Operation> &&left,
                                                            std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::Lesser, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_leq(std::unique_ptr<Operation> &&left,
                                                             std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::LesserEquals, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_gt(std::unique_ptr<Operation> &&left,
                                                            std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::Greater, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_geq(std::unique_ptr<Operation> &&left,
                                                             std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::GreaterEquals, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_like(std::unique_ptr<Operation> &&operation,
                                                              std::string &&expression)
    {
        if (expression.empty())
        {
            throw exception::PlanningException{"Can not accept empty string in LIKE."};
        }

        auto id = Operation::Id::Like;
        if (expression.starts_with('%') && expression.ends_with('%'))
        {
            expression = expression.substr(1U, expression.size() - 2U);
            id = Operation::Id::Contains;
        }
        else if (expression.starts_with('%'))
        {
            expression.erase(0U, 1U);
            id = Operation::Id::EndsWith;
        }
        else if (expression.ends_with('%'))
        {
            expression.pop_back();
            id = Operation::Id::StartsWith;
        }

        return std::make_unique<BinaryOperation>(
            id, std::move(operation),
            make_value(data::Value{type::Type::make_char(expression.size()), std::move(expression)}));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_between(std::unique_ptr<Operation> &&first,
                                                                 std::unique_ptr<Operation> &&second,
                                                                 std::unique_ptr<Operation> &&third)
    {
        auto operands =
            std::make_unique<BinaryOperation>(Operation::Id::BetweenOperands, std::move(second), std::move(third));

        return std::make_unique<BinaryOperation>(Operation::Id::Between, std::move(first), std::move(operands));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_between(std::unique_ptr<Operation> &&attribute,
                                                                 data::Value &&min, data::Value &&max)
    {
        return make_between(std::move(attribute), make_value(std::move(min)), make_value(std::move(max)));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_in(std::unique_ptr<Operation> &&left,
                                                            std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::In, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_exists(std::unique_ptr<parser::NodeInterface> &&sub_query)
    {
        return std::make_unique<ExistsOperation>(std::move(sub_query));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_add(std::unique_ptr<Operation> &&left,
                                                             std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::Add, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_sub(std::unique_ptr<Operation> &&left,
                                                             std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::Sub, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_multiply(std::unique_ptr<Operation> &&left,
                                                                  std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::Multiply, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_divide(std::unique_ptr<Operation> &&left,
                                                                std::unique_ptr<Operation> &&right)
    {
        return std::make_unique<BinaryOperation>(Operation::Id::Divide, std::move(left), std::move(right));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_sum(std::unique_ptr<Operation> &&operation)
    {
        return std::make_unique<UnaryOperation>(Operation::Id::Sum, std::move(operation));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_count(std::unique_ptr<Operation> &&operation)
    {
        return std::make_unique<UnaryOperation>(Operation::Id::Count, std::move(operation));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_avg(std::unique_ptr<Operation> &&operation)
    {
        return std::make_unique<UnaryOperation>(Operation::Id::Average, std::move(operation));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_min(std::unique_ptr<Operation> &&operation)
    {
        return std::make_unique<UnaryOperation>(Operation::Id::Min, std::move(operation));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_max(std::unique_ptr<Operation> &&operation)
    {
        return std::make_unique<UnaryOperation>(Operation::Id::Max, std::move(operation));
    }

    [[nodiscard]] static std::unique_ptr<Operation> make_user_defined_function(
        std::string &&name, std::vector<std::unique_ptr<Operation>> &&parameter_list)
    {
        return std::make_unique<UserDefinedFunctionOperation>(std::move(name), std::move(parameter_list));
    }
};
} // namespace db::expression