#include "expression.h"
#include "materializer.h"
#include <db/exception/execution_exception.h>
#include <flounder/comparator.h>
#include <flounder/statement.h>
#include <flounder/string.h>
#include <fmt/core.h>
#include <type_traits>

using namespace db::execution::compilation;

void Expression::emit(flounder::Program &program, const topology::PhysicalSchema &schema, ExpressionSet &expression_set,
                      const std::unique_ptr<expression::Operation> &operation)
{
    /// Cancel re-computation of an already computed expression.
    if (expression_set.is_set(operation))
    {
        //        expression_set.touch(operation);
        return;
    }

    if (operation->is_nullary() && operation->result()->is_value())
    {
        auto constant = Expression::constant(program, operation->result()->get<data::Value>());
        expression_set.set(program, operation, flounder::Operand{constant});
    }
    else if (operation->is_cast())
    {
        Expression::emit_cast(program, schema, expression_set, operation);
    }
    else if (operation->is_case())
    {
        Expression::emit_case(program, schema, expression_set, operation);
    }
    else if (operation->is_arithmetic() || operation->is_comparison())
    {
        Expression::emit_arithmetic(program, schema, expression_set, operation);
    }
}

void Expression::emit(flounder::Program &program, const topology::PhysicalSchema &schema, ExpressionSet &expression_set,
                      const std::unique_ptr<expression::Operation> &predicate, flounder::Label target_if_false)
{
    if (predicate->is_logical_connective())
    {
        Expression::emit_logical_connective(program, schema, expression_set, predicate, target_if_false);
    }
    else if (predicate->is_comparison())
    {
        Expression::emit_comparison(program, schema, expression_set, predicate, target_if_false);
    }
    else if (predicate->id() == expression::Operation::Id::IsTrue)
    {
        Expression::emit_is_true(program, schema, expression_set, predicate, target_if_false);
    }
    else
    {
        Expression::emit(program, schema, expression_set, predicate);
    }
}

void Expression::emit_cast(flounder::Program &program, const topology::PhysicalSchema &schema,
                           ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &operation)
{
    auto *cast = reinterpret_cast<expression::CastOperation *>(operation.get());

    /// Emit the to-be-casted expression first (i.e., CAST(foo, double) -> emit foo first).
    const auto &child = cast->child();
    Expression::emit(program, schema, expression_set, child);
    auto emitted_child = expression_set.get(child);

    if (emitted_child.is_constant())
    {
        auto constant = emitted_child.constant();
        emitted_child = program.vreg(SymbolSet::make_vreg_name(child->result().value()));
        program << program.request_vreg(emitted_child.reg(), constant.width()) << program.mov(emitted_child, constant);
    }

    const auto &result_term = cast->result().value();
    const auto result_type = operation->type(schema);

    auto cast_vreg = program.vreg(SymbolSet::make_vreg_name(result_term));
    program << program.request_vreg(cast_vreg, result_type.register_width());

    Expression::emit_cast(program, child->type(schema), emitted_child.reg(), result_type, cast_vreg);

    expression_set.set(program, operation, flounder::Operand{cast_vreg});
}

void Expression::emit_cast(flounder::Program &program, const type::Type from_type, flounder::Register from_vreg,
                           const type::Type to_type, flounder::Register to_vreg)
{
    if (from_type != to_type)
    {
        if (from_type == type::Id::INT)
        {
            if (to_type == type::Id::DATE || to_type == type::Id::CHAR)
            {
                throw exception::CastException(from_type.to_string(), to_type.to_string());
            }

            if (to_type == type::Id::DECIMAL)
            {
                program << program.mov(to_vreg, from_vreg)
                        << program.imul(to_vreg,
                                        program.constant64(std::pow(10U, to_type.decimal_description().scale())));
            }
            else if (to_type == type::Id::BOOL)
            {
                program << program.cmp(from_vreg, program.constant8(0)) << program.setne(to_vreg)
                        << program.and_(to_vreg, program.constant8(1));
            }
            else if (to_type == type::Id::BIGINT)
            {
                program << program.mov(to_vreg, from_vreg);
            }
        }
        else if (from_type == type::Id::BIGINT)
        {
            if (to_type == type::Id::DATE || to_type == type::Id::CHAR)
            {
                throw exception::CastException(from_type.to_string(), to_type.to_string());
            }

            if (to_type == type::Id::DECIMAL)
            {
                program << program.mov(to_vreg, from_vreg)
                        << program.imul(to_vreg,
                                        program.constant64(std::pow(10U, to_type.decimal_description().scale())));
            }
            else if (to_type == type::Id::BOOL)
            {
                program << program.cmp(from_vreg, program.constant8(0)) << program.setne(to_vreg)
                        << program.and_(to_vreg, program.constant8(1));
            }
            else if (to_type == type::Id::INT)
            {
                program << program.mov(to_vreg, from_vreg);
            }
        }
        else if (from_type == type::Id::DECIMAL)
        {
            if (to_type == type::Id::DATE || to_type == type::Id::CHAR)
            {
                throw exception::CastException(from_type.to_string(), to_type.to_string());
            }

            if (to_type == type::Id::DECIMAL)
            {
                if (to_type.decimal_description().scale() > from_type.decimal_description().scale())
                {
                    const auto factor = std::int64_t(
                        std::pow(10U, to_type.decimal_description().scale() - from_type.decimal_description().scale()));
                    program << program.mov(to_vreg, from_vreg) << program.imul(to_vreg, program.constant64(factor));
                }
                else
                {
                    const auto divisor = std::int64_t(
                        std::pow(10U, from_type.decimal_description().scale() - to_type.decimal_description().scale()));
                    program << program.mov(to_vreg, from_vreg) << program.fdiv(to_vreg, program.constant64(divisor));
                }
            }
            else if (to_type == type::Id::BOOL)
            {
                program << program.cmp(from_vreg, program.constant8(0)) << program.setne(to_vreg)
                        << program.and_(to_vreg, program.constant8(1));
            }
            else if (to_type == type::Id::BIGINT)
            {
                program << program.mov(to_vreg, from_vreg)
                        << program.fdiv(to_vreg,
                                        program.constant64(std::pow(10U, from_type.decimal_description().scale())));
            }
            else if (to_type == type::Id::INT)
            {
                auto help_cast_vreg = program.vreg("decimal_int_cast");
                program << program.request_vreg64(help_cast_vreg) << program.mov(help_cast_vreg, from_vreg)
                        << program.fdiv(help_cast_vreg,
                                        program.constant64(std::pow(10U, from_type.decimal_description().scale())))
                        << program.mov(to_vreg, help_cast_vreg) << program.clear(help_cast_vreg);
            }
        }
        else if (from_type == type::Id::DATE)
        {
            throw exception::CastException(from_type.to_string(), to_type.to_string());
        }
        else if (from_type == type::Id::CHAR)
        {
            throw exception::CastException(from_type.to_string(), to_type.to_string());
        }
        else if (from_type == type::Id::BOOL)
        {
            if (to_type == type::Id::DATE || to_type == type::Id::CHAR)
            {
                throw exception::CastException(from_type.to_string(), to_type.to_string());
            }

            if (to_type == type::Id::INT || to_type == type::Id::BIGINT)
            {
                program << program.mov(to_vreg, from_vreg);
            }
            else if (to_type == type::Id::DECIMAL)
            {
                program << program.mov(to_vreg, from_vreg)
                        << program.imul(to_vreg,
                                        program.constant64(std::pow(10U, to_type.decimal_description().scale())));
            }
        }
    }
    else
    {
        program << program.mov(to_vreg, from_vreg);
    }
}

void Expression::emit_arithmetic(flounder::Program &program, const topology::PhysicalSchema &schema,
                                 ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &operation)
{
    auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());

    const auto &result_term = operation->result().value();
    const auto result_type = operation->type(schema);

    /// Emit the left child.
    const auto &left_child = binary_operation->left_child();
    const auto &left_child_term = left_child->result().value();
    Expression::emit(program, schema, expression_set, left_child);

    /// Emit the right child.
    const auto &right_child = binary_operation->right_child();
    const auto &right_child_term = right_child->result().value();
    Expression::emit(program, schema, expression_set, right_child);

    /// Calculate the result.
    auto arithmetic_vreg = program.vreg(SymbolSet::make_vreg_name(result_term));
    program << program.request_vreg(arithmetic_vreg, result_type.register_width());

    if (operation->is_comparison())
    {
        if (operation->id() == expression::Operation::Id::StartsWith)
        {
            Expression::emit_starts_with_arithmetic(program, schema, expression_set, operation, arithmetic_vreg);
        }
    }
    else
    {
        program << program.mov(arithmetic_vreg, expression_set.get(left_child));

        /// Emit the arithmetic.
        auto right_operand = expression_set.get(right_child);
        if (operation->id() == expression::Operation::Id::Add)
        {
            program << program.add(flounder::Operand{arithmetic_vreg}, right_operand);
        }
        else if (operation->id() == expression::Operation::Id::Sub)
        {
            program << program.sub(flounder::Operand{arithmetic_vreg}, right_operand);
        }
        else if (operation->id() == expression::Operation::Id::Multiply)
        {
            program << program.imul(flounder::Operand{arithmetic_vreg}, right_operand);
        }
        else if (operation->id() == expression::Operation::Id::Divide)
        {
            program << program.fdiv(flounder::Operand{arithmetic_vreg}, right_operand);
        }
    }

    expression_set.set(program, operation, flounder::Operand{arithmetic_vreg});
}

void Expression::emit_case(flounder::Program &program, const topology::PhysicalSchema &schema,
                           ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &predicate)
{
    auto *case_operation = reinterpret_cast<expression::ListOperation *>(predicate.get());

    const auto &predicate_term = predicate->result().value();
    const auto predicate_type = predicate->type(schema);
    auto case_name = SymbolSet::make_vreg_name(predicate_term);
    auto case_result_vreg = program.vreg(std::string{case_name});

    const auto has_else = case_operation->children().empty() == false &&
                          case_operation->children().back()->id() == expression::Operation::Id::Else;

    /// Create labels for each operation and one for the end.
    auto case_labels = std::vector<flounder::Label>();
    case_labels.reserve(case_operation->children().size() + 2U);
    for (auto child_id = 0U; child_id < case_operation->children().size() - static_cast<std::uint8_t>(has_else);
         ++child_id)
    {
        const auto &child = case_operation->children()[child_id];
        case_labels.emplace_back(program.label(fmt::format("{}_case_{}", case_name, child_id)));
    }

    if (has_else)
    {
        case_labels.emplace_back(program.label(fmt::format("{}_else", case_name)));
    }
    case_labels.emplace_back(program.label(fmt::format("{}_end", std::move(case_name))));

    program << program.request_vreg(case_result_vreg, predicate_type.register_width());

    /// Emit
    ///     WHEN [expression]
    ///     THEN
    ///         [expression];
    ///         mov case_result, expression;
    ///         jmp case_end;
    for (auto child_id = 0U; child_id < case_operation->children().size() - static_cast<std::uint8_t>(has_else);
         ++child_id)
    {
        const auto &child = case_operation->children()[child_id];
        if (child->id() == expression::Operation::Id::WhenThen)
        {
            auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(child.get());
            program << program.section(case_labels[child_id]);

            /// Emit the WHEN
            Expression::emit(program, schema, expression_set, binary_operation->left_child(),
                             case_labels[child_id + 1U]);

            /// Emit the THEN
            Expression::emit(program, schema, expression_set, binary_operation->right_child());
            program << program.mov(case_result_vreg, expression_set.get(binary_operation->right_child()));

            /// If WHEN is true, jump to the end.
            program << program.jmp(case_labels.back());
        }
    }

    /// Emit ELSE
    ///     [expression]
    ///     mov case_result, expression
    if (has_else)
    {
        const auto &child = case_operation->children().back();
        auto *unary_operation = reinterpret_cast<expression::UnaryOperation *>(child.get());
        program << program.section(case_labels[case_labels.size() - 2U]);

        Expression::emit(program, schema, expression_set, unary_operation->child());
        program << program.mov(case_result_vreg, expression_set.get(unary_operation->child()));
    }
    program << program.section(case_labels.back());

    expression_set.set(program, predicate, flounder::Operand{case_result_vreg});
}

void Expression::emit_logical_connective(flounder::Program &program, const topology::PhysicalSchema &schema,
                                         ExpressionSet &expression_set,
                                         const std::unique_ptr<expression::Operation> &predicate,
                                         flounder::Label target_if_false)
{
    auto *logical_connective_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
    if (predicate->id() == expression::Operation::Id::And)
    {
        Expression::emit(program, schema, expression_set, logical_connective_operation->left_child(), target_if_false);
        Expression::emit(program, schema, expression_set, logical_connective_operation->right_child(), target_if_false);
    }

    else if (predicate->id() == expression::Operation::Id::Or)
    {
        auto operation_name = predicate->result().value().to_string();
        auto label_second_operation = program.label(fmt::format("{}_test_or", operation_name));
        auto label_true = program.label(fmt::format("{}_true", operation_name));

        /// Emit the first operand.
        Expression::emit(program, schema, expression_set, logical_connective_operation->left_child(),
                         label_second_operation);

        /// If it is true, jump directly to the end of the or.
        program << program.jmp(label_true) << program.section(label_second_operation);

        /// Check the second operand.
        Expression::emit(program, schema, expression_set, logical_connective_operation->right_child(), target_if_false);
        program << program.section(label_true);
    }
}

void Expression::emit_comparison(flounder::Program &program, const topology::PhysicalSchema &schema,
                                 ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &predicate,
                                 flounder::Label target_if_false)
{
    if (predicate->id() == expression::Operation::Id::In)
    {
        Expression::emit_in_comparison(program, schema, expression_set, predicate, target_if_false);
    }
    else if (predicate->id() == expression::Operation::Id::StartsWith)
    {
        Expression::emit_starts_with_comparison(program, schema, expression_set, predicate, target_if_false);
    }
    else if (predicate->id() == expression::Operation::Id::Between)
    {
        Expression::emit_between_comparison(program, schema, expression_set, predicate, target_if_false);
    }
    else
    {
        auto *comparison_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());

        /// Emit the left side of the comparison.
        const auto left_type = comparison_operation->left_child()->type(schema);
        const auto is_left_pointer = RowMaterializer::is_materialize_with_pointer(left_type);
        Expression::emit(program, schema, expression_set, comparison_operation->left_child(), target_if_false);
        auto left_expression = expression_set.get(comparison_operation->left_child());

        /// Emit the right side of the comparison.
        const auto right_type = comparison_operation->right_child()->type(schema);
        const auto is_right_pointer = RowMaterializer::is_materialize_with_pointer(right_type);
        Expression::emit(program, schema, expression_set, comparison_operation->right_child(), target_if_false);
        auto right_expression = expression_set.get(comparison_operation->right_child());

        if (is_left_pointer == false && is_right_pointer == false)
        {
            const auto is_likely = Expression::is_likely(predicate->annotation().selectivity().value_or(1));
            switch (predicate->id())
            {
            case expression::Operation::Id::Equals:
                flounder::IsNotEquals{left_expression, right_expression, is_likely}.emit(program, target_if_false);
                break;
            case expression::Operation::Id::NotEquals:
                flounder::IsEquals{left_expression, right_expression, is_likely}.emit(program, target_if_false);
                break;
            case expression::Operation::Id::Lesser:
                flounder::IsGreaterEquals{left_expression, right_expression, is_likely}.emit(program, target_if_false);
                break;
            case expression::Operation::Id::LesserEquals:
                flounder::IsGreater{left_expression, right_expression, is_likely}.emit(program, target_if_false);
                break;
            case expression::Operation::Id::Greater:
                flounder::IsLowerEquals{left_expression, right_expression, is_likely}.emit(program, target_if_false);
                break;
            case expression::Operation::Id::GreaterEquals:
                flounder::IsLower{left_expression, right_expression, is_likely}.emit(program, target_if_false);
                break;
            default:
                throw exception::NotImplementedException{
                    fmt::format("Comparison of type {}", std::uint16_t(comparison_operation->id()))};
            }
        }
        else if (left_type == type::Id::CHAR && right_type == type::Id::CHAR)
        {
            Expression::emit_string_comparison(program, predicate, target_if_false, left_expression, left_type,
                                               is_left_pointer, right_expression, right_type, is_right_pointer);
        }
        else
        {
            throw exception::NotImplementedException{
                fmt::format("Comparison of type {}", std::uint16_t(comparison_operation->id()))};
        }

        expression_set.release(program, predicate);
    }
}

void Expression::emit_in_comparison(flounder::Program &program, const topology::PhysicalSchema &schema,
                                    ExpressionSet &expression_set,
                                    const std::unique_ptr<expression::Operation> &predicate,
                                    flounder::Label target_if_false)
{
    auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
    auto end_label = program.label(fmt::format("{}_end", predicate->result()->to_string()));

    /// Emit the operand.
    const auto expression_type = binary_operation->left_child()->type(schema);
    const auto is_expression_pointer = RowMaterializer::is_materialize_with_pointer(expression_type);
    Expression::emit(program, schema, expression_set, binary_operation->left_child());
    auto expression = expression_set.get(binary_operation->left_child());

    auto *values = reinterpret_cast<expression::NullaryListOperation *>(binary_operation->right_child().get());

    auto in_id = 0U;
    for (const auto &term : values->terms())
    {
        if (term.is_value())
        {
            const auto &value = term.get<data::Value>();
            auto constant = Expression::constant(program, value);

            const auto is_value_pointer = RowMaterializer::is_materialize_with_pointer(value.type());
            if (is_expression_pointer || is_value_pointer)
            {
                auto result = flounder::String::is_equals(
                    program, fmt::format("{}_{}_in", in_id++, term.to_string()),
                    flounder::String::Descriptor{expression, expression_type.char_description().length(), false,
                                                 is_expression_pointer},
                    flounder::String::Descriptor{flounder::Operand{constant}, value.type().char_description().length(),
                                                 true, is_value_pointer});

                program << program.cmp(result, program.constant8(1)) << program.clear(result) << program.je(end_label);
            }
            else
            {
                program << program.cmp(expression, flounder::Operand{constant}) << program.je(end_label);
            }
        }
    }
    program << program.jmp(target_if_false) << program.section(end_label);
}

void Expression::emit_between_comparison(flounder::Program &program, const topology::PhysicalSchema &schema,
                                         db::execution::compilation::ExpressionSet &expression_set,
                                         const std::unique_ptr<expression::Operation> &predicate,
                                         flounder::Label target_if_false)
{
    auto *between_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());

    /// Emit the left side of the comparison.
    Expression::emit(program, schema, expression_set, between_operation->left_child(), target_if_false);
    auto left_expression = expression_set.get(between_operation->left_child());

    auto *between_operands_operation =
        reinterpret_cast<expression::BinaryOperation *>(between_operation->right_child().get());
    Expression::emit(program, schema, expression_set, between_operands_operation->left_child(), target_if_false);
    auto left_operand = expression_set.get(between_operands_operation->left_child());
    Expression::emit(program, schema, expression_set, between_operands_operation->right_child(), target_if_false);
    auto right_operand = expression_set.get(between_operands_operation->right_child());

    if (left_expression.is_reg() && left_operand.is_constant() && right_operand.is_constant())
    {
        const auto min_value =
            std::min(left_operand.constant().value_as_int64(), right_operand.constant().value_as_int64());
        const auto max_value =
            std::max(left_operand.constant().value_as_int64(), right_operand.constant().value_as_int64());

        const auto is_likely = Expression::is_likely(predicate->annotation().selectivity().value_or(1));
        const auto term_request_count = expression_set.count_requests(between_operation->left_child());

        /// The term is requested only for the filter.
        const auto compared_value = flounder::Operand{program.constant64(max_value - min_value)};

        if (term_request_count == 1U)
        {
            program << program.sub(left_expression, flounder::Operand{program.constant64(min_value)})
                    << program.cmp(left_expression, compared_value, is_likely) << program.ja(target_if_false);
        }

        /// The term will be also used in the wider query.
        else
        {
            auto cmp_reg = program.vreg(fmt::format("{}_cmp", left_expression.reg().virtual_name().value()));
            program << program.request_vreg64(cmp_reg)
                    << program.lea(cmp_reg, program.mem(left_expression.reg(), min_value * -1))
                    << program.cmp(flounder::Operand{cmp_reg}, compared_value, is_likely) << program.ja(target_if_false)
                    << program.clear(cmp_reg);
        }
    }
    else
    {
        flounder::IsLower{left_expression, left_operand}.emit(program, target_if_false);
        flounder::IsGreater{left_expression, right_operand}.emit(program, target_if_false);
    }

    expression_set.release(program, predicate);
}

flounder::Register Expression::emit_starts_with(flounder::Program &program, const topology::PhysicalSchema &schema,
                                                ExpressionSet &expression_set,
                                                const std::unique_ptr<expression::Operation> &predicate)
{
    auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());

    /// Emit the left side (i.e., load the string).
    Expression::emit(program, schema, expression_set, binary_operation->left_child());
    const auto is_left_pointer =
        RowMaterializer::is_materialize_with_pointer(binary_operation->left_child()->type(schema));

    /// Emit the right side.
    auto like_value = reinterpret_cast<expression::NullaryOperation *>(binary_operation->right_child().get())
                          ->result()
                          ->get<data::Value>();
    auto right_expression = Expression::constant(program, like_value);
    const auto is_right_pointer = RowMaterializer::is_materialize_with_pointer(like_value.type());

    /// Max length of the comparison.
    const auto length = like_value.type().char_description().length();

    auto left_expression = expression_set.get(binary_operation->left_child());
    auto is_equals_vreg = flounder::String::is_equals(
        program, fmt::format("{}_starts_with", predicate->result()->to_string()),
        flounder::String::Descriptor{left_expression, length, false, is_left_pointer},
        flounder::String::Descriptor{flounder::Operand{right_expression}, length, true, is_right_pointer});
    expression_set.release(program, binary_operation->left_child());
    return is_equals_vreg;
}

void Expression::emit_starts_with_comparison(flounder::Program &program, const topology::PhysicalSchema &schema,
                                             ExpressionSet &expression_set,
                                             const std::unique_ptr<expression::Operation> &predicate,
                                             flounder::Label target_if_false)
{
    auto result = Expression::emit_starts_with(program, schema, expression_set, predicate);
    flounder::IsNotEquals{flounder::Operand{result}, flounder::Operand{program.constant8(1)}}.emit(program,
                                                                                                   target_if_false);
    program << program.clear(result);
    expression_set.release(program, predicate);
}

void Expression::emit_starts_with_arithmetic(flounder::Program &program, const topology::PhysicalSchema &schema,
                                             ExpressionSet &expression_set,
                                             const std::unique_ptr<expression::Operation> &predicate,
                                             flounder::Register target_register)
{
    auto result = Expression::emit_starts_with(program, schema, expression_set, predicate);
    program << program.xor_(target_register, target_register);
    {
        auto if_is_like = flounder::If{
            program, flounder::IsEquals{flounder::Operand{result}, flounder::Operand{program.constant8(1)}}};
        program << program.inc(target_register);
    }
    program << program.clear(result);
    expression_set.release(program, predicate);
}

void Expression::emit_string_comparison(flounder::Program &program,
                                        const std::unique_ptr<expression::Operation> &operation,
                                        flounder::Label target_if_false, flounder::Operand left_expression,
                                        type::Type left_type, bool is_left_pointer, flounder::Operand right_expression,
                                        type::Type right_type, bool is_right_pointer)
{
    if (operation->id() == expression::Operation::Id::Equals || operation->id() == expression::Operation::Id::NotEquals)
    {
        auto *binary_expression = reinterpret_cast<expression::BinaryOperation *>(operation.get());

        const auto is_left_constant =
            binary_expression->left_child()->is_nullary() && binary_expression->left_child()->result()->is_value();
        const auto is_right_constant =
            binary_expression->right_child()->is_nullary() && binary_expression->right_child()->result()->is_value();

        auto result = flounder::String::is_equals(
            program, fmt::format("{}_strcmp_result", operation->result()->to_string()),
            flounder::String::Descriptor{left_expression, left_type.char_description().length(), is_left_constant,
                                         is_left_pointer},
            flounder::String::Descriptor{right_expression, right_type.char_description().length(), is_right_constant,
                                         is_right_pointer});

        if (operation->id() == expression::Operation::Id::Equals)
        {
            flounder::IsNotEquals{flounder::Operand{result}, flounder::Operand{program.constant8(1)}}.emit(
                program, target_if_false);
        }
        else
        {
            flounder::IsEquals{flounder::Operand{result}, flounder::Operand{program.constant8(1)}}.emit(
                program, target_if_false);
        }
        program << program.clear(result);
    }
    else
    {
        throw exception::NotImplementedException{"Comparison (other than EQ,NEQ) of strings"};
    }
}

void Expression::emit_is_true(flounder::Program &program, const topology::PhysicalSchema &schema,
                              ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &predicate,
                              flounder::Label target_if_false)
{
    auto *is_true_expression = reinterpret_cast<expression::UnaryOperation *>(predicate.get());
    Expression::emit(program, schema, expression_set, is_true_expression->child());

    /// Check if an attribute is true.
    flounder::IsNotEquals{expression_set.get(is_true_expression->child()), flounder::Operand{program.constant8(1)}}
        .emit(program, target_if_false);
    expression_set.release(program, is_true_expression->child());
}

flounder::Constant Expression::constant(flounder::Program &program, const data::Value &value)
{
    switch (value.type().id())
    {
    case type::Id::INT:
        return program.constant32(value.get<type::Id::INT>());
    case type::Id::BIGINT:
        return program.constant64(value.get<type::Id::BIGINT>());
    case type::Id::DECIMAL:
        return program.constant64(value.get<type::Id::DECIMAL>());
    case type::Id::DATE:
        return program.constant32(value.get<type::Id::DATE>().data());
    case type::Id::BOOL:
        return program.constant8(static_cast<std::int8_t>(value.get<type::Id::BOOL>()));
    case type::Id::CHAR: {
        if (RowMaterializer::is_materialize_with_pointer(value.type()))
        {
            auto *target_data = program.data(value.type().char_description().length());
            std::visit(
                [target_data](const auto &str) {
                    using T = std::decay_t<decltype(str)>;
                    if constexpr (std::is_same<T, std::string>::value || std::is_same<T, std::string_view>::value)
                    {
                        std::memcpy(target_data, str.data(), str.length());
                    }
                },
                value.value());
            return program.address(std::uintptr_t(target_data));
        }

        const char *data = nullptr;
        std::visit(
            [&data](const auto &str) {
                using T = std::decay_t<decltype(str)>;
                if constexpr (std::is_same<T, std::string>::value || std::is_same<T, std::string_view>::value)
                {
                    data = str.data();
                }
            },
            value.value());
        if (data != nullptr)
        {
            const auto length = value.type().char_description().length();
            if (length == 1U)
            {
                return program.constant8(*reinterpret_cast<const std::int8_t *>(data));
            }

            if (length == 2U)
            {
                return program.constant16(*reinterpret_cast<const std::int16_t *>(data));
            }

            if (length == 4U)
            {
                return program.constant32(*reinterpret_cast<const std::int32_t *>(data));
            }
        }
    }
    default:
        throw exception::NotImplementedException{fmt::format("loading expression {}", value.type().to_string())};
    }
}
