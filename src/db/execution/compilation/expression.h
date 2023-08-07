#pragma once
#include "expression_set.h"
#include <db/expression/operation.h>
#include <db/topology/physical_schema.h>
#include <flounder/program.h>
#include <memory>

namespace db::execution::compilation {
class Expression
{
public:
    static void emit(flounder::Program &program, const topology::PhysicalSchema &schema, ExpressionSet &expression_set,
                     const std::unique_ptr<expression::Operation> &operation);
    static void emit(flounder::Program &program, const topology::PhysicalSchema &schema, ExpressionSet &expression_set,
                     const std::unique_ptr<expression::Operation> &predicate, flounder::Label target_if_false);

private:
    static void emit_cast(flounder::Program &program, const topology::PhysicalSchema &schema,
                          ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &operation);
    static void emit_cast(flounder::Program &program, type::Type from_type, flounder::Register from_vreg,
                          type::Type to_type, flounder::Register to_vreg);
    static void emit_arithmetic(flounder::Program &program, const topology::PhysicalSchema &schema,
                                ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &operation);
    static void emit_case(flounder::Program &program, const topology::PhysicalSchema &schema,
                          ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &predicate);
    static void emit_logical_connective(flounder::Program &program, const topology::PhysicalSchema &schema,
                                        ExpressionSet &expression_set,
                                        const std::unique_ptr<expression::Operation> &predicate,
                                        flounder::Label target_if_false);
    static void emit_comparison(flounder::Program &program, const topology::PhysicalSchema &schema,
                                ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &predicate,
                                flounder::Label target_if_false);
    static void emit_in_comparison(flounder::Program &program, const topology::PhysicalSchema &schema,
                                   ExpressionSet &expression_set,
                                   const std::unique_ptr<expression::Operation> &predicate,
                                   flounder::Label target_if_false);
    static void emit_between_comparison(flounder::Program &program, const topology::PhysicalSchema &schema,
                                        ExpressionSet &expression_set,
                                        const std::unique_ptr<expression::Operation> &predicate,
                                        flounder::Label target_if_false);
    [[nodiscard]] static flounder::Register emit_starts_with(flounder::Program &program,
                                                             const topology::PhysicalSchema &schema,
                                                             ExpressionSet &expression_set,
                                                             const std::unique_ptr<expression::Operation> &predicate);
    static void emit_starts_with_arithmetic(flounder::Program &program, const topology::PhysicalSchema &schema,
                                            ExpressionSet &expression_set,
                                            const std::unique_ptr<expression::Operation> &predicate,
                                            flounder::Register target_register);
    static void emit_starts_with_comparison(flounder::Program &program, const topology::PhysicalSchema &schema,
                                            ExpressionSet &expression_set,
                                            const std::unique_ptr<expression::Operation> &predicate,
                                            flounder::Label target_if_false);
    static void emit_string_comparison(flounder::Program &program,
                                       const std::unique_ptr<expression::Operation> &operation,
                                       flounder::Label target_if_false, flounder::Operand left_expression,
                                       type::Type left_type, bool is_left_pointer, flounder::Operand right_expression,
                                       type::Type right_type, bool is_right_pointer);
    static void emit_is_true(flounder::Program &program, const topology::PhysicalSchema &schema,
                             ExpressionSet &expression_set, const std::unique_ptr<expression::Operation> &predicate,
                             flounder::Label target_if_false);
    [[nodiscard]] static flounder::Constant constant(flounder::Program &program, const data::Value &value);

    [[nodiscard]] static bool is_likely(const float selectivity) noexcept { return selectivity > .2; }
};
} // namespace db::execution::compilation