#include "user_defined_operator.h"
#include <db/execution/compilation/scan_loop.h>

using namespace db::execution::compilation;

void UserDefinedOperator::produce(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    if (phase == GenerationPhase::finalization || phase == GenerationPhase::prefetching)
    {
        /// Since this operator is a pipeline breaker, tuples are consumed from here.
        this->parent()->consume(phase, program, context);
        return;
    }

    {
        auto context_guard = flounder::ContextGuard{program, "Scan"};

        auto scan_loop =
            PaxScanLoop{program, context, std::string{"user_defined_operator"}, this->child()->schema(), true};

        for (const auto &operation : this->_user_defined_functions)
        {
            const auto &udf_descriptor = operation->descriptor()->get();
            auto udf_context_guard = flounder::ContextGuard{program, fmt::format("UDF '{}'", udf_descriptor.name())};

            /// Get all vregs of the UDF parameters.
            auto parameters = std::vector<flounder::Operand>{};
            parameters.reserve(operation->size());
            for (const auto &child : operation->children())
            {
                parameters.emplace_back(context.symbols().get(child->result().value()));
            }

            /// Call the UDF.
            const auto &result_term = operation->result().value();
            auto user_defined_function_call =
                flounder::FunctionCall{program, udf_descriptor.callable(), result_term.to_string()};
            auto result_vreg = user_defined_function_call.call(std::move(parameters));

            /// Release all parameter terms.
            for (const auto &child : operation->children())
            {
                context.symbols().release(program, child->result().value());
            }

            /// Set the result for the parent operator.
            if (result_vreg.has_value())
            {
                context.symbols().set(result_term, result_vreg.value());
            }
        }

        this->parent()->consume(phase, program, context);
    }
}

void UserDefinedOperator::request_symbols(const db::execution::compilation::OperatorInterface::GenerationPhase phase,
                                          db::execution::compilation::SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        for (const auto &operation : this->_user_defined_functions)
        {
            for (const auto &child : operation->children())
            {
                symbols.request(child->result().value());
            }
        }
    }
}
