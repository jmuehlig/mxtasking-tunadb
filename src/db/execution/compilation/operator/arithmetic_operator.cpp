#include "arithmetic_operator.h"
#include <db/exception/execution_exception.h>
#include <db/execution/compilation/expression.h>

using namespace db::execution::compilation;

void ArithmeticOperator::produce(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    this->child()->produce(phase, program, context);
}

void ArithmeticOperator::consume(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    if (phase == GenerationPhase::prefetching)
    {
        this->parent()->consume(phase, program, context);
        return;
    }

    auto context_guard = flounder::ContextGuard{program, "Arithmetic"};

    const auto is_emit_in_phase = this->is_emit_arithmetic(phase);

    if (is_emit_in_phase)
    {
        /// Request all expressions to don't discard computed sub expressions
        /// too early.
        for (const auto &arithmetic : this->_arithmetics)
        {
            if (context.symbols().is_requested(arithmetic->result().value()))
            {
                context.expressions().request(arithmetic);
            }
        }

        /// Emit code for doing arithmetic.
        std::sort(this->_arithmetics.begin(), this->_arithmetics.end(), ArithmeticComparator{});
        for (const auto &arithmetic : this->_arithmetics)
        {
            if (context.symbols().is_requested(arithmetic->result().value()))
            {
                Expression::emit(program, this->_schema, context.expressions(), arithmetic);
            }
        }
    }

    this->parent()->consume(phase, program, context);

    if (is_emit_in_phase)
    {
        /// Release symbols.
        context.symbols().release(program, this->_arithmetics);
    }
}

void ArithmeticOperator::request_symbols(GenerationPhase phase, SymbolSet &symbols)
{
    if (phase != GenerationPhase::prefetching && this->is_emit_arithmetic(phase))
    {
        symbols.request(this->_arithmetics);
    }
    this->child()->request_symbols(phase, symbols);
}
