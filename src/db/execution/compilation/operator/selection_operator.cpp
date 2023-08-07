#include "selection_operator.h"
#include <db/exception/execution_exception.h>
#include <db/execution/compilation/expression.h>
#include <db/execution/compilation/materializer.h>
#include <flounder/comparator.h>
#include <flounder/lib.h>

using namespace db::execution::compilation;

void SelectionOperator::produce(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    this->child()->produce(phase, program, context);
}

void SelectionOperator::consume(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    if (phase == GenerationPhase::finalization || phase == GenerationPhase::prefetching)
    {
        this->parent()->consume(phase, program, context);
        return;
    }

    auto context_guard = flounder::ContextGuard{program, "Selection"};

    Expression::emit(program, this->_schema, context.expressions(), this->_predicate, context.label_next_record());
    program << program.begin_branch(0);
    context.symbols().release(program, this->_required_terms);

    this->parent()->consume(phase, program, context);

    program << program.end_branch();
}

void SelectionOperator::request_symbols(const GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        this->_required_terms = std::vector<expression::Term>{};
        expression::for_each_term(this->_predicate,
                                  [&symbols, &terms = this->_required_terms](const expression::Term &term) {
                                      if (term.is_attribute())
                                      {
                                          terms.emplace_back(term);
                                          symbols.request(term);
                                      }
                                  });
    }

    this->child()->request_symbols(phase, symbols);
}