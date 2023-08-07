#include "scan_operator.h"
#include <db/execution/compilation/expression.h>
#include <db/execution/compilation/materializer.h>
#include <db/execution/compilation/prefetcher.h>
#include <db/execution/compilation/scan_loop.h>
#include <flounder/statement.h>
#include <mx/tasking/runtime.h>
using namespace db::execution::compilation;

void ScanOperator::produce(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    if (phase == GenerationPhase::finalization)
    {
        this->parent()->consume(phase, program, context);
        return;
    }

    if (phase == GenerationPhase::prefetching)
    {
        this->_count_prefetches =
            PrefetchCallbackGenerator::produce(program, this->_table.schema(), std::move(this->_prefetch_candidates));

        return;
    }

    /// Scan loop.
    {
        auto context_guard = flounder::ContextGuard{program, "Scan"};

        auto scan_loop = PaxScanLoop{program, context, std::string{this->_table.name()}, this->_table.schema(),
                                     this->_selection_predicates.empty()};

        if (this->_selection_predicates.empty())
        {
            /// Place next operators of the pipeline.
            this->parent()->consume(phase, program, context);
        }
        else
        {
            auto data_vreg = scan_loop.tile_data_vreg();
            auto row_vreg = scan_loop.row_index();

            /// For each predicate: load, emit, release.
            for (const auto &predicate : this->_selection_predicates)
            {
                expression::for_each_term(
                    predicate, [&program, &context, &schema = this->_schema, data_vreg, row_vreg](const auto &term) {
                        if (term.is_attribute())
                        {
                            /// Load predicate.
                            PaxMaterializer::load(program, context.symbols(), schema, term, data_vreg, row_vreg);
                        }
                    });

                Expression::emit(program, this->_schema, context.expressions(), predicate, context.label_next_record());

                expression::for_each_term(predicate, [&program, &context](const auto &term) {
                    if (term.is_attribute())
                    {
                        context.symbols().release(program, term);
                    }
                });
            }

            /// Load rest and emit parent operator.
            program << program.begin_branch(0);
            PaxMaterializer::load(program, context.symbols(), this->_schema, data_vreg, row_vreg);
            this->parent()->consume(phase, program, context);
            program << program.end_branch();
        }
    }
}

void ScanOperator::request_symbols(const db::execution::compilation::OperatorInterface::GenerationPhase phase,
                                   db::execution::compilation::SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        this->_prefetch_candidates.reserve(this->_table.schema().size());

        for (const auto &predicate : this->_selection_predicates)
        {
            expression::for_each_term(predicate, [&symbols](const auto &term) {
                if (term.is_attribute())
                {
                    symbols.request(term);
                }
            });

            if (predicate->is_comparison())
            {
                const auto &binary = reinterpret_cast<const std::unique_ptr<expression::BinaryOperation> &>(predicate);
                if (binary->left_child()->is_nullary())
                {
                    const auto index = this->_table.schema().index(binary->left_child()->result().value());
                    if (index.has_value())
                    {
                        this->_prefetch_candidates.insert(
                            std::make_pair(index.value(), predicate->annotation().selectivity().value_or(1)));
                    }
                }
            }
        }

        for (auto i = 0U; i < this->_table.schema().size(); ++i)
        {
            const auto &term = this->_table.schema().term(i);
            if (symbols.is_requested(term))
            {
                if (this->_prefetch_candidates.find(i) == this->_prefetch_candidates.end())
                {
                    this->_prefetch_candidates.insert(std::make_pair(i, 1.0));
                }
            }
        }
    }
}

void ScanOperator::split_and(std::vector<std::unique_ptr<expression::Operation>> &predicate_list,
                             std::unique_ptr<expression::Operation> &&predicate)
{
    if (predicate->id() == expression::Operation::Id::And)
    {
        auto *and_predicate = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        ScanOperator::split_and(predicate_list, std::move(and_predicate->left_child()));
        ScanOperator::split_and(predicate_list, std::move(and_predicate->right_child()));
    }
    else
    {
        predicate_list.emplace_back(std::move(predicate));
    }
}