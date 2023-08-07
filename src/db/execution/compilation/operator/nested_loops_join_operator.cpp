#include "nested_loops_join_operator.h"
#include <db/execution/compilation/expression.h>
#include <db/execution/compilation/scan_loop.h>

using namespace db::execution::compilation;

void NestedLoopsJoinOperator::produce(db::execution::compilation::OperatorInterface::GenerationPhase phase,
                                      flounder::Program &program,
                                      db::execution::compilation::CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        this->_buffer_end_vreg = program.vreg("buffer_size");
        program.header() << program.request_vreg64(this->_buffer_end_vreg.value())
                         << program.mov(
                                this->_buffer_end_vreg.value(),
                                program.mem(std::uintptr_t(this->_record_buffer) + RowRecordBuffer::size_offset()))
                         << program.imul(this->_buffer_end_vreg.value(),
                                         program.constant32(this->left_child()->schema().row_size()))
                         << program.add(this->_buffer_end_vreg.value(),
                                        program.constant64(std::uintptr_t(this->_record_buffer->begin())));
    }

    this->right_child()->produce(phase, program, context);
}

void NestedLoopsJoinOperator::consume(db::execution::compilation::OperatorInterface::GenerationPhase phase,
                                      flounder::Program &program,
                                      db::execution::compilation::CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        auto probe_record_vreg = program.vreg("probe_record");
        program << program.request_vreg64(probe_record_vreg)
                << program.mov(probe_record_vreg, program.constant64(std::uintptr_t(this->_record_buffer->begin())));

        {
            auto context_guard = flounder::ContextGuard{program, "Nested Loops Join"};
            {
                auto inner_scan = RowScanLoop{program,
                                              context,
                                              probe_record_vreg,
                                              this->_buffer_end_vreg.value(),
                                              "probe_record_buffer",
                                              this->left_child()->schema(),
                                              true};

                Expression::emit(program, this->_schema, context.expressions(), this->_join_predicate,
                                 context.label_next_record());
                program << program.begin_branch(0);
                expression::for_each_term(this->_join_predicate,
                                          [&program, &symbols = context.symbols()](const auto &term) {
                                              if (term.is_attribute())
                                              {
                                                  symbols.release(program, term);
                                              }
                                          });

                this->parent()->consume(phase, program, context);

                program << program.end_branch();
            }
        }

        program << program.clear(probe_record_vreg) << program.clear(this->_buffer_end_vreg.value());
    }
    else if (phase == GenerationPhase::prefetching)
    {
        this->parent()->consume(phase, program, context);
    }
}

void NestedLoopsJoinOperator::request_symbols(
    const db::execution::compilation::OperatorInterface::GenerationPhase phase,
    db::execution::compilation::SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        expression::for_each_term(this->_join_predicate, [&symbols](const auto &term) {
            if (term.is_attribute())
            {
                symbols.request(term);
            }
        });
    }
}