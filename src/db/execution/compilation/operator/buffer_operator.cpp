#include "buffer_operator.h"
#include <db/execution/compilation/materializer.h>

using namespace db::execution::compilation;

void BufferOperator::produce(db::execution::compilation::OperatorInterface::GenerationPhase phase,
                             flounder::Program &program, db::execution::compilation::CompilationContext &context)
{
    this->child()->produce(phase, program, context);
}

void BufferOperator::consume(db::execution::compilation::OperatorInterface::GenerationPhase phase,
                             flounder::Program &program, db::execution::compilation::CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        auto context_guard = flounder::ContextGuard{program, "Record Buffer"};

        auto record_id_vreg = program.vreg("record_id");
        program << program.request_vreg64(record_id_vreg) << program.mov(record_id_vreg, program.constant32(1))
                << program.xadd(program.mem(std::uintptr_t(this->_record_buffer) + RowRecordBuffer::size_offset()),
                                record_id_vreg, true);

        auto record_address_vreg = program.vreg("record_addr");
        program << program.request_vreg64(record_address_vreg) << program.mov(record_address_vreg, record_id_vreg)
                << program.clear(record_id_vreg)
                << program.imul(record_address_vreg, program.constant32(this->_stored_schema.row_size()))
                << program.add(record_address_vreg, program.constant64(std::uintptr_t(this->_record_buffer->begin())));

        /// Materialize the record.
        RowMaterializer::materialize(program, context.symbols(), this->_stored_schema, record_address_vreg, 0U);

        program << program.clear(record_address_vreg);
        context.symbols().release(program, this->_stored_schema.terms());
    }
}

void BufferOperator::request_symbols(db::execution::compilation::OperatorInterface::GenerationPhase phase,
                                     db::execution::compilation::SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        symbols.request(this->_stored_schema.terms());
    }

    return this->child()->request_symbols(phase, symbols);
}

std::unique_ptr<OutputProviderInterface> BufferOperator::output_provider(
    db::execution::compilation::OperatorInterface::GenerationPhase phase)
{
    if (phase == GenerationPhase::execution)
    {
        return std::make_unique<RecordBufferOutputProvider>(this->_record_buffer);
    }

    return this->child()->output_provider(phase);
}