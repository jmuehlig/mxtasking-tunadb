#pragma once

#include "operator_interface.h"
#include <db/execution/compilation/record_token.h>
#include <db/execution/compilation/row_record_buffer.h>
#include <db/expression/term.h>
#include <fmt/core.h>
#include <mx/memory/alignment_helper.h>
#include <mx/memory/global_heap.h>

namespace db::execution::compilation {

class RecordBufferOutputProvider final : public OutputProviderInterface
{
public:
    RecordBufferOutputProvider(RowRecordBuffer *buffer) noexcept : _buffer(buffer) {}

    ~RecordBufferOutputProvider() override { std::free(_buffer); }

    std::uintptr_t get(const std::uint16_t /*worker_id*/,
                       std::optional<std::reference_wrapper<const RecordToken>> /*token*/,
                       mx::tasking::dataflow::EmitterInterface<execution::RecordSet> & /*graph*/,
                       mx::tasking::dataflow::NodeInterface<execution::RecordSet> * /*node*/) override
    {
        return std::uintptr_t(_buffer);
    }

private:
    RowRecordBuffer *_buffer;
};

class BufferOperator final : public UnaryOperator
{
public:
    BufferOperator(topology::PhysicalSchema &&schema, RowRecordBuffer *record_buffer)
        : _stored_schema(std::move(schema)), _record_buffer(record_buffer)
    {
    }

    ~BufferOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        return std::nullopt;
    }

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return this->child()->dependencies();
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) override;

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const override
    {
        return this->child()->input_data_generator();
    }

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("Buffer {{ {} }}", this->pipeline_identifier());
    }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        container.insert(std::make_pair("#Buffer capacity", std::to_string(_record_buffer->capacity())));
        this->child()->emit_information(container);
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _stored_schema; }

private:
    /// The schema stored in the hash table.
    /// This operator has no "output" schema since all tuples are consumed.
    topology::PhysicalSchema _stored_schema;

    /// Buffer where to write the records.
    RowRecordBuffer *_record_buffer;
};
} // namespace db::execution::compilation