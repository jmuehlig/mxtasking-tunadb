#pragma once

#include "operator_interface.h"
#include <db/execution/compilation/row_record_buffer.h>
#include <db/expression/term.h>
#include <fmt/core.h>
#include <mx/memory/alignment_helper.h>
#include <mx/memory/global_heap.h>

namespace db::execution::compilation {

class NestedLoopsJoinOperator final : public BinaryOperator
{
public:
    NestedLoopsJoinOperator(topology::PhysicalSchema &&schema, const topology::PhysicalSchema &record_buffer_schema,
                            RowRecordBuffer *record_buffer,
                            std::unique_ptr<expression::Operation> &&join_predicate) noexcept
        : _schema(std::move(schema)), _record_buffer_schema(record_buffer_schema), _record_buffer(record_buffer),
          _join_predicate(std::move(join_predicate))
    {
    }

    ~NestedLoopsJoinOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        return std::nullopt;
    }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const override
    {
        return this->right_child()->input_data_generator();
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(const GenerationPhase /*phase*/) override
    {
        return nullptr;
    }

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        auto child_dependency = this->right_child()->dependencies();
        if (child_dependency.has_value())
        {
            child_dependency->add_dependent_operator(this->left_child().get());
            return child_dependency;
        }

        return std::make_optional(OperatorProgramContext{nullptr, this->left_child().get()});
    }

    [[nodiscard]] std::uint8_t count_prefeches() const override
    {
        if (this->right_child() != nullptr) [[likely]]
        {
            return this->right_child()->count_prefeches();
        }

        return 0U;
    }

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("{} ⋈ {}", this->left_child()->pipeline_identifier(),
                           this->right_child()->pipeline_identifier());
        ;
    }

    [[nodiscard]] std::string pipeline_identifier() const override { return this->to_string(); }

    void emit_information(std::unordered_map<std::string, std::string> & /*container*/) override {}

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    /// Schema produced by the probe,
    topology::PhysicalSchema _schema;

    /// Schema of the probed record buffer.
    const topology::PhysicalSchema &_record_buffer_schema;

    /// Buffer where the to probe tuples are stored.
    RowRecordBuffer *_record_buffer;

    /// Join predicate.
    std::unique_ptr<expression::Operation> _join_predicate;

    std::optional<flounder::Register> _buffer_end_vreg{std::nullopt};
};
} // namespace db::execution::compilation