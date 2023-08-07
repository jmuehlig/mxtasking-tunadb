#pragma once

#include "operator_interface.h"
#include <db/execution/scan_generator.h>
#include <db/expression/operation.h>
#include <db/topology/table.h>
#include <fmt/core.h>

namespace db::execution::compilation {
class ScanOperator final : public OperatorInterface
{
public:
    ScanOperator(const topology::Table &table, topology::PhysicalSchema &&schema,
                 std::unique_ptr<expression::Operation> &&predicate) noexcept
        : _table(table), _schema(std::move(schema))
    {
        if (predicate != nullptr)
        {
            ScanOperator::split_and(_selection_predicates, std::move(predicate));
        }
    }

    ScanOperator(const topology::Table &table, topology::PhysicalSchema &&schema) noexcept
        : ScanOperator(table, std::move(schema), nullptr)
    {
    }

    ~ScanOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(const GenerationPhase /*phase*/, flounder::Program & /*program*/,
                 CompilationContext & /*context*/) override
    {
        /// Since the scan is the last operator in the chain, consume will never get called.
    }

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        return std::nullopt;
    }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::annotation<RecordSet>::CompletionCallbackInterface>
    completion_callback() override
    {
        return nullptr;
    }

    [[nodiscard]] enum mx::tasking::annotation::resource_boundness resource_boundness() const noexcept override
    {
        return mx::tasking::annotation::resource_boundness::mixed;
    }

    [[nodiscard]] bool is_finalize_pipeline_premature() const noexcept override { return false; }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const override
    {
        return std::make_unique<ScanGenerator>(_table);
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(const GenerationPhase /*phase*/) override
    {
        return nullptr;
    }

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override { return std::nullopt; }

    [[nodiscard]] std::uint8_t count_prefeches() const override { return _count_prefetches; }

    [[nodiscard]] std::string to_string() const override { return _table.name(); }

    [[nodiscard]] std::string pipeline_identifier() const override { return this->to_string(); }

    void emit_information(std::unordered_map<std::string, std::string> & /*container*/) override {}

    void emit_memory_tags(std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>
                              & /*container*/) const override
    {
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    const topology::Table &_table;
    const topology::PhysicalSchema _schema;

    std::vector<std::unique_ptr<expression::Operation>> _selection_predicates;

    /// List of terms for prefetching.
    std::unordered_map<std::uint16_t, float> _prefetch_candidates;

    /// Number of prefetched cache lines.
    std::uint8_t _count_prefetches;

    static void split_and(std::vector<std::unique_ptr<expression::Operation>> &predicate_list,
                          std::unique_ptr<expression::Operation> &&predicate);
};
} // namespace db::execution::compilation