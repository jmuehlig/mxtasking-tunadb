#pragma once

#include "abstract_aggregation_operator.h"
#include "aggregation_result.h"
#include "operator_interface.h"
#include <db/expression/operation.h>
#include <fmt/core.h>
#include <memory>
#include <utility>
#include <vector>

namespace db::execution::compilation {
class AggregationOutputProvider final : public OutputProviderInterface
{
public:
    AggregationOutputProvider(std::shared_ptr<LocalAggregationResult> local_results) noexcept
        : _local_results(std::move(local_results))
    {
    }

    ~AggregationOutputProvider() override = default;

    std::uintptr_t get(const std::uint16_t worker_id,
                       std::optional<std::reference_wrapper<const RecordToken>> /*token*/,
                       mx::tasking::dataflow::EmitterInterface<execution::RecordSet> & /*graph*/,
                       mx::tasking::dataflow::NodeInterface<execution::RecordSet> * /*node*/) override
    {
        return std::uintptr_t(_local_results->at(worker_id).data());
    }

private:
    std::shared_ptr<LocalAggregationResult> _local_results;
};

class AggregationOperator final : public AbstractAggregationOperator
{
public:
    AggregationOperator(topology::PhysicalSchema &&schema, topology::PhysicalSchema &&aggregation_schema,
                        const topology::PhysicalSchema &incoming_schema,
                        std::vector<std::unique_ptr<db::expression::Operation>> &&aggregations);
    ~AggregationOperator() override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        return std::make_pair(mx::tasking::dataflow::annotation<RecordSet>::FinalizationType::sequential,
                              std::vector<mx::resource::ptr>{});
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) override;

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return child()->dependencies();
    }

    [[nodiscard]] std::string to_string() const override { return child()->to_string(); }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

    void emit_memory_tags(std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>
                              &container) const override
    {
        if (_local_results != nullptr)
        {
            auto tags = std::vector<std::pair<std::uintptr_t, std::uintptr_t>>{};
            tags.emplace_back(std::uintptr_t(_local_results.get()),
                              std::uintptr_t(_local_results.get()) + _local_results->size_in_bytes());
            container.insert(std::make_pair("Aggregation", std::move(tags)));
        }

        UnaryOperator::emit_memory_tags(container);
    }

private:
    /// Map of channel to channel-local results.
    std::shared_ptr<LocalAggregationResult> _local_results{nullptr};

    std::optional<flounder::Register> _local_aggregation_result_vreg{std::nullopt};

    /// List of registers that are used during aggregation when consuming the incoming records.
    std::vector<flounder::Register> _consume_aggregation_result_registers;

    /// List of registers that are used when merging different local results.
    std::vector<flounder::Register> _finalize_aggregation_result_registers;

    void merge_results_into_core_local(flounder::Program &program, CompilationContext &context);
};

} // namespace db::execution::compilation