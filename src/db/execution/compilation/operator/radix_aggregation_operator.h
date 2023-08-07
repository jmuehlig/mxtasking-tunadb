#pragma once

#include "abstract_aggregation_operator.h"
#include "operator_interface.h"
#include <db/execution/compilation/hashtable/descriptor.h>
#include <db/execution/compilation/hashtable/table_proxy.h>
#include <db/expression/operation.h>
#include <fmt/core.h>
#include <memory>
#include <utility>
#include <vector>

namespace db::execution::compilation {
class RadixAggregationOperator final : public AbstractAggregationOperator
{
public:
    RadixAggregationOperator(topology::PhysicalSchema &&schema, topology::PhysicalSchema &&group_schema,
                             topology::PhysicalSchema &&aggregation_schema,
                             const topology::PhysicalSchema &incoming_schema,
                             std::vector<std::unique_ptr<db::expression::Operation>> &&aggregations,
                             std::vector<mx::resource::ptr> &&hash_tables,
                             const hashtable::Descriptor &hash_table_descriptor);
    ~RadixAggregationOperator() override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase /*phase*/, flounder::Program & /*program*/, CompilationContext & /*context*/) override
    {
        /// Since this operator consumes tuples from emitted tiles, it will call the parents consume(),
        /// but its own consume will never get called (like scan).
    }

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        return std::make_pair(mx::tasking::dataflow::annotation<RecordSet>::FinalizationType::parallel, _hash_tables);
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) override;

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        /// This operator breaks the pipeline, the child is executed as its own
        /// node within the graph.
        return std::make_optional(OperatorProgramContext{this->child().get()});
    }

    [[nodiscard]] std::uint8_t count_prefeches() const override { return _count_prefetches; }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::annotation<RecordSet>::CompletionCallbackInterface>
    completion_callback() override
    {
        return nullptr;
    }

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("Aggregate {{ {} }}", this->pipeline_identifier());
        ;
    }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        container.insert(std::make_pair("#Hash Tables", std::to_string(_hash_tables.size())));

        const auto hash_table_size = hashtable::TableProxy::size(_hash_table_descriptor);
        container.insert(
            std::make_pair("#Slots / Hash Table", util::string::shorten_number(_hash_table_descriptor.capacity())));
        container.insert(std::make_pair("Size / Hash Table", util::string::shorten_data_size(hash_table_size)));
        container.insert(
            std::make_pair("Size Hash Tables", util::string::shorten_data_size(hash_table_size * _hash_tables.size())));

        this->child()->emit_information(container);
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    /// Schema of the group stored as key within the hash table.
    topology::PhysicalSchema _group_schema;

    /// List of all hash tables.
    std::vector<mx::resource::ptr> _hash_tables;

    hashtable::Descriptor _hash_table_descriptor;

    std::uint8_t _count_prefetches;

    /**
     * Aggregates the consuming tuples into the worker-local hash table.
     *
     * @param program Program to emit code.
     * @param context Context.
     */
    void aggregate(flounder::Program &program, CompilationContext &context);

    /**
     * Scans the worker-local hash table and emits tuples to the next operator,
     * that, in the end, materializes the tuples.
     *
     * @param program Program to emit code.
     * @param context Context.
     */
    void scan_aggregations(flounder::Program &program, CompilationContext &context);
};

} // namespace db::execution::compilation