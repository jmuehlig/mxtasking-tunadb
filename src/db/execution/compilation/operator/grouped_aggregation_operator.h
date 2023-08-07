#pragma once

#include "abstract_aggregation_operator.h"
#include "operator_interface.h"
#include <db/execution/compilation/hashtable/abstract_table.h>
#include <db/execution/compilation/hashtable/table_proxy.h>
#include <db/expression/operation.h>
#include <fmt/core.h>
#include <memory>
#include <utility>
#include <vector>

namespace db::execution::compilation {
class GroupedAggregationOperator final : public AbstractAggregationOperator
{
public:
    GroupedAggregationOperator(topology::PhysicalSchema &&schema, topology::PhysicalSchema &&group_schema,
                               topology::PhysicalSchema &&aggregation_schema,
                               const topology::PhysicalSchema &incoming_schema,
                               std::vector<std::unique_ptr<db::expression::Operation>> &&aggregations,
                               std::vector<hashtable::AbstractTable *> &&hash_tables,
                               hashtable::Descriptor hash_table_descriptor);
    ~GroupedAggregationOperator() override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        /// Since we have hash tables equal to the number of workers,
        /// we can map each hash table to its worker.
        auto data = std::vector<mx::resource::ptr>{};
        data.reserve(this->_hash_tables.size());
        for (auto worker_id = std::uint16_t(0U); worker_id < this->_hash_tables.size(); ++worker_id)
        {
            data.emplace_back(
                mx::resource::ptr{this->_hash_tables[worker_id],
                                  mx::resource::information{worker_id, mx::synchronization::primitive::None}});
        }

        return std::make_pair(mx::tasking::dataflow::annotation<RecordSet>::FinalizationType::reduce, std::move(data));
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) override;

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return child()->dependencies();
    }

    [[nodiscard]] std::string to_string() const override { return child()->to_string(); }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        container.insert(std::make_pair("#Hash Tables", std::to_string(_hash_tables.size())));

        container.insert(
            std::make_pair("#Slots / Hash Table", util::string::shorten_number(_hash_table_descriptor.capacity())));
        container.insert(std::make_pair(
            "Size / Hash Table", util::string::shorten_data_size(hashtable::TableProxy::size(_hash_table_descriptor))));
        container.insert(std::make_pair(
            "Size Hash Tables", util::string::shorten_data_size(hashtable::TableProxy::size(_hash_table_descriptor) *
                                                                _hash_tables.size())));

        this->child()->emit_information(container);
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    /// Schema of the group stored as key within the hash table.
    topology::PhysicalSchema _group_schema;

    /// One local grouped result (= Hashtable + dynamic tile) for each channel.
    std::vector<hashtable::AbstractTable *> _hash_tables;

    hashtable::Descriptor _hash_table_descriptor;

    std::optional<flounder::Register> _hash_table_vreg{std::nullopt};

    /**
     * Aggregates the consuming tuples into the core-local hash table.
     *
     * @param program Program to emit code.
     * @param context Compilation context.
     */
    void aggregate(flounder::Program &program, CompilationContext &context);

    /**
     * Merges the core-local aggregations into a single hash table by
     * inserting all missing tuples and merging existing ones.
     * After that, the tuples will be emitted to the graph.
     *
     * @param program Program to emit code.
     * @param context Compilation context.
     */
    void merge_aggregations(flounder::Program &program, CompilationContext &context);

    /**
     * Emits code for creating a hash over the group and
     * returns the vreg holding the hash value.
     *
     * @param program Program to emit code.
     * @param symbol_set Symbol set.
     * @param incoming_schema Schema of the incoming data.
     * @param group_schema Schema of the group that is hashed.
     * @return Virtual Register contining the group hash.
     */
    [[nodiscard]] static flounder::Register hash_group(flounder::Program &program, SymbolSet &symbol_set,
                                                       const topology::PhysicalSchema &incoming_schema,
                                                       const topology::PhysicalSchema &group_schema);
};

} // namespace db::execution::compilation