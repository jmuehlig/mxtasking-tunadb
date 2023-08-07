#pragma once

#include "operator_interface.h"
#include <db/execution/compilation/hashtable/descriptor.h>
#include <db/execution/compilation/hashtable/table_proxy.h>
#include <db/execution/compilation/record_token.h>
#include <db/expression/term.h>
#include <fmt/core.h>
#include <mx/memory/alignment_helper.h>
#include <mx/memory/global_heap.h>

namespace db::execution::compilation {

class HashJoinBuildOperator final : public UnaryOperator
{
public:
    HashJoinBuildOperator(topology::PhysicalSchema &&keys_schema, topology::PhysicalSchema &&entries_schema,
                          mx::resource::ptr hash_table, const hashtable::Descriptor &hash_table_descriptor)
        : _keys_schema(std::move(keys_schema)), _entries_schema(std::move(entries_schema)), _hash_table(hash_table),
          _hash_table_descriptor(hash_table_descriptor)
    {
    }

    ~HashJoinBuildOperator() noexcept override = default;

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
        /// This operator breaks the pipeline, the child is executed as its own
        /// node within the graph.
        return this->child()->dependencies(); // std::make_optional(OperatorProgramContext{this->child().get()});
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) override;

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const override
    {
        return this->child()->input_data_generator();
    }

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("Build {{ {} }}", this->pipeline_identifier());
    }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        const auto hash_table_size = hashtable::TableProxy::size(_hash_table_descriptor);
        container.insert(std::make_pair("Hash Table Keys", _keys_schema.to_string()));
        container.insert(std::make_pair("Hash Table Entries", _entries_schema.to_string()));
        container.insert(
            std::make_pair("#Slots / Hash Table", util::string::shorten_number(_hash_table_descriptor.capacity())));
        container.insert(std::make_pair("Size Hash Table", util::string::shorten_data_size(hash_table_size)));
        container.insert(std::make_pair("#Entries / Bucket", std::to_string(_hash_table_descriptor.bucket_capacity())));
        container.insert(
            std::make_pair("Is multiple Entries", _hash_table_descriptor.is_multiple_entries_per_key() ? "Yes" : "No"));
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _entries_schema; }

    [[nodiscard]] const topology::PhysicalSchema &keys_schema() const { return _keys_schema; }

    [[nodiscard]] const topology::PhysicalSchema &entries_schema() const { return _entries_schema; }

private:
    /// The schema keys are stored within the hash table.
    topology::PhysicalSchema _keys_schema;

    /// The schema entries are stored within the hash table.
    /// This operator has no "output" schema since all tuples are consumed.
    topology::PhysicalSchema _entries_schema;

    /// Hash table.
    mx::resource::ptr _hash_table;

    hashtable::Descriptor _hash_table_descriptor;
};

class HashJoinProbeOperator final : public BinaryOperator
{
public:
    HashJoinProbeOperator(topology::PhysicalSchema &&schema, const topology::PhysicalSchema &hash_table_keys_schema,
                          const topology::PhysicalSchema &hash_table_entries_schema, const mx::resource::ptr hash_table,
                          const hashtable::Descriptor &hash_table_descriptor,
                          std::vector<expression::Term> &&probe_terms) noexcept
        : _schema(std::move(schema)), _hash_table_keys_schema(hash_table_keys_schema),
          _hash_table_entries_schema(hash_table_entries_schema), _hash_table(hash_table),
          _hash_table_descriptor(hash_table_descriptor), _probe_terms(std::move(probe_terms))
    {
    }

    ~HashJoinProbeOperator() noexcept override = default;

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

    /// Schema of the probed hash table keys.
    const topology::PhysicalSchema &_hash_table_keys_schema;

    /// Schema of the probed hash table entries.
    const topology::PhysicalSchema &_hash_table_entries_schema;

    /// Hash table.
    const mx::resource::ptr _hash_table;

    hashtable::Descriptor _hash_table_descriptor;

    /// Terms to probe.
    const std::vector<expression::Term> _probe_terms;
};
} // namespace db::execution::compilation