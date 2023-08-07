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

class RadixJoinBuildOperator final : public UnaryOperator
{
public:
    static inline expression::Term MAIN_HASH = expression::Term{expression::Attribute{"probe_main_hash"}};

    RadixJoinBuildOperator(topology::PhysicalSchema &&key_schema, topology::PhysicalSchema &&entries_schema,
                           const std::vector<mx::resource::ptr> &hash_tables,
                           const hashtable::Descriptor &hash_table_descriptor, const std::uint8_t radix_bits)
        : _keys_schema(std::move(key_schema)), _entries_schema(std::move(entries_schema)), _hash_tables(hash_tables),
          _hash_table_descriptor(hash_table_descriptor), _radix_bits(radix_bits)
    {
    }

    ~RadixJoinBuildOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase /*phase*/, flounder::Program & /*program*/, CompilationContext & /*context*/) override
    {
        /// Since this operator is standalone (only consumes tuples from the tile and inserts
        /// them into the hash table), we do not need to consume.
    }

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        //        return std::make_pair(mx::tasking::dataflow::annotation<RecordSet>::FinalizationType::sequential,
        //        std::vector<mx::resource::ptr>{mx::resource::ptr{}});
        return std::nullopt;
    }

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        /// This operator breaks the pipeline, the child is executed as its own
        /// node within the graph.
        return std::make_optional(OperatorProgramContext{this->child().get()});
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) override;

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const override
    {
        /// Since the build will consume data from the graph,
        /// we do not need to commit any data.
        return nullptr;
    }

    [[nodiscard]] std::uint8_t count_prefeches() const override { return _count_prefetches; }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::annotation<RecordSet>::CompletionCallbackInterface>
    completion_callback() override
    {
        return nullptr;
    }

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("Build {{ {} }}", this->pipeline_identifier());
    }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        container.insert(std::make_pair("#Hash Tables", std::to_string(_hash_tables.size())));

        const auto hash_table_size = hashtable::TableProxy::size(_hash_table_descriptor);
        container.insert(std::make_pair("Hash Table Keys", _keys_schema.to_string()));
        container.insert(std::make_pair("Hash Table Entries", _entries_schema.to_string()));
        container.insert(
            std::make_pair("#Slots / Hash Table", util::string::shorten_number(_hash_table_descriptor.capacity())));
        container.insert(std::make_pair("Size / Hash Table", util::string::shorten_data_size(hash_table_size)));
        container.insert(
            std::make_pair("Size Hash Tables", util::string::shorten_data_size(hash_table_size * _hash_tables.size())));
        container.insert(std::make_pair("#Entries / Slot", std::to_string(_hash_table_descriptor.bucket_capacity())));
        container.insert(
            std::make_pair("Is multiple Entries", _hash_table_descriptor.is_multiple_entries_per_key() ? "Yes" : "No"));
    }

    void emit_memory_tags(std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>
                              &container) const override
    {
        auto name = fmt::format("Hash Table ({})", this->to_string());

        auto hash_tables = std::vector<std::pair<std::uintptr_t, std::uintptr_t>>{};
        hash_tables.reserve(_hash_tables.size());
        for (const auto table : _hash_tables)
        {
            const auto begin = std::uintptr_t(table.get());
            const auto end = begin + hashtable::TableProxy::size(_hash_table_descriptor);
            hash_tables.emplace_back(begin, end);
        }
        container.insert(std::make_pair(std::move(name), std::move(hash_tables)));

        UnaryOperator::emit_memory_tags(container);
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

    /// List of all hash tables.
    std::vector<mx::resource::ptr> _hash_tables;

    hashtable::Descriptor _hash_table_descriptor;

    const std::uint8_t _radix_bits;

    std::uint8_t _count_prefetches;
};

class RadixJoinProbeOperator final : public BinaryOperator
{
public:
    RadixJoinProbeOperator(topology::PhysicalSchema &&schema, const topology::PhysicalSchema &hash_table_keys_schema,
                           const topology::PhysicalSchema &hash_table_entries_schema,
                           const hashtable::Descriptor &hash_table_descriptor,
                           std::vector<expression::Term> &&probe_terms, const std::uint8_t radix_bits) noexcept
        : _schema(std::move(schema)), _hash_table_keys_schema(hash_table_keys_schema),
          _hash_table_entries_schema(hash_table_entries_schema), _hash_table_descriptor(hash_table_descriptor),
          _probe_terms(std::move(probe_terms)), _radix_bits(radix_bits)
    {
    }

    ~RadixJoinProbeOperator() noexcept override = default;

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
        return std::nullopt;
    }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const override
    {
        /// Since the probe will consume data from the graph,
        /// we do not need to commit any data.
        return nullptr;
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(const GenerationPhase /*phase*/) override
    {
        return nullptr;
    }

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        /// This operator breaks the pipeline, the right child is executed as its own
        /// node within the graph. Furthermore, this operator depends on the operators
        /// on the left side (which is a build operator).
        return std::make_optional(OperatorProgramContext{this->right_child().get(), this->left_child().get()});
    }

    [[nodiscard]] std::uint8_t count_prefeches() const override { return _count_prefetches; }

    [[nodiscard]] enum mx::tasking::annotation::resource_boundness resource_boundness() const noexcept override
    {
        return mx::tasking::annotation::resource_boundness::memory;
    }

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("{} ⋈ {}", this->left_child()->pipeline_identifier(),
                           this->right_child()->pipeline_identifier());
        ;
    }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::annotation<RecordSet>::CompletionCallbackInterface>
    completion_callback() override
    {
        return nullptr;
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

    hashtable::Descriptor _hash_table_descriptor;

    /// Terms to probe.
    const std::vector<expression::Term> _probe_terms;

    const std::uint8_t _radix_bits;

    std::uint8_t _count_prefetches;
};
} // namespace db::execution::compilation