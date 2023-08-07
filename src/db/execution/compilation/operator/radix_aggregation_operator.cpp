#include "radix_aggregation_operator.h"
#include "hash_table_output_provider.h"
#include <db/execution/compilation/hash.h>
#include <db/execution/compilation/hash_emitter.h>
#include <db/execution/compilation/key_comparator.h>
#include <db/execution/compilation/prefetcher.h>
#include <db/execution/compilation/scan_loop.h>
#include <flounder/lib.h>
#include <flounder/string.h>

using namespace db::execution::compilation;

RadixAggregationOperator::RadixAggregationOperator(
    topology::PhysicalSchema &&schema, topology::PhysicalSchema &&group_schema,
    topology::PhysicalSchema &&aggregation_schema, const topology::PhysicalSchema &incoming_schema,
    std::vector<std::unique_ptr<db::expression::Operation>> &&aggregations,
    std::vector<mx::resource::ptr> &&hash_tables, const hashtable::Descriptor &hash_table_descriptor)
    : AbstractAggregationOperator(std::move(schema), std::move(aggregation_schema), incoming_schema,
                                  std::move(aggregations)),
      _group_schema(std::move(group_schema)), _hash_tables(std::move(hash_tables)),
      _hash_table_descriptor(hash_table_descriptor)
{
}

void RadixAggregationOperator::produce(const GenerationPhase phase, flounder::Program &program,
                                       CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        this->aggregate(program, context);
    }
    else if (phase == GenerationPhase::finalization)
    {
        this->scan_aggregations(program, context);
    }
    else if (phase == GenerationPhase::prefetching)
    {
        this->_count_prefetches = PrefetchCallbackGenerator::produce(program, this->_incoming_schema);
    }
}

void RadixAggregationOperator::aggregate(flounder::Program &program, CompilationContext &context)
{
    /// Create the register where the address to the hash table is stored (needed by children).
    auto hash_table_vreg = program.vreg("ra_hash_table");

    /// Load the address for the hash table from arguments.
    program.arguments() << program.request_vreg64(hash_table_vreg) << program.get_arg2(hash_table_vreg);

    /// Scan loop.
    auto scan_context_guard = flounder::ContextGuard{program, "Scan"};
    {
        auto scan_loop = PaxScanLoop{program, context, "ht_aggregate", this->_incoming_schema, true};

        {
            auto aggregation_context_guard = flounder::ContextGuard{program, "Radix Group Aggregation"};

            /// Create the hash.
            auto group_term_vregs = std::vector<flounder::Register>{};
            std::transform(this->_group_schema.terms().begin(), this->_group_schema.terms().end(),
                           std::back_inserter(group_term_vregs),
                           [&context](const auto &term) { return context.symbols().get(term); });
            auto group_hash_vreg =
                HashEmitter<SimpleHash>::hash(program, group_term_vregs, this->_group_schema.types());

            /// Insert new groups into the hash table and update existing ones.
            hashtable::TableProxy::insert_or_update(
                program, this->_hash_table_descriptor, hash_table_vreg, group_hash_vreg,
                /// Callback to compare the keys in the hash table with current record values.
                [&group_schema = this->_group_schema,
                 &context](flounder::Program &program_, flounder::Register key_address, const std::uint32_t offset,
                           flounder::Label eq_label, flounder::Label else_label) {
                    AggregationKeyComparator::emit(program_, group_schema, context, key_address, offset, eq_label,
                                                   else_label);
                },
                /// Callback to write keys into the hash table.
                [&group_schema = this->_group_schema,
                 &context](flounder::Program &program_, flounder::Register key_address, const std::uint32_t offset) {
                    /// Materialize keys (= the group).
                    RowMaterializer::materialize(program_, context.symbols(), group_schema, key_address, offset);
                },
                /// Callback to insert values into the hash table (slot was allocated first time).
                [&context, &schema = this->_aggregation_schema, &aggregations = this->_aggregations](
                    flounder::Program &program_, flounder::Register record_address_vreg, const std::uint32_t offset) {
                    /// New entry in hash table allocated. Set default values:
                    /// * 1 for COUNT
                    /// * value of this record for SUM, AVG, MIN, MAX
                    for (const auto &operation : aggregations)
                    {
                        const auto index = schema.index(operation->result().value());
                        if (index.has_value())
                        {
                            auto target_address =
                                RowMaterializer::access(program_, record_address_vreg, offset, schema, index.value());
                            if (operation->id() == expression::Operation::Id::Count)
                            {
                                /// Default value for COUNT() is 1.
                                program_ << program_.mov(target_address, program_.constant32(1));
                            }
                            else
                            {
                                /// Every other aggregation need to access the record.
                                /// The record is arranged like the incoming schema.
                                auto *aggregation = reinterpret_cast<expression::UnaryOperation *>(operation.get());

                                /// Emit code for the operation (could be simple attribute access or more complex
                                /// arithmetic).
                                auto operation_register = context.symbols().get(aggregation->child()->result().value());

                                /// Initialize the aggregation value with the value of the first group record.
                                program_ << program_.mov(target_address, operation_register);
                            }
                        }
                    }
                },
                /// Callback to update the values in the hash table (aggregate record values into existing hash table
                /// entry).
                [&context, &schema = this->_aggregation_schema, &aggregations = this->_aggregations](
                    flounder::Program &program_, flounder::Register record_address_vreg, const std::uint32_t offset) {
                    /// Update existing values within the hash table.
                    for (const auto &operation : aggregations)
                    {
                        const auto index = schema.index(operation->result().value());
                        if (index.has_value())
                        {
                            auto target_address =
                                RowMaterializer::access(program_, record_address_vreg, offset, schema, index.value());
                            if (operation->id() == expression::Operation::Id::Count)
                            {
                                /// Default value for COUNT() is 1.
                                program_ << program_.add(target_address, program_.constant8(1));
                            }
                            else
                            {
                                /// Every other aggregation need to access the record.
                                /// The record is arranged like the incoming schema.
                                auto *aggregation = reinterpret_cast<expression::UnaryOperation *>(operation.get());

                                /// Emit code for the operation (could be simple attribute access or more complex
                                /// arithmetic).
                                auto operation_register = context.symbols().get(aggregation->child()->result().value());

                                if (aggregation->id() == expression::Operation::Id::Sum ||
                                    aggregation->id() == expression::Operation::Id::Average)
                                {
                                    /// Initialize the aggregation value with the value of the first group record.
                                    program_ << program_.add(target_address, operation_register);
                                }
                            }
                        }
                    }
                });

            program << program.clear(group_hash_vreg);
        }

        for (const auto &operation : this->_aggregations)
        {
            expression::for_each_term(operation, [&program, &context](const expression::Term &term) {
                if (term.is_attribute())
                {
                    context.symbols().release(program, term);
                }
            });
        }

        for (const auto &group : this->_group_schema.terms())
        {
            context.symbols().release(program, group);
        }
    }

    /// Free the hash table register at end of the program.
    program << program.clear(hash_table_vreg);
}

void RadixAggregationOperator::scan_aggregations(flounder::Program &program, CompilationContext &context)
{
    auto aggregation_context_guard = flounder::ContextGuard{program, "Radix Group Aggregation"};

    /// Read the local worker id.
    auto partition_hash_table = program.vreg("partition_hash_table");
    program.arguments() << program.request_vreg64(partition_hash_table) << program.get_arg2(partition_hash_table);

    hashtable::TableProxy::for_each(
        program, "local_aggregation_table", this->_hash_table_descriptor, partition_hash_table,
        [parent_operator = this->parent(), &context, &groups_schema = this->_group_schema,
         &schema = this->_aggregation_schema, count_index = this->_count_index, &aggregations = this->_aggregations](
            flounder::Program &program_, flounder::Label next_step_label, flounder::Label foot_label,
            flounder::Register slot_vreg, const std::uint32_t /*hash_offset*/, const std::uint32_t key_offset,
            flounder::Register records_vreg, const std::uint32_t records_offset) {
            /// Calculate the averages (until now, we only stores the sum).
            /// Therefore, we need to request the count term to be loaded (which
            /// is not need to be a "real" aggregation).
            auto count_term = std::optional<expression::Term>{std::nullopt};
            if (count_index.has_value())
            {
                count_term = schema.term(count_index.value());
                context.symbols().request(count_term.value());
            }

            /// Load the keys into register.
            RowMaterializer::load(program_, context.symbols(), groups_schema, slot_vreg, key_offset);

            /// Load the aggregated values into register.
            RowMaterializer::load(program_, context.symbols(), schema, records_vreg, records_offset);

            /// Calculate the averages from sum and (requested) count.
            if (count_term.has_value())
            {
                auto count_register = context.symbols().get(count_term.value());
                for (const auto &aggregation : aggregations)
                {
                    if (aggregation->id() == expression::Operation::Id::Average)
                    {
                        const auto index = schema.index(aggregation->result().value());
                        if (index.has_value())
                        {
                            auto avg_register = context.symbols().get(aggregation->result().value());

                            /// For AVERAGE divide aggregation_register (which is SUM) by COUNT and
                            /// store into aggregation_target_address.
                            program_ << program_.fdiv(avg_register, count_register);
                        }
                    }
                }
            }

            if (count_term.has_value())
            {
                context.symbols().release(program_, count_term.value());
            }

            context.label_next_record(next_step_label);
            context.label_scan_end(foot_label);
            parent_operator->consume(GenerationPhase::finalization, program_, context);
            context.label_next_record(std::nullopt);
            context.label_scan_end(std::nullopt);
        });
    program << program.clear(partition_hash_table);
}

std::unique_ptr<OutputProviderInterface> RadixAggregationOperator::output_provider(const GenerationPhase phase)
{
    if (phase == GenerationPhase::execution)
    {
        return std::make_unique<HashtableOutputProvider<true>>(this->_hash_tables);
    }

    return nullptr;
}

void RadixAggregationOperator::request_symbols(const GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        symbols.request(this->_aggregations);
        symbols.request(this->_group_schema.terms());
    }
}