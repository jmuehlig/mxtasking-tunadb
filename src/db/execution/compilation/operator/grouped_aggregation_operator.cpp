#include "grouped_aggregation_operator.h"
#include "hash_table_output_provider.h"
#include <db/execution/compilation/hash.h>
#include <db/execution/compilation/key_comparator.h>
#include <db/execution/compilation/materializer.h>
#include <flounder/lib.h>
#include <flounder/statement.h>
#include <flounder/string.h>
#include <fmt/core.h>
#include <mx/tasking/runtime.h>

using namespace db::execution::compilation;

GroupedAggregationOperator::GroupedAggregationOperator(
    topology::PhysicalSchema &&schema, topology::PhysicalSchema &&group_schema,
    topology::PhysicalSchema &&aggregation_schema, const topology::PhysicalSchema &incoming_schema,
    std::vector<std::unique_ptr<db::expression::Operation>> &&aggregations,
    std::vector<hashtable::AbstractTable *> &&hash_tables, hashtable::Descriptor hash_table_descriptor)
    : AbstractAggregationOperator(std::move(schema), std::move(aggregation_schema), incoming_schema,
                                  std::move(aggregations)),
      _group_schema(std::move(group_schema)), _hash_tables(std::move(hash_tables)),
      _hash_table_descriptor(hash_table_descriptor)
{
}

void GroupedAggregationOperator::produce(const GenerationPhase phase, flounder::Program &program,
                                         CompilationContext &context)
{
    auto context_guard = flounder::ContextGuard{program, "Grouped Aggregation"};

    if (phase == GenerationPhase::finalization || phase == GenerationPhase::prefetching)
    {
        this->child()->produce(phase, program, context);
        return;
    }

    this->_hash_table_vreg = program.vreg("ga_hash_table", false);
    program.arguments() << program.request_vreg64(this->_hash_table_vreg.value())
                        << program.get_arg2(this->_hash_table_vreg.value());

    this->child()->produce(phase, program, context);

    program << program.clear(this->_hash_table_vreg.value());
}
void GroupedAggregationOperator::consume(const GenerationPhase phase, flounder::Program &program,
                                         CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        auto context_guard = flounder::ContextGuard{program, "Grouped Aggregation"};

        /// Aggregate the tuples by inserting them into the core-local hash table.
        this->aggregate(program, context);
    }
    else if (phase == GenerationPhase::finalization)
    {
        auto context_guard = flounder::ContextGuard{program, "Grouped Aggregation"};

        /// Merge core-local hash tables and emit records to the graph.
        this->merge_aggregations(program, context);
    }
    else if (phase == GenerationPhase::prefetching)
    {
        this->parent()->consume(phase, program, context);
    }
}

void GroupedAggregationOperator::request_symbols(const GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        symbols.request(this->_aggregations);
        symbols.request(this->_group_schema.terms());
    }

    this->child()->request_symbols(phase, symbols);
}
void GroupedAggregationOperator::aggregate(flounder::Program &program, CompilationContext &context)
{
    /// Create hash from groups.
    auto group_hash_vreg =
        GroupedAggregationOperator::hash_group(program, context.symbols(), this->_incoming_schema, this->_group_schema);

    /// Insert new groups into the hash table and update existing ones.
    hashtable::TableProxy::insert_or_update(
        program, this->_hash_table_descriptor, this->_hash_table_vreg.value(), group_hash_vreg,
        /// Callback to compare the keys in the hash table with current record values.
        [&group_schema = this->_group_schema, &context](flounder::Program &program_, flounder::Register key_address,
                                                        const std::uint32_t offset, flounder::Label eq_label,
                                                        flounder::Label else_label) {
            AggregationKeyComparator::emit(program_, group_schema, context, key_address, offset, eq_label, else_label);
        },
        /// Callback to write keys into the hash table.
        [&group_schema = this->_group_schema, &context](flounder::Program &program_, flounder::Register key_address,
                                                        const std::uint32_t offset) {
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
                        program_ << program_.mov(target_address, program_.constant8(1));
                    }
                    else
                    {
                        /// Every other aggregation need to access the record.
                        /// The record is arranged like the incoming schema.
                        auto *aggregation = reinterpret_cast<expression::UnaryOperation *>(operation.get());

                        /// Emit code for the operation (could be simple attribute access or more complex arithmetic).
                        auto operation_register = context.symbols().get(aggregation->child()->result().value());

                        /// Initialize the aggregation value with the value of the first group record.
                        program_ << program_.mov(target_address, operation_register);
                    }
                }
            }
        },
        /// Callback to update the values in the hash table (aggregate record values into existing hash table entry).
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

                        /// Emit code for the operation (could be simple attribute access or more complex arithmetic).
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

    /// Release the group hash vreg.
    program << program.clear(group_hash_vreg);
}

void GroupedAggregationOperator::merge_aggregations(flounder::Program &program, CompilationContext &context)
{
    const auto record_size = this->_aggregation_schema.row_size();
    const auto keys_size = this->_group_schema.row_size();

    /// Whenever there are more than a single hash table ( = more than one worker ), we need
    /// to provide reduce.
    const auto is_need_reduce = this->_hash_tables.size() > 1U;

    /// Read the local worker id.
    auto local_hash_table_vreg = program.vreg("local_aggregation_table");
    program.arguments() << program.request_vreg64(local_hash_table_vreg) << program.get_arg2(local_hash_table_vreg);

    auto is_last_label = program.label("emit_aggregation_result");
    auto finalization_finished_label = program.label("finalize_end");

    if (is_need_reduce)
    {
        /// Read the reduced worker id.
        auto reduced_hash_table_vreg = program.vreg("reduced_aggregation_table");
        program.arguments() << program.request_vreg64(reduced_hash_table_vreg)
                            << program.get_arg3(reduced_hash_table_vreg);

        /// Jump to the end if there is nothing to reduce.
        program << program.test(reduced_hash_table_vreg, reduced_hash_table_vreg) << program.jz(is_last_label);

        /// Copy all entries from the reduced hash table to the local hash table.
        hashtable::TableProxy::for_each(
            program, "reduced_aggregation_table", this->_hash_table_descriptor, reduced_hash_table_vreg,
            [record_size, keys_size, &schema = this->_aggregation_schema, &group_schema = this->_group_schema,
             &aggregations = this->_aggregations, hash_table_descriptor = this->_hash_table_descriptor,
             local_hash_table_vreg](flounder::Program &program_, flounder::Label /*next_step_label*/,
                                    flounder::Label /*foot_label*/, flounder::Register local_slot_vreg,
                                    const std::uint32_t local_hash_offset, const std::uint32_t local_key_offset,
                                    flounder::Register local_records_vreg, const std::uint32_t local_records_offset) {
                auto local_hash_vreg = program_.vreg("local_aggregation_table_hash");
                program_ << program_.request_vreg64(local_hash_vreg)
                         << program_.mov(local_hash_vreg, program_.mem(local_slot_vreg, local_hash_offset,
                                                                       flounder::RegisterWidth::r64));
                hashtable::TableProxy::insert_or_update(
                    program_, hash_table_descriptor, local_hash_table_vreg, local_hash_vreg,

                    /// Compare hash table keys with current record keys.
                    [&group_schema, local_slot_vreg, local_key_offset](
                        flounder::Program &insert_program, flounder::Register global_key_address_vreg,
                        const std::uint32_t offset, flounder::Label eq_label, flounder::Label else_label) {
                        for (auto group_index = 0U; group_index < group_schema.size(); ++group_index)
                        {
                            const auto type = group_schema.type(group_index);
                            if (RowMaterializer::is_materialize_with_pointer(type))
                            {
                                const auto &group_term = group_schema.term(group_index);
                                const auto group_term_offset = group_schema.row_offset(group_index);

                                auto result = flounder::String::is_equals(
                                    insert_program, fmt::format("merge_group_key_{}", group_term.to_string()),
                                    flounder::String::Descriptor{local_slot_vreg,
                                                                 std::int32_t(local_key_offset) + group_term_offset,
                                                                 type.char_description().length(), false, true},
                                    flounder::String::Descriptor{global_key_address_vreg,
                                                                 std::int32_t(offset) + group_term_offset,
                                                                 type.char_description().length(), false, true});
                                insert_program << insert_program.cmp(result, insert_program.constant8(1))
                                               << insert_program.jne(else_label) << insert_program.clear(result);
                            }
                            else
                            {
                                auto local_key_vreg = insert_program.vreg(
                                    fmt::format("{}_key", SymbolSet::make_vreg_name(group_schema.term(group_index))));
                                insert_program
                                    << insert_program.request_vreg(local_key_vreg,
                                                                   group_schema.type(group_index).register_width())
                                    << insert_program.mov(local_key_vreg,
                                                          RowMaterializer::access(insert_program, local_slot_vreg,
                                                                                  local_key_offset, group_schema,
                                                                                  group_index))
                                    << insert_program.cmp(local_key_vreg, RowMaterializer::access(
                                                                              insert_program, global_key_address_vreg,
                                                                              offset, group_schema, group_index))
                                    << insert_program.jne(else_label) << insert_program.clear(local_key_vreg);
                            }
                        }
                        insert_program << insert_program.jmp(eq_label);
                    },

                    /// Materialize keys into hash table (new entry allocated).
                    [keys_size, local_slot_vreg, local_key_offset](flounder::Program &insert_program,
                                                                   flounder::Register global_key_address_vreg,
                                                                   const std::uint32_t offset) {
                        flounder::Lib::memcpy(insert_program, global_key_address_vreg, offset, local_slot_vreg,
                                              local_key_offset, keys_size);
                    },

                    /// Materialize values into hash table (new entry allocated).
                    [record_size, local_records_vreg,
                     local_records_offset](flounder::Program &insert_program,
                                           flounder::Register global_record_address_vreg, const std::uint32_t offset) {
                        flounder::Lib::memcpy(insert_program, global_record_address_vreg, offset, local_records_vreg,
                                              local_records_offset, record_size);
                    },

                    /// Merge entry in hash table with existing record.
                    [&schema, &aggregations, local_records_vreg,
                     local_records_offset](flounder::Program &insert_program,
                                           flounder::Register global_record_address_vreg, const std::uint32_t offset) {
                        for (const auto &aggregation : aggregations)
                        {
                            /// Read all aggregated values from the local hash table and merge with the local entry.
                            auto index = schema.index(aggregation->result().value());
                            if (index.has_value())
                            {
                                auto local_aggregation_vreg = insert_program.vreg(
                                    fmt::format("local_{}", SymbolSet::make_vreg_name(aggregation->result().value())));
                                auto local_aggregate_address = RowMaterializer::access(
                                    insert_program, local_records_vreg, local_records_offset, schema, index.value());
                                insert_program
                                    << insert_program.request_vreg(local_aggregation_vreg,
                                                                   schema.type(index.value()).register_width())
                                    << insert_program.mov(local_aggregation_vreg, local_aggregate_address);
                                if (aggregation->id() == expression::Operation::Id::Count ||
                                    aggregation->id() == expression::Operation::Id::Sum ||
                                    aggregation->id() == expression::Operation::Id::Average)
                                {
                                    auto global_aggregate_address = RowMaterializer::access(
                                        insert_program, global_record_address_vreg, offset, schema, index.value());
                                    insert_program
                                        << insert_program.add(global_aggregate_address, local_aggregation_vreg);
                                }
                                insert_program << insert_program.clear(local_aggregation_vreg);
                            }
                        }
                    });
                program_ << program_.clear(local_hash_vreg);
            });
        program << program.clear(reduced_hash_table_vreg) << program.jmp(finalization_finished_label);
    }

    /// Emit all entries from the (last) local table.
    program << program.section(is_last_label);
    hashtable::TableProxy::for_each(
        program, "local_aggregation_table", this->_hash_table_descriptor, local_hash_table_vreg,
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
    program << program.section(finalization_finished_label) << program.clear(local_hash_table_vreg);
}

std::unique_ptr<OutputProviderInterface> GroupedAggregationOperator::output_provider(const GenerationPhase phase)
{
    if (phase == GenerationPhase::execution)
    {
        auto hash_tables = std::vector<mx::resource::ptr>{};
        std::transform(this->_hash_tables.begin(), this->_hash_tables.end(), std::back_inserter(hash_tables),
                       [](auto *hash_table) { return mx::resource::ptr(hash_table); });

        return std::make_unique<HashtableOutputProvider<false>>(std::move(hash_tables));
    }

    return this->child()->output_provider(phase);
}

flounder::Register GroupedAggregationOperator::hash_group(flounder::Program &program, SymbolSet &symbol_set,
                                                          const topology::PhysicalSchema &incoming_schema,
                                                          const topology::PhysicalSchema &group_schema)
{
    auto group_hash_vreg = program.vreg("ga_group_hash");
    program << program.request_vreg(group_hash_vreg, flounder::RegisterWidth::r64)
            << program.xor_(group_hash_vreg, group_hash_vreg);
    for (const auto &term : group_schema.terms())
    {
        const auto index = incoming_schema.index(term);
        if (index.has_value())
        {
            auto term_hash_vreg =
                MurmurHash{0xc6a4a7935bd1e995}.emit(program, incoming_schema.type(index.value()), symbol_set.get(term));

            if (term == group_schema.terms().front())
            {
                program << program.mov(group_hash_vreg, term_hash_vreg);
            }
            else
            {
                HashCombine::emit(program, group_hash_vreg, term_hash_vreg);
            }

            program << program.clear(term_hash_vreg);
        }
    }

    return group_hash_vreg;
}