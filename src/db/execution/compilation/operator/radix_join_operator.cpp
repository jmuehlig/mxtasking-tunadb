#include "radix_join_operator.h"
#include "hash_table_output_provider.h"
#include <db/execution/compilation/hash.h>
#include <db/execution/compilation/hash_emitter.h>
#include <db/execution/compilation/key_comparator.h>
#include <db/execution/compilation/materializer.h>
#include <db/execution/compilation/prefetcher.h>
#include <db/execution/compilation/scan_loop.h>
#include <flounder/lib.h>
#include <fmt/format.h>

using namespace db::execution::compilation;

void RadixJoinBuildOperator::produce(const GenerationPhase phase, flounder::Program &program,
                                     CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        /// Create the register where the address to the hash table is stored (needed by children).
        auto hash_table_vreg = program.vreg("rj_hash_table_addr");

        /// Load the address for the hash table from arguments.
        program.arguments() << program.request_vreg64(hash_table_vreg) << program.get_arg2(hash_table_vreg);

        /// Replace the hash table pointer, if the table was resized.
        hashtable::TableProxy::replace_hash_table_address_with_resized_hash_table(
            program, "ht_build", this->_hash_table_descriptor, hash_table_vreg);

        /// Resize the hash table if needed.
        hashtable::TableProxy::resize_if_required(
            program, this->_hash_table_descriptor, hash_table_vreg,
            [&keys_schema = this->_keys_schema, radix_bits = this->_radix_bits](
                flounder::Program &program_, flounder::Register key_address_vreg, const std::uint32_t key_offset) {
                auto key_vregs = std::vector<flounder::Register>{};
                for (auto i = 0U; i < keys_schema.size(); ++i)
                {
                    auto term_vreg = program_.vreg(fmt::format("key_{}_for_hash", keys_schema.term(i).to_string()));
                    program_ << program_.request_vreg(term_vreg, keys_schema.type(i).register_width())
                             << program_.mov(term_vreg,
                                             program_.mem(key_address_vreg, key_offset + keys_schema.row_offset(i)));
                    key_vregs.emplace_back(term_vreg);
                }
                auto hash_vreg =
                    HashEmitter<RadixHash>::hash(RadixHash{radix_bits}, program_, key_vregs, keys_schema.types());

                for (auto key_vreg : key_vregs)
                {
                    program_ << program_.clear(key_vreg);
                }

                return hash_vreg;
            });

        const auto &incoming_schema = this->child()->schema();

        /// Scan loop.
        auto scan_context_guard = flounder::ContextGuard{program, "Scan"};
        {
            auto scan_loop = PaxScanLoop{program, context, "ht_build", incoming_schema, true};

            {
                auto build_context_guard = flounder::ContextGuard{program, "Radix Join Build"};

                /// Create the hash.
                auto key_vregs = std::vector<flounder::Register>{};
                std::transform(this->_keys_schema.terms().begin(), this->_keys_schema.terms().end(),
                               std::back_inserter(key_vregs),
                               [&context](const auto &term) { return context.symbols().get(term); });
                auto hash_vreg = HashEmitter<RadixHash>::hash(RadixHash{this->_radix_bits}, program, key_vregs,
                                                              this->_keys_schema.types());

                /// Maybe the partition filter wants to reuse the hash; if yes, set up in symbols.
                if (context.symbols().is_requested(RadixJoinBuildOperator::MAIN_HASH))
                {
                    context.symbols().set(RadixJoinBuildOperator::MAIN_HASH, hash_vreg);
                }

                /// Insert record into hash table.
                hashtable::TableProxy::insert(
                    program, this->_hash_table_descriptor, hash_table_vreg, hash_vreg,

                    /// Compare the keys.
                    [&key_vregs, &key_types = this->_keys_schema.types()](
                        flounder::Program &program_, flounder::Register key_address, const std::uint32_t offset,
                        flounder::Label eq_label, flounder::Label else_label) {
                        JoinKeyComparator::emit(program_, key_vregs, key_types, key_address, offset, eq_label,
                                                else_label);
                    },

                    /// Write the keys.
                    [&key_vregs, &key_types = this->_keys_schema.types()](
                        flounder::Program &program_, flounder::Register key_address, std::uint32_t offset) {
                        for (auto i = 0U; i < key_vregs.size(); ++i)
                        {
                            program_ << program_.mov(program_.mem(key_address, offset, key_types[i].register_width()),
                                                     key_vregs[i]);
                            offset += key_types[i].size();
                        }
                    },

                    /// Write the record.
                    [&schema = this->_entries_schema,
                     &symbols = context.symbols()](flounder::Program &program_, flounder::Register record_address_vreg,
                                                   const std::uint32_t offset) {
                        /// Copy the record into the hash table (more specifically the record vector).
                        /// In general, we could also use the materialize to dematerialize the values
                        /// and materialize here; in fact it is slower and uses more registers.
                        RowMaterializer::materialize(program_, symbols, schema, record_address_vreg, offset);
                    });

                context.symbols().release(program, this->_keys_schema.terms());
                context.symbols().release(program, this->_entries_schema.terms());

                if (this->parent() != nullptr)
                {
                    this->parent()->consume(phase, program, context);
                }

                program << program.clear(hash_vreg);
            }
        }

        /// Free the hash table register at end of the program.
        program << program.clear(hash_table_vreg);
    }
    else if (phase == GenerationPhase::prefetching)
    {
        this->_count_prefetches = PrefetchCallbackGenerator::produce(program, this->child()->schema());
    }
    //    else
    //    {
    //        for(auto hash_table : this->_hash_tables)
    //        {
    //            auto table_vreg = program.vreg("htable");
    //            program << program.request_vreg64(table_vreg)
    //                    << program.mov(table_vreg, program.constant64(std::uintptr_t(hash_table.get())));
    //            flounder::FunctionCall{program,
    //            std::uintptr_t(&hashtable::ChainedTable::dump)}.call({flounder::Operand{table_vreg}}); program <<
    //            program.clear(table_vreg);
    //        }
    //    }
}

void RadixJoinBuildOperator::request_symbols(const GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        symbols.request(this->_keys_schema.terms());
        symbols.request(this->_entries_schema.terms());
    }
}

std::unique_ptr<OutputProviderInterface> RadixJoinBuildOperator::output_provider(const GenerationPhase phase)
{
    if (phase == GenerationPhase::execution)
    {
        return std::make_unique<HashtableOutputProvider<true>>(this->_hash_tables);
    }

    return nullptr;
}
void RadixJoinProbeOperator::produce(const GenerationPhase phase, flounder::Program &program,
                                     CompilationContext &context)
{
    if (phase == GenerationPhase::finalization)
    {
        /// Since this operator is a pipeline breaker, tuples are consumed from here.
        this->parent()->consume(phase, program, context);
        return;
    }

    if (phase == GenerationPhase::prefetching)
    {
        this->_count_prefetches = PrefetchCallbackGenerator::produce(program, this->right_child()->schema());
        return;
    }

    auto probe_term_names = std::vector<std::string>{};
    std::transform(this->_probe_terms.begin(), this->_probe_terms.end(), std::back_inserter(probe_term_names),
                   [](const auto &term) { return term.to_string(); });
    auto hash_table_identifier = fmt::format("rj_probe_{}", fmt::join(std::move(probe_term_names), "_"));

    auto hash_table_vreg = program.vreg(fmt::format("rj_hash_table_{}", hash_table_identifier));

    /// Load start and end parameter into registers.
    program.arguments() << program.request_vreg64(hash_table_vreg) << program.get_arg3(hash_table_vreg);

    /// Replace the hash table pointer, if the table was resized.
    hashtable::TableProxy::replace_hash_table_address_with_resized_hash_table(
        program, "ht_probe", this->_hash_table_descriptor, hash_table_vreg);

    /// Create and request a register for the hash table.
    /// Store the value from the build operators hash table there.
    const auto &child_schema = this->right_child()->schema();

    /// Scan loop.
    auto scan_context_guard = flounder::ContextGuard{program, "Scan"};
    {
        auto scan_loop = PaxScanLoop{program, context, "ht_probe", child_schema, false};
        {
            auto probe_context_guard = flounder::ContextGuard{program, "Radix Join Probe"};

            /// Load the probe term. The probe term may be or not requested by parent operators.
            /// Therefor, we have to clear the register, if the symbol is not requested by others.
            auto probe_term_is_requested = std::vector<bool>();
            auto probe_term_types = std::vector<type::Type>{};
            auto probe_term_vregs = std::vector<flounder::Register>{};
            for (const auto &term : this->_probe_terms)
            {
                const auto is_requested = context.symbols().is_requested(term);
                const auto term_index = child_schema.index(term).value();
                probe_term_is_requested.emplace_back(is_requested);
                probe_term_types.emplace_back(child_schema.type(term_index));

                auto loaded_vreg = PaxMaterializer::load(program, context.symbols(), term, probe_term_types.back(),
                                                         child_schema.pax_offset(term_index),
                                                         scan_loop.tile_data_vreg(), scan_loop.row_index());
                probe_term_vregs.emplace_back(loaded_vreg);
                if (is_requested)
                {
                    context.symbols().set(term, loaded_vreg);
                }
            }

            /// Hash the term.
            auto probe_term_hash_vreg =
                HashEmitter<RadixHash>::hash(RadixHash{this->_radix_bits}, program, probe_term_vregs, probe_term_types);

            /// Emit the hash table lookup.
            hashtable::TableProxy::find(
                program, std::string{hash_table_identifier}, this->_hash_table_descriptor, hash_table_vreg,
                probe_term_hash_vreg,
                [&probe_term_vregs, &probe_term_types](flounder::Program &program_, flounder::Register key_address,
                                                       const std::uint32_t offset, flounder::Label neq_label) {
                    JoinKeyComparator::emit(program_, probe_term_vregs, probe_term_types, key_address, offset,
                                            neq_label);
                },
                [&context, &hash_table_keys_schema = this->_hash_table_keys_schema,
                 &hash_table_entries_schema = this->_hash_table_entries_schema, &child_schema, &scan_loop,
                 parent = this->parent(), phase](flounder::Program &program_, flounder::Register key_address,
                                                 const std::uint32_t key_offset, flounder::Register record_address,
                                                 const std::uint32_t record_offset) {
                    /// Load requested values found in hash table key into registers.
                    RowMaterializer::load(program_, context.symbols(), hash_table_keys_schema, key_address, key_offset);

                    /// Load requested values found in hash table into registers.
                    RowMaterializer::load(program_, context.symbols(), hash_table_entries_schema, record_address,
                                          record_offset);

                    /// Load all requested symbols from the tile into registers.
                    PaxMaterializer::load(program_, context.symbols(), child_schema, scan_loop.tile_data_vreg(),
                                          scan_loop.row_index());

                    /// Place next operators of the pipeline.
                    parent->consume(phase, program_, context);
                });

            program << program.clear(probe_term_hash_vreg);

            for (auto i = 0U; i < probe_term_vregs.size(); ++i)
            {
                if (probe_term_is_requested[i] == false)
                {
                    program << program.clear(probe_term_vregs[i]);
                }
            }
        }
    }

    /// Clear virtual registers used to scan.
    program << program.clear(hash_table_vreg);
}

void RadixJoinProbeOperator::request_symbols(
    const db::execution::compilation::OperatorInterface::GenerationPhase /*phase*/,
    db::execution::compilation::SymbolSet & /*symbols*/)
{
}
