#include "hash_join_operator.h"
#include "hash_table_output_provider.h"
#include <db/execution/compilation/hash.h>
#include <db/execution/compilation/hash_emitter.h>
#include <db/execution/compilation/key_comparator.h>
#include <db/execution/compilation/materializer.h>
#include <flounder/lib.h>
#include <fmt/format.h>

using namespace db::execution::compilation;

void HashJoinBuildOperator::produce(const GenerationPhase phase, flounder::Program &program,
                                    CompilationContext &context)
{
    this->child()->produce(phase, program, context);
}

void HashJoinBuildOperator::consume(db::execution::compilation::OperatorInterface::GenerationPhase phase,
                                    flounder::Program &program, db::execution::compilation::CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        const auto &incoming_schema = this->child()->schema();

        auto build_context_guard = flounder::ContextGuard{program, "Hash Join Build"};

        /// Create the hash.
        auto key_vregs = std::vector<flounder::Register>{};
        std::transform(this->_keys_schema.terms().begin(), this->_keys_schema.terms().end(),
                       std::back_inserter(key_vregs),
                       [&context](const auto &term) { return context.symbols().get(term); });
        auto hash_vreg = HashEmitter<SimpleHash>::hash(program, key_vregs, this->_keys_schema.types());

        /// Insert record into hash table.
        auto hash_table_vreg = program.vreg("hj_hash_table");
        program << program.request_vreg64(hash_table_vreg)
                << program.mov(hash_table_vreg, program.constant64(std::uintptr_t(this->_hash_table.get())));
        hashtable::TableProxy::insert(
            program, this->_hash_table_descriptor, hash_table_vreg, hash_vreg,

            /// Compare the keys.
            [&key_vregs, &key_types = this->_keys_schema.types()](
                flounder::Program &program_, flounder::Register key_address, const std::uint32_t offset,
                flounder::Label eq_label, flounder::Label else_label) {
                JoinKeyComparator::emit(program_, key_vregs, key_types, key_address, offset, eq_label, else_label);
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

            /// Write the entry.
            [&schema = this->_entries_schema, &symbols = context.symbols()](
                flounder::Program &program_, flounder::Register record_address_vreg, const std::uint32_t offset) {
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

        program << program.clear(hash_vreg) << program.clear(hash_table_vreg);
    }
    else if (this->parent() != nullptr)
    {
        this->parent()->consume(phase, program, context);
    }
}

void HashJoinBuildOperator::request_symbols(const GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        symbols.request(this->_keys_schema.terms());
        symbols.request(this->_entries_schema.terms());
    }

    this->child()->request_symbols(phase, symbols);
}

std::unique_ptr<OutputProviderInterface> HashJoinBuildOperator::output_provider(const GenerationPhase /*phase*/)
{
    return nullptr;
}

void HashJoinProbeOperator::produce(const GenerationPhase phase, flounder::Program &program,
                                    CompilationContext &context)
{
    this->right_child()->produce(phase, program, context);
}

void HashJoinProbeOperator::consume(db::execution::compilation::OperatorInterface::GenerationPhase phase,
                                    flounder::Program &program, db::execution::compilation::CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        auto probe_term_names = std::vector<std::string>{};
        std::transform(this->_probe_terms.begin(), this->_probe_terms.end(), std::back_inserter(probe_term_names),
                       [](const auto &term) { return term.to_string(); });
        auto hash_table_identifier = fmt::format("hj_probe_{}", fmt::join(std::move(probe_term_names), "_"));

        /// Schema of the probing child.
        const auto &child_schema = this->right_child()->schema();

        auto probe_context_guard = flounder::ContextGuard{program, "Hash Join Probe"};

        /// Load the probe term. The probe term may be or not requested by parent operators.
        /// Therefor, we have to clear the register, if the symbol is not requested by others.
        auto probe_term_types = std::vector<type::Type>{};
        auto probe_term_vregs = std::vector<flounder::Register>{};
        for (const auto &term : this->_probe_terms)
        {
            const auto term_index = child_schema.index(term).value();
            probe_term_types.emplace_back(child_schema.type(term_index));
            probe_term_vregs.emplace_back(context.symbols().get(term));
        }

        /// Hash the term.
        auto probe_term_hash_vreg = HashEmitter<SimpleHash>::hash(program, probe_term_vregs, probe_term_types);

        /// Emit the hash table lookup.
        auto hash_table_vreg = program.vreg(fmt::format("hj_hash_table_{}", hash_table_identifier));
        program << program.request_vreg64(hash_table_vreg)
                << program.mov(hash_table_vreg, program.constant64(std::uintptr_t(this->_hash_table.get())));
        hashtable::TableProxy::find(
            program, std::string{hash_table_identifier}, this->_hash_table_descriptor, hash_table_vreg,
            probe_term_hash_vreg,
            /// Compare the keys.
            [&probe_term_vregs, &probe_term_types](flounder::Program &program_, flounder::Register key_address,
                                                   const std::uint32_t offset, flounder::Label neq_label) {
                JoinKeyComparator::emit(program_, probe_term_vregs, probe_term_types, key_address, offset, neq_label);
            },
            [&context, &hash_table_keys_schema = this->_hash_table_keys_schema,
             &hash_table_entries_schema = this->_hash_table_entries_schema, parent = this->parent()](
                flounder::Program &program_, flounder::Register key_address, const std::uint32_t key_offset,
                flounder::Register record_address, const std::uint32_t record_offset) {
                /// Load requested values found in hash table key into registers.
                RowMaterializer::load(program_, context.symbols(), hash_table_keys_schema, key_address, key_offset);

                /// Load requested values found in hash table into registers.
                RowMaterializer::load(program_, context.symbols(), hash_table_entries_schema, record_address,
                                      record_offset);

                /// Place next operators of the pipeline.
                parent->consume(GenerationPhase::execution, program_, context);
            });
        context.symbols().release(program, this->_probe_terms);
        program << program.clear(probe_term_hash_vreg) << program.clear(hash_table_vreg);
    }
    else
    {
        this->parent()->consume(phase, program, context);
    }
}

void HashJoinProbeOperator::request_symbols(const db::execution::compilation::OperatorInterface::GenerationPhase phase,
                                            db::execution::compilation::SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        symbols.request(this->_probe_terms);
    }
    this->right_child()->request_symbols(phase, symbols);
}
