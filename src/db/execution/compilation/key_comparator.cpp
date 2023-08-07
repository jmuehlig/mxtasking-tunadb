#include "key_comparator.h"
#include "materializer.h"
#include <flounder/string.h>

using namespace db::execution::compilation;

void JoinKeyComparator::emit(flounder::Program &program, const std::vector<flounder::Register> &key_vregs,
                             const std::vector<type::Type> &key_types, flounder::Register key_address,
                             std::uint32_t offset, flounder::Label eq_label, flounder::Label else_label)
{
    if (key_vregs.size() == 1U)
    {
        program << program.cmp(key_vregs.front(), program.mem(key_address, offset, key_types.front().register_width()))
                << program.je(eq_label) << program.jmp(else_label);
    }
    else
    {
        for (auto i = 0U; i < key_vregs.size(); ++i)
        {
            program << program.cmp(key_vregs[i], program.mem(key_address, offset, key_types[i].register_width()))
                    << program.jne(else_label);
            offset += key_types[i].size();
        }

        program << program.jmp(eq_label);
    }
}

void JoinKeyComparator::emit(flounder::Program &program, const std::vector<flounder::Register> &key_vregs,
                             const std::vector<type::Type> &key_types, flounder::Register key_address,
                             std::uint32_t offset, flounder::Label neq_label)
{
    if (key_vregs.size() == 1U)
    {
        program << program.cmp(key_vregs.front(), program.mem(key_address, offset, key_types.front().register_width()))
                << program.jne(neq_label);
    }
    else
    {
        for (auto i = 0U; i < key_vregs.size(); ++i)
        {
            program << program.cmp(key_vregs[i], program.mem(key_address, offset, key_types[i].register_width()))
                    << program.jne(neq_label);
            offset += key_types[i].size();
        }
    }
}

void AggregationKeyComparator::emit(flounder::Program &program, const topology::PhysicalSchema &key_schema,
                                    db::execution::compilation::CompilationContext &context,
                                    flounder::Register key_address, std::uint32_t offset, flounder::Label eq_label,
                                    flounder::Label else_label)
{
    /// Compare keys with keys in the hash table.
    for (auto group_term_id = 0U; group_term_id < key_schema.size(); ++group_term_id)
    {
        const auto &type = key_schema.type(group_term_id);
        const auto &group_term = key_schema.term(group_term_id);
        auto record_key_vreg = context.symbols().get(group_term);

        if (Materializer::is_materialize_with_pointer(type))
        {
            auto result = flounder::String::is_equals(
                program, fmt::format("group_cmp_{}", group_term.to_string()),
                flounder::String::Descriptor{record_key_vreg, type.char_description().length(), false, true},
                flounder::String::Descriptor{key_address, std::int32_t(offset) + key_schema.row_offset(group_term_id),
                                             type.char_description().length(), false, true});
            program << program.cmp(result, program.constant8(1)) << program.clear(result) << program.jne(else_label);
        }
        else
        {
            auto materialized_key = RowMaterializer::access(program, key_address, offset, key_schema, group_term_id);
            program << program.cmp(record_key_vreg, materialized_key) << program.jne(else_label);
        }
    }
    program << program.jmp(eq_label);
}