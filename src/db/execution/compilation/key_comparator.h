#pragma once

#include <db/execution/compilation/context.h>
#include <db/topology/physical_schema.h>
#include <db/type/type.h>
#include <flounder/program.h>
#include <vector>

namespace db::execution::compilation {
class JoinKeyComparator
{
public:
    static void emit(flounder::Program &program, const std::vector<flounder::Register> &key_vregs,
                     const std::vector<type::Type> &key_types, flounder::Register key_address, std::uint32_t offset,
                     flounder::Label eq_label, flounder::Label else_label);

    static void emit(flounder::Program &program, const std::vector<flounder::Register> &key_vregs,
                     const std::vector<type::Type> &key_types, flounder::Register key_address, std::uint32_t offset,
                     flounder::Label neq_label);
};

class AggregationKeyComparator
{
public:
    static void emit(flounder::Program &program, const topology::PhysicalSchema &key_schema,
                     CompilationContext &context, flounder::Register key_address, std::uint32_t offset,
                     flounder::Label eq_label, flounder::Label else_label);
};

} // namespace db::execution::compilation