#pragma once

#include "hash.h"
#include "symbol_set.h"
#include <flounder/program.h>
#include <vector>

namespace db::execution::compilation {
template <class H> class HashEmitter
{
public:
    [[nodiscard]] static flounder::Register hash(flounder::Program &program,
                                                 const std::vector<flounder::Register> &terms,
                                                 const std::vector<type::Type> &types)
    {
        return hash(H{}, program, terms, types);
    }

    [[nodiscard]] static flounder::Register hash(H &&hash, flounder::Program &program,
                                                 const std::vector<flounder::Register> &terms,
                                                 const std::vector<type::Type> &types)
    {
        if (terms.size() == 1U)
        {
            return hash.emit(program, types.front(), terms.front());
        }

        auto group_hash_vreg = program.vreg("group_hash");
        program << program.request_vreg(group_hash_vreg, flounder::RegisterWidth::r64);
        for (auto i = 0U; i < terms.size(); ++i)
        {
            const auto &term_vreg = terms[i];
            const auto type = types[i];

            auto hash_vreg = hash.emit(program, type, term_vreg);
            if (i == 0U)
            {
                program << program.mov(group_hash_vreg, hash_vreg);
            }
            else
            {
                HashCombine::emit(program, group_hash_vreg, hash_vreg);
            }
            program << program.clear(hash_vreg);
        }

        return group_hash_vreg;
    }
};
} // namespace db::execution::compilation