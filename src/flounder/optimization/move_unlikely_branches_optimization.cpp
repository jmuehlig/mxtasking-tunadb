#include "move_unlikely_branches_optimization.h"
#include <fmt/core.h>
#include <iostream>

using namespace flounder;

void MoveUnlikelyBranchesOptimization::apply(flounder::Program &program)
{
    auto &code = program.body();
    if (code.empty())
    {
        return;
    }

    for (auto line = 0U; line < code.size(); ++line)
    {
        const auto &instruction = code[line];

        if (std::holds_alternative<CmpInstruction>(instruction))
        {
            const auto &cmp_instruction = std::get<CmpInstruction>(instruction);
            if (cmp_instruction.is_likely() == false)
            {
                auto &jmp_instruction = std::get<JumpInstruction>(code[line + 1U]);
                const auto branch_end_label = jmp_instruction.label();
                auto branch_end_line =
                    MoveUnlikelyBranchesOptimization::find_branch_end_section(code, line, branch_end_label);

                if (branch_end_line.has_value())
                {
                    /// New label to jump to, the moved branch starts here,
                    auto new_branch_label = program.label(fmt::format("{}_moved_branch", branch_end_label.label()));

                    /// Inverse the jmp.
                    jmp_instruction.inverse();
                    jmp_instruction.label(new_branch_label);

                    auto branch_code = InstructionSet{branch_end_line.value() - line};

                    /// Start of the branch.
                    branch_code << program.section(new_branch_label);

                    /// Move the code of the branch.
                    std::move(code.lines().begin() + line + 2U, code.lines().begin() + branch_end_line.value(),
                              std::back_inserter(branch_code.lines()));
                    code.lines().erase(code.lines().begin() + line + 2U,
                                       code.lines().begin() + branch_end_line.value());

                    /// Jump to the end of the compare.
                    branch_code << program.jmp(branch_end_label);

                    /// Move the code to the end of the original code.
                    code << std::move(branch_code);

                    /// Overjump the jmp instruction.
                    ++line;
                }
            }
        }

        if (std::holds_alternative<RetInstruction>(instruction))
        {
            break;
        }
    }
}

std::optional<std::size_t> MoveUnlikelyBranchesOptimization::find_branch_end_section(
    const flounder::InstructionSet &code, const std::size_t begin_line, const flounder::Label end_label) noexcept
{
    for (auto branch_line = begin_line + 2U; branch_line < code.size(); ++branch_line)
    {
        const auto &branch_instruction = code[branch_line];
        if (std::holds_alternative<SectionInstruction>(branch_instruction))
        {
            const auto &section_instruction = std::get<SectionInstruction>(branch_instruction);
            if (section_instruction.label() == end_label)
            {
                return branch_line;
            }
        }
    }

    return std::nullopt;
}