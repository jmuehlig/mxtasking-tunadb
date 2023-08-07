#include "program.h"

using namespace flounder;

std::vector<std::string> InstructionSet::code() const noexcept
{
    auto lines = std::vector<std::string>{};
    lines.reserve(this->_lines.size() + static_cast<std::size_t>(this->_name.has_value()));
    if (this->_name.has_value())
    {
        lines.emplace_back(fmt::format("; ---- {} ----", this->_name.value()));
    }

    auto intend_count = std::uint16_t(0U);
    auto intend = std::string{""};
    for (const auto &instruction : this->_lines)
    {
        auto is_loop_begin = false;
        auto is_loop_end = false;

        //        if (*node == NodeType::BRANCH_BEGIN_MARKER)
        //        {
        //            lines.emplace_back(fmt::format("{}{}", intend, node->to_string()));
        //            intend_count += 4U;
        //            intend = std::string(intend_count, ' ');
        //        }
        //        else if (*node == NodeType::BRANCH_END_MARKER)
        //        {
        //            intend_count -= 4U;
        //            intend = std::string(intend_count, ' ');
        //        }
        //        else
        {
            std::visit([&lines, intend](
                           const auto &instr) { lines.emplace_back(fmt::format("{}{}", intend, instr.to_string())); },
                       instruction);
        }
    }

    return lines;
}

std::vector<std::string> Program::code() const noexcept
{
    auto blocks = std::vector<std::string>{};
    for (const auto &block : _blocks)
    {
        if (block.empty() == false)
        {
            auto code = block.code();
            std::move(code.begin(), code.end(), std::back_inserter(blocks));
        }
    }

    return blocks;
}