#pragma once
#include <array>
#include <cstdint>
#include <flounder/ir/instructions.h>
#include <optional>
#include <unordered_set>

namespace flounder {
class ABI
{
public:
    /**
     * @return List of machine register ids that are allowed for register allocation.
     */
    [[nodiscard]] static constexpr auto available_mreg_ids() noexcept
    {
        return std::array<std::uint8_t, 12U>{{3U, 5U, 6U, 7U, 8U, 9U, 10U, 11U, 12U, 13U, 14U, 15U}};
    }

    /**
     * @return List of machine register ids that are used for spilling, when needed.
     */
    [[nodiscard]] static constexpr auto spill_mreg_ids() noexcept { return std::array<std::uint8_t, 3U>{{1U, 0U, 2U}}; }

    /**
     * @return Id of the register that points to the top of the stack.
     */
    [[nodiscard]] static constexpr auto stack_pointer_mreg_id() noexcept { return 4U; }

    /**
     * @return List of register ids that are used for call arguments.
     */
    [[nodiscard]] static constexpr auto call_argument_register_ids() noexcept
    {
        return std::array<std::uint8_t, 6U>{{7U, 6U, 2U, 1U, 8U, 9U}};
    }

    /**
     * @return The Id of the call return register.
     */
    [[nodiscard]] static constexpr auto call_return_register_id() noexcept { return 0U; }

    /**
     * @param register_id Id of a register.
     * @return True, if the given register is a scratch register that has to be saved by the caller.
     */
    [[nodiscard]] static auto is_scratch_mreg(const std::uint8_t register_id) noexcept
    {
        switch (register_id)
        {
        case 0U:
        case 1U:
        case 2U:
        case 6U:
        case 7U:
        case 8U:
        case 9U:
        case 10U:
        case 11U:
            return true;
        default:
            return false;
        }
    }

    /**
     * @param register_id Id of a register.
     * @return True, if the given register is a scratch register that has to be saved by the caller.
     */
    [[nodiscard]] static auto is_preserved_mreg(const std::uint8_t register_id) noexcept
    {
        switch (register_id)
        {
        case 3U:
        case 4U:
        case 5U:
        case 12U:
        case 13U:
        case 14U:
        case 15U:
            return true;
        default:
            return false;
        }
    }

    [[nodiscard]] static bool has_mreg_dependency(const InstructionType type)
    {
        return type == InstructionType::Idiv || type == InstructionType::Shl;
    }

    [[nodiscard]] static std::optional<std::unordered_set<std::uint8_t>> mreg_dependencies(const InstructionType type)
    {
        if (type == InstructionType::Idiv)
        {
            return std::make_optional(std::unordered_set<std::uint8_t>{0U, 1U, 2U});
        }

        if (type == InstructionType::Shl || type == InstructionType::Shr)
        {
            return std::make_optional(std::unordered_set<std::uint8_t>{1U});
        }

        return std::nullopt;
    }
};
} // namespace flounder