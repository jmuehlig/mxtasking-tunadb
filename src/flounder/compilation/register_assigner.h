#pragma once

#include "register_allocator.h"
#include <exception>
// #include <experimental/fixed_capacity_vector>
#include <ecpp/static_vector.hpp>
#include <flounder/abi/x86_64.h>
#include <flounder/program.h>
#include <optional>
#include <unordered_map>
#include <vector>

namespace flounder {
class RegisterAssigner
{
public:
    using mreg_id_t = std::uint8_t;
    using save_mreg_vector_t = ecpp::static_vector<mreg_id_t, 32U>;

    RegisterAssigner()
    {
        _live_machine_registers.reserve(ABI::available_mreg_ids().size());
        _touched_registers.reserve(ABI::available_mreg_ids().max_size() * 2U);
    }
    ~RegisterAssigner() = default;

    /**
     * Takes the given program, allocates machine registers for virtual ones
     * and returns the program with machine instead of virtual registers.
     *
     * @param program Program with virtual registers.
     * @param generate_inline_comments If true, all flounder instructions including virtual registers will be added as
     * an inline comment.
     */
    void process(Program &program, bool generate_inline_comments);

private:
    class SpillRegisterState
    {
    public:
        constexpr SpillRegisterState() noexcept = default;

        SpillRegisterState(Register reg, const bool is_dirty) noexcept : _loaded_vreg(reg), _is_dirty(is_dirty) {}

        ~SpillRegisterState() noexcept = default;

        SpillRegisterState &operator=(SpillRegisterState &&) noexcept = default;

        [[nodiscard]] std::optional<Register> vreg() const noexcept { return _loaded_vreg; }
        [[nodiscard]] bool holds(Register vreg) const noexcept { return _loaded_vreg == vreg; }
        [[nodiscard]] bool holds(std::string_view vreg_name) const noexcept
        {
            return _loaded_vreg.has_value() && _loaded_vreg->virtual_name().value() == vreg_name;
        }
        [[nodiscard]] bool is_dirty() const noexcept { return _loaded_vreg.has_value() && _is_dirty; }
        [[nodiscard]] bool empty() const noexcept { return _loaded_vreg.has_value() == false; }

        void is_dirty(const bool is_dirty) noexcept { _is_dirty = is_dirty; }

    private:
        std::optional<Register> _loaded_vreg{std::nullopt};
        bool _is_dirty{false};
    };

    class SpillRegisterAllocation
    {
    public:
        constexpr SpillRegisterAllocation() noexcept : _spill_mreg_ids(ABI::spill_mreg_ids()) {}

        ~SpillRegisterAllocation() noexcept = default;

        [[nodiscard]] auto &mreg_ids() noexcept { return _spill_mreg_ids; }

        [[nodiscard]] bool full() const noexcept
        {
            return std::find(_allocation.begin(), _allocation.end(), std::nullopt) == _allocation.end();
        }

        [[nodiscard]] std::optional<std::pair<mreg_id_t, bool>> find(Register vreg) const noexcept
        {
            for (auto i = 0U; i < _allocation.max_size(); ++i)
            {
                if (_allocation[i].has_value())
                {
                    if (std::holds_alternative<Register>(_allocation[i].value()))
                    {
                        if (std::get<Register>(_allocation[i].value()) == vreg)
                        {
                            return std::make_pair(_spill_mreg_ids[i], _is_load[i]);
                        }
                    }
                }
            }

            return std::nullopt;
        }

        void allocate(const mreg_id_t reg, std::variant<Register, Constant> value, const bool is_load) noexcept
        {
            for (auto i = 0U; i < _spill_mreg_ids.max_size(); ++i)
            {
                if (_spill_mreg_ids[i] == reg)
                {
                    _allocation[i] = std::make_optional(value);
                    _is_load[i] |= is_load;
                    return;
                }
            }
        }

        [[nodiscard]] std::optional<mreg_id_t> allocate(std::variant<Register, Constant> value,
                                                        const bool is_load) noexcept
        {
            for (auto i = 0U; i < _spill_mreg_ids.max_size(); ++i)
            {
                if (_allocation[i].has_value() == false)
                {
                    _allocation[i] = std::make_optional(value);
                    _is_load[i] = is_load;
                    return _spill_mreg_ids[i];
                }
            }

            return std::nullopt;
        }

        [[nodiscard]] bool is_free(const mreg_id_t reg) const noexcept
        {
            for (auto i = 0U; i < _spill_mreg_ids.max_size(); ++i)
            {
                if (_spill_mreg_ids[i] == reg)
                {
                    return _allocation[i].has_value() == false;
                }
            }

            return false;
        }

    private:
        std::array<mreg_id_t, ABI::spill_mreg_ids().max_size()> _spill_mreg_ids;
        std::array<std::optional<std::variant<Register, Constant>>, ABI::spill_mreg_ids().max_size()> _allocation;
        std::array<bool, ABI::spill_mreg_ids().max_size()> _is_load{false};
    };

    /// Register allocator.
    LinearScanRegisterAllocator _register_allocator;

    /// Schedule of the virtual registers.
    RegisterSchedule _vreg_schedule;

    /// States of spill registers.
    std::unordered_map<mreg_id_t, SpillRegisterState> _spill_reg_state;

    /// List of all touched machine registers.
    /// Only these will pushed and poped.
    std::unordered_set<mreg_id_t> _touched_registers;

    /// Current live machine register ids.
    std::unordered_set<std::uint8_t> _live_machine_registers;

    /**
     * Scans the given code and replaces virtual registers by machine ones.
     *
     * @param program Program to allocate register nodes.
     * @param code Node to replace virtual registers in.
     * @param schedule Schedule of virtual registers to machine registers and spill locations.
     * @param generate_inline_comments If true, all flounder instructions including virtual registers will be added as
     * an inline comment.
     * @return Instruction set with machine registers.
     */
    [[nodiscard]] InstructionSet assign(Program &program, InstructionSet &&code, bool generate_inline_comment);

    /**
     * Assigns register if needed and conveys the source instruction into the target set.
     *
     * @param program Program to allocate register nodes.
     * @param source Instruction to convey.
     * @param target Target.
     * @param is_generate_inline_comment If true, all flounder instructions including virtual registers will be added as
     * an inline comment.
     */
    void convey(Program &program, const Instruction &source, InstructionSet &target, bool is_generate_inline_comment);

    /**
     * Assigns register if needed and conveys the source instruction into the target set.
     *
     * @param program Program to allocate register nodes.
     * @param source Instructions to convey.
     * @param target Target.
     * @param generate_inline_comment If true, all flounder instructions including virtual registers will be added as
     * an inline comment.
     */
    void convey(Program &program, InstructionSet &&source, InstructionSet &target, const bool generate_inline_comment)
    {
        for (const auto &instruction : source.lines())
        {
            convey(program, instruction, target, generate_inline_comment);
        }
    }

    /**
     * Replaces the virtual registers and/or 64bit constants within
     * the given operand by machine registers.
     *
     * @param program Program to allocate register nodes from.
     * @param opeinstructionrand Instruction to replace vregs and large constants.
     * @param spill_register_allocation List of register ids, that are free for temporary use.
     * @param code Set to store spill load instructions.
     * @param is_generate_inline_comment If true, spill loads/writes will be generated with inline comments.
     */
    void replace_vreg_and_constant(Program &program, InstructionInterface &instruction,
                                   SpillRegisterAllocation &spill_register_allocation, InstructionSet &code,
                                   bool is_generate_inline_comment);

    /**
     * Finds a machine register to replace the virtual register.
     *
     * @param program Program to allocate register nodes from.
     * @param instruction Instruction holding the vreg.
     * @param operand_index Index of the operand that is the given vreg.
     * @param vreg Virtual register to replace by machine register.
     * @param spill_register_allocation List of register ids, that are free for temporary use.
     * @param code Set to store spill load instructions.
     * @param is_generate_inline_comment If true, spill loads/writes will be generated with inline comments.
     * @return Machine register (could be spill register or normal register) or the spill address.
     */
    [[nodiscard]] std::variant<Register, MemoryAddress> unspill_vreg(
        Program &program, const InstructionInterface &instruction, std::uint8_t operand_index, Register vreg,
        SpillRegisterAllocation &spill_register_allocation, InstructionSet &code, bool is_generate_inline_comment);

    /**
     * Finds a machine register to replace the virtual register.
     *
     * @param program Program to allocate register nodes from.
     * @param vreg Virtual register to replace by machine register.
     * @param spill_register_allocation List of register ids, that are free for temporary use.
     * @param code Set to store spill load instructions.
     * @param is_generate_inline_comment If true, spill loads/writes will be generated with inline comments.
     * @return Machine register (could be spill register or normal register).
     */
    [[nodiscard]] Register unspill_vreg(Program &program, Register vreg,
                                        SpillRegisterAllocation &spill_register_allocation, InstructionSet &code,
                                        bool is_generate_inline_comment);
    /**
     * Finds a machine register to replace the constant.
     *
     * @param program Program to allocate register nodes from.
     * @param vreg Virtual register to replace by machine register.
     * @param spill_register_allocation List of register ids, that are free for temporary use.
     * @param code Set to store spill load instructions.
     * @param is_generate_inline_comment If true, spill loads/writes will be generated with inline comments.
     * @return Machine register (will be spill register).
     */
    [[nodiscard]] Register unspill_constant(Program &program, Constant constant,
                                            SpillRegisterAllocation &spill_register_allocation, InstructionSet &code,
                                            bool is_generate_inline_comment);

    /**
     * Returns true, if a machine register is claimed.
     *
     * @param mreg_id Id of the machine register.
     * @return True, if the register is currently in use.
     */
    [[nodiscard]] bool is_live(mreg_id_t mreg_id) const noexcept;

    /**
     * Claims one register from the given list of free temporary registers.
     * In most cases, this will be the last one of the list.
     * However, some node types have dependencies to specific registers.
     * In this case, another register will be chosen.
     * The selected register will be removed from the list.
     *
     * @param spill_register_allocation List of free temporary registers.
     * @param type Type of the instruction that holds the virtual register.
     * @param value Value for the spill register
     * @param is_load True, if the value will be loaded into that spill register.
     * @return A free spill register.
     */
    [[nodiscard]] mreg_id_t claim_spill_mreg(SpillRegisterAllocation &spill_register_allocation, InstructionType type,
                                             std::variant<Register, Constant> value, bool is_load);

    /**
     * Tries to re-use a spill-register that holds the value of the given vreg.
     *
     * @param spill_register_allocation List of free temporary registers.
     * @param type Type of the instruction that holds the virtual register.
     * @param vreg Virtual register.
     * @param is_instruction_writing True, if the instruction will write to the register, will update the spill state.
     * @return The machine register that holds the value of the virtual register, if the mache register is free.
     */
    [[nodiscard]] std::optional<mreg_id_t> reuse_spill_mreg(SpillRegisterAllocation &spill_register_allocation,
                                                            InstructionType type, Register vreg,
                                                            bool is_instruction_writing);
    /**
     * Writes all dirty spill registers to the stack.
     */
    void flush_dirty_spill_regs(Program &program, InstructionSet &code, bool is_clear_state,
                                bool is_generate_inline_comment);

    /**
     * Flushes the given spill register to the stack, if it is dirty.
     *
     * @param program Program for allocating further instructions.
     * @param spill_mreg_id Spill register to flush.
     * @param is_clear_state If true, clear the spill register state.
     * @param is_generate_inline_comment If true, spill loads/writes will be generated with inline comments.
     * @return Instruction to flush the value, if dirty.
     */
    [[nodiscard]] std::optional<MovInstruction> flush_if_dirty(Program &program, mreg_id_t spill_mreg_id,
                                                               bool is_clear_state, bool is_generate_inline_comment);

    /**
     * Flushes the given spill register to the stack.
     *
     * @param program Program for allocating further instructions.
     * @param mreg_id Id of the spill register.
     * @param state The register state to flush.
     * @param is_clear_state If true, clear the spill register state.
     * @param is_generate_inline_comment If true, spill loads/writes will be generated with inline comments.
     * @return Mov instruction to flush the register.
     */
    [[nodiscard]] std::optional<MovInstruction> flush(Program &program, mreg_id_t mreg_id, SpillRegisterState &state,
                                                      bool is_clear_state, bool is_generate_inline_comment);

    /**
     * Generates instructions to save and restore the caller registers for a
     * call that is placed at the given line.
     *
     * @param program Program for allocating further nodes.
     * @param call_instruction Instruction that represents the call.
     * @return Code that replaces the call, including register saving and spill code.
     */
    [[nodiscard]] InstructionSet translate_function_call(Program &program, const FcallInstruction &instruction);

    /**
     * Translates the flounder div instruction to asm backed instructions
     * including register save/restore instructions for rax. The fdiv call
     * will be removed.
     *
     * @param program Program to allocate nodes.
     * @param instruction Fdiv or Fmod instruction to translate.
     */
    [[nodiscard]] InstructionSet translate_fdiv(Program &program, const InstructionInterface &instruction);

    /**
     * Generates and emits instructions to save the given registers on the stack (i.e., before call).
     *
     * @param program Program to generate instructions.
     * @param code Code to emit instructions.
     * @param registers_to_save Registers to save.
     * @return Stack offset.
     */
    [[nodiscard]] static std::uint16_t save_registers_on_stack(Program &program, InstructionSet &code,
                                                               const save_mreg_vector_t &registers_to_save);

    /**
     * Generates and emits instructions to restore the given registers from the stack (i.e., after call).
     *
     * @param program Program to generate instructions.
     * @param code Code to emit instructions.
     * @param registers_to_save Registers to save.
     * @param stack_offset Offset from saving instructions.
     */
    void static restore_registers_from_stack(Program &program, InstructionSet &code,
                                             const save_mreg_vector_t &registers_to_save, std::uint16_t stack_offset);
    /**
     * Returns a mem at node that accesses the stack at the given slot.
     *
     * @param program Program to generate nodes.
     * @param slot Slot to access.
     * @return Node that accesses the stack at the given slot
     */
    [[nodiscard]] static MemoryAddress access_stack(Program &program, const SpillSlot &slot,
                                                    const std::optional<std::uint16_t> offset = std::nullopt)
    {
        return program.mem(program.mreg64(ABI::stack_pointer_mreg_id()), slot.offset() + offset.value_or(0U),
                           slot.width());
    }

    /**
     * Creates a mov instruction that loads the value from the stack into the given spill register.
     *
     * @param program
     * @param vreg
     * @param stack_address
     * @param spill_register
     * @param is_generate_inline_comment
     * @return
     */
    [[nodiscard]] static MovInstruction load_from_stack(Program &program, Register vreg, MemoryAddress stack_address,
                                                        Register spill_register, const bool is_generate_inline_comment)
    {
        auto load = program.mov(spill_register, stack_address);

        if (is_generate_inline_comment) [[unlikely]]
        {
            load.inline_comment(fmt::format("RegSpill: Load {}", vreg.to_string()));
        }

        return load;
    }

    /**
     * Emphasizes, if a spilled (register) value needs to be loaded into a temporary
     * register. Some instructions can handle <mem> as operand (i.e., MOV <reg> <mem>).
     * In this case, we do not need to load the value into a temporary register but
     * can access the stack directly.
     *
     * @param instruction Instruction holding the VREG that was spilled.
     * @param index Index of the VREG within the instruction.
     * @return True, when the value does not need to be loaded from the stack.
     */
    [[nodiscard]] static bool can_use_spilled_value(const flounder::InstructionInterface &instruction,
                                                    const std::uint8_t index)
    {
        const auto type = instruction.type();

        if ((type == InstructionType::Mov && index > 0U) || (type == InstructionType::Cmovle && index > 0U) ||
            (type == InstructionType::Cmovge && index > 0U) || type == InstructionType::Or ||
            type == InstructionType::And || type == InstructionType::And || type == InstructionType::Cmp ||
            type == InstructionType::Add || type == InstructionType::Sub || type == InstructionType::GetArgument ||
            (type == InstructionType::Imul && index == 1U) || (type == InstructionType::Test && index == 0U))
        {
            const auto other_operand = instruction.operand(static_cast<std::uint8_t>(!static_cast<bool>(index)));
            return other_operand.is_reg() || other_operand.is_constant();
        }

        /// Unary types.
        if (type == InstructionType::Idiv)
        {
            return true;
        }

        return false;
    }

    /**
     * Emphasizes, if the given instruction will overwrite the child at the given index.
     * In this case, we do not need to load the value from the stack.
     *
     * @param instruction Instruction.
     * @param index Index of the child needed to be replaced from the stack.
     * @return True, if the instruction will write to the given child.
     */
    [[nodiscard]] static bool is_overwriting_value(const InstructionInterface &instruction,
                                                   const std::uint8_t index) noexcept
    {
        const auto type = instruction.type();

        return type == InstructionType::GetArgument ||
               ((type == InstructionType::Mov || type == InstructionType::Lea) && index == 0U) ||
               type == InstructionType::Sete || type == InstructionType::Setne;
    }

    [[nodiscard]] static std::optional<bool> is_flush_dirty_spill_regs(const InstructionType type) noexcept
    {
        if (type == InstructionType::Section)
        {
            return true;
        }

        if (type == InstructionType::Jump || type == InstructionType::Cmp || type == InstructionType::Test)
        {
            return false;
        }

        return std::nullopt;
    }
};
} // namespace flounder