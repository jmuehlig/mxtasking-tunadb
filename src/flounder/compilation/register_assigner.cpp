#include "register_assigner.h"
#include <ecpp/static_vector.hpp>

using namespace flounder;

void RegisterAssigner::process(Program &program, const bool generate_inline_comment)
{
    /// Clear states of spill register.
    for (const auto spill_mreg_id : ABI::spill_mreg_ids())
    {
        this->_spill_reg_state[spill_mreg_id] = SpillRegisterState{};
    }

    /// Allocate registers.
    this->_vreg_schedule = this->_register_allocator.allocate(program);

    /// Clear touched registers that are pushed and popped.
    this->_touched_registers = this->_vreg_schedule.used_machine_register_ids();
    this->_touched_registers.insert(ABI::stack_pointer_mreg_id());

    /// Machine registers that need to be saved on calls.
    this->_live_machine_registers.clear();

    /// Scan argument handler.
    auto arguments = this->assign(program, std::move(program.arguments()), generate_inline_comment);

    /// Scan header.
    auto header = this->assign(program, std::move(program.header()), generate_inline_comment);

    /// Scan body.
    auto body = this->assign(program, std::move(program.body()), generate_inline_comment);

    /// Save callee registers.
    auto prologue = InstructionSet{16U};
    auto epilogue = InstructionSet{16U};

    for (const auto machine_register_id : this->_touched_registers)
    {
        if (ABI::is_preserved_mreg(machine_register_id))
        {
            prologue << program.push(program.mreg64(machine_register_id));
            epilogue << program.pop(program.mreg64(machine_register_id));
        }
    }

    /// Reserve stack for register spilling.
    if (this->_vreg_schedule.max_stack_height() > 0U)
    {
        auto stack_size = std::max(16U, this->_vreg_schedule.max_stack_height() + 8U);

        /// Align to a base of 16.
        if (stack_size > 16U)
        {
            const auto mod = stack_size % 16U;
            stack_size += (16U - mod) * static_cast<std::uint16_t>(mod != 0U);
        }

        /// Sub stack pointer to have enough space on the stack.
        prologue << program.sub(program.mreg64(ABI::stack_pointer_mreg_id()), program.constant32(stack_size));

        /// Restore the stack pointer.
        epilogue << program.add(program.mreg64(ABI::stack_pointer_mreg_id()), program.constant32(stack_size));
    }

    /// Push the prologue and replace flounder by allocated flounder.
    arguments << std::make_pair(0U, std::move(prologue));
    program.arguments() = std::move(arguments);

    /// Replace header by allocated flounder.
    program.header() = std::move(header);

    /// Push the epilogue and replace flounder by allocated flounder.
    std::reverse(epilogue.lines().begin(), epilogue.lines().end());
    body << std::move(epilogue) << program.ret();
    program.body() = std::move(body);
}

InstructionSet RegisterAssigner::assign(Program &program, InstructionSet &&code, bool generate_inline_comment)
{
    auto allocated_code = InstructionSet{code.size()};

    for (const auto &instruction : code.lines())
    {
        if (std::holds_alternative<VregInstruction>(instruction))
        {
            const auto &vreg_instruction = std::get<VregInstruction>(instruction);

            const auto assigned_register = this->_vreg_schedule.schedule(vreg_instruction.vreg());
            if (assigned_register.has_value() && assigned_register->is_mreg())
            {
                this->_live_machine_registers.insert(assigned_register->mreg().machine_register_id().value());
            }
        }
        else if (std::holds_alternative<ClearInstruction>(instruction))
        {
            const auto &clear_instruction = std::get<ClearInstruction>(instruction);

            const auto assigned_register = this->_vreg_schedule.schedule(clear_instruction.vreg());
            if (assigned_register.has_value() && assigned_register->is_mreg())
            {
                this->_live_machine_registers.erase(assigned_register->mreg().machine_register_id().value());
            }
        }
        else if (std::holds_alternative<FdivInstruction>(instruction))
        {
            this->flush_dirty_spill_regs(program, allocated_code, true, generate_inline_comment);

            auto fdiv_code = this->translate_fdiv(program, std::get<FdivInstruction>(instruction));
            this->convey(program, std::move(fdiv_code), allocated_code, generate_inline_comment);
        }
        else if (std::holds_alternative<FmodInstruction>(instruction))
        {
            this->flush_dirty_spill_regs(program, allocated_code, true, generate_inline_comment);

            auto fdiv_code = this->translate_fdiv(program, std::get<FmodInstruction>(instruction));
            this->convey(program, std::move(fdiv_code), allocated_code, generate_inline_comment);
        }
        else if (std::holds_alternative<FcallInstruction>(instruction))
        {
            this->flush_dirty_spill_regs(program, allocated_code, true, generate_inline_comment);

            auto call_code = this->translate_function_call(program, std::get<FcallInstruction>(instruction));
            this->convey(program, std::move(call_code), allocated_code, generate_inline_comment);
        }
        else
        {
            if (std::holds_alternative<ShlInstruction>(instruction))
            {
                /// TODO: Would great to implement this in a more general way
                ///     (also other instructions than SHL may flush if dirty).
                auto store_instruction = this->flush_if_dirty(program, 1U, true, false);
                if (store_instruction.has_value())
                {
                    allocated_code << store_instruction.value();
                }
            }

            this->convey(program, instruction, allocated_code, generate_inline_comment);
        }
    }

    return allocated_code;
}

void RegisterAssigner::convey(flounder::Program &program, const flounder::Instruction &source,
                              flounder::InstructionSet &target, bool is_generate_inline_comment)
{
    auto instruction = source;
    if (is_generate_inline_comment) [[unlikely]]
    {
        std::visit([](auto &instr) { instr.inline_comment(instr.to_string()); }, instruction);
    }

    auto spill_register_allocation = SpillRegisterAllocation{};

    std::visit(
        [this, &program, &spill_register_allocation, is_generate_inline_comment, &target](auto &instr) {
            /// If the instruction indicates a jump or a section, dirty spill registers need to be flushed.
            const auto is_flush_dirty_spill_registers = RegisterAssigner::is_flush_dirty_spill_regs(instr.type());
            if (is_flush_dirty_spill_registers.has_value())
            {
                this->flush_dirty_spill_regs(program, target, is_flush_dirty_spill_registers.value(),
                                             is_generate_inline_comment);
            }

            /// Replace all virtual registers and constants by machine registers.
            if (instr.operands() > 0U)
            {
                this->replace_vreg_and_constant(program, instr, spill_register_allocation, target,
                                                is_generate_inline_comment);
            }
        },
        instruction);

    /// Convey the instruction.
    target.lines().emplace_back(std::move(instruction));
}

void RegisterAssigner::replace_vreg_and_constant(
    flounder::Program &program, flounder::InstructionInterface &instruction,
    flounder::RegisterAssigner::SpillRegisterAllocation &spill_register_allocation, flounder::InstructionSet &code,
    const bool is_generate_inline_comment)
{
    for (auto operand_index = 0U; operand_index < instruction.operands(); ++operand_index)
    {
        auto &operand = instruction.operand(operand_index).value().get();

        if (operand.is_reg() && operand.reg().is_virtual())
        {
            const auto machine_register_or_memory_address =
                this->unspill_vreg(program, instruction, operand_index, operand.reg(), spill_register_allocation, code,
                                   is_generate_inline_comment);
            if (std::holds_alternative<Register>(machine_register_or_memory_address))
            {
                operand.reg().assign(std::get<Register>(machine_register_or_memory_address));
            }
            else if (std::holds_alternative<MemoryAddress>(machine_register_or_memory_address))
            {
                operand = std::get<MemoryAddress>(machine_register_or_memory_address);
            }
        }
        else if (operand.is_mem())
        {
            /// Mem can hold a register or constant in the base.
            auto &mem = operand.mem();
            if (std::holds_alternative<Register>(mem.base()) && std::get<Register>(mem.base()).is_virtual())
            {
                auto &base_register = std::get<Register>(mem.base());
                const auto machine_register = this->unspill_vreg(program, base_register, spill_register_allocation,
                                                                 code, is_generate_inline_comment);
                base_register.assign(machine_register);
            }
            else if (std::holds_alternative<Constant>(mem.base()) &&
                     std::get<Constant>(mem.base()).width() == RegisterWidth::r64)
            {
                const auto machine_register =
                    this->unspill_constant(program, std::get<Constant>(mem.base()), spill_register_allocation, code,
                                           is_generate_inline_comment);
                mem.base() = machine_register;
            }

            /// If mem has an index, its a register.
            if (mem.index().has_value() && mem.index()->is_virtual())
            {
                auto &index_register = mem.index().value();
                const auto machine_register = this->unspill_vreg(program, index_register, spill_register_allocation,
                                                                 code, is_generate_inline_comment);
                index_register.assign(machine_register);
            }
        }
        else if (operand.is_constant() && operand.constant().width() == RegisterWidth::r64)
        {
            /// Replace the const.
            const auto machine_register = this->unspill_constant(program, operand.constant(), spill_register_allocation,
                                                                 code, is_generate_inline_comment);
            operand = Operand{machine_register};
        }
    }
}

std::variant<Register, MemoryAddress> RegisterAssigner::unspill_vreg(
    flounder::Program &program, const flounder::InstructionInterface &instruction, const std::uint8_t operand_index,
    Register vreg, RegisterAssigner::SpillRegisterAllocation &spill_register_allocation, flounder::InstructionSet &code,
    bool is_generate_inline_comment)
{
    const auto machine_register_or_spill = this->_vreg_schedule.schedule(vreg);
    if (machine_register_or_spill.has_value() == false) [[unlikely]]
    {
        throw CanNotFindVirtualRegisterException{vreg};
    }

    if (machine_register_or_spill->is_mreg())
    {
        return machine_register_or_spill->mreg();
    }

    const auto spill_slot = machine_register_or_spill->spill_slot();
    const auto is_overwriting = RegisterAssigner::is_overwriting_value(instruction, operand_index);
    const auto stack_address = RegisterAssigner::access_stack(program, spill_slot);

    /// The vreg may be used within the instruction.
    auto already_used_spill_mreg = spill_register_allocation.find(vreg);
    if (already_used_spill_mreg.has_value())
    {
        const auto [spill_mreg_id, is_load] = already_used_spill_mreg.value();
        const auto spill_register = program.mreg(
            spill_slot.width(), spill_slot.sign_type().value_or(RegisterSignType::Unsigned), spill_mreg_id);

        if (is_overwriting == false && is_load == false)
        {
            code << RegisterAssigner::load_from_stack(program, vreg, stack_address, spill_register,
                                                      is_generate_inline_comment);
        }

        return spill_register;
    }

    const auto is_instruction_writing = instruction.is_writing(operand_index);

    /// Maybe the vreg is still available within one of the spill registers from another instruction.
    auto reusable_spill_mreg_id =
        this->reuse_spill_mreg(spill_register_allocation, instruction.type(), vreg, is_instruction_writing);
    if (reusable_spill_mreg_id.has_value())
    {
        return program.mreg(spill_slot.width(), spill_slot.sign_type().value_or(RegisterSignType::Unsigned),
                            reusable_spill_mreg_id.value());
    }

    /// Maybe the instruction can use the spill address instead of the register.
    if (RegisterAssigner::can_use_spilled_value(instruction, operand_index))
    {
        return stack_address;
    }

    /// Hint the spill register allocation if the value will be loaded into that register.
    /// If the value will be needed by the instruction twice and the instruction is not loaded
    /// (i.e., mov %var, [%var + 42]), the instruction must be loaded.
    const auto is_load = is_overwriting == false;

    /// Need to load the spilled value into a spill register.
    const auto spill_mreg_id = this->claim_spill_mreg(spill_register_allocation, instruction.type(), vreg, is_load);
    const auto spill_register =
        program.mreg(spill_slot.width(), spill_slot.sign_type().value_or(RegisterSignType::Unsigned), spill_mreg_id);

    /// Flush the vreg that was in the spill register.
    auto flush_old_value = this->flush_if_dirty(program, spill_mreg_id, false, is_generate_inline_comment);
    if (flush_old_value.has_value())
    {
        code << std::move(flush_old_value.value());
    }

    /// Since some instructions overwrites the value in the register,
    /// there is no need to restore them from the stack.
    if (is_load)
    {
        code << RegisterAssigner::load_from_stack(program, vreg, stack_address, spill_register,
                                                  is_generate_inline_comment);
    }

    this->_spill_reg_state[spill_mreg_id] = SpillRegisterState{vreg, is_instruction_writing};

    return spill_register;
}

Register RegisterAssigner::unspill_vreg(flounder::Program &program, flounder::Register vreg,
                                        flounder::RegisterAssigner::SpillRegisterAllocation &spill_register_allocation,
                                        flounder::InstructionSet &code, const bool is_generate_inline_comment)
{
    const auto machine_register_or_spill = this->_vreg_schedule.schedule(vreg);
    if (machine_register_or_spill.has_value() == false) [[unlikely]]
    {
        throw CanNotFindVirtualRegisterException{vreg};
    }

    if (machine_register_or_spill->is_mreg())
    {
        return machine_register_or_spill->mreg();
    }

    const auto spill_slot = machine_register_or_spill->spill_slot();

    /// This is the address where the value is spilled on the stack.
    const auto stack_address = RegisterAssigner::access_stack(program, spill_slot);

    /// The vreg may be used within the instruction.
    auto already_used_spill_mreg = spill_register_allocation.find(vreg);
    if (already_used_spill_mreg.has_value())
    {
        const auto [spill_mreg_id, is_load] = already_used_spill_mreg.value();
        const auto spill_register = program.mreg(
            spill_slot.width(), spill_slot.sign_type().value_or(RegisterSignType::Unsigned), spill_mreg_id);

        if (is_load == false)
        {
            code << RegisterAssigner::load_from_stack(program, vreg, stack_address, spill_register,
                                                      is_generate_inline_comment);
        }

        return spill_register;
    }

    /// Maybe the vreg is available within one of the spill registers.
    auto reusable_spill_mreg_id = this->reuse_spill_mreg(spill_register_allocation, InstructionType::Mov, vreg, false);
    if (reusable_spill_mreg_id.has_value())
    {
        return program.mreg(spill_slot.width(), spill_slot.sign_type().value_or(RegisterSignType::Unsigned),
                            reusable_spill_mreg_id.value());
    }

    /// Need to load the spilled value into a spill register.
    const auto spill_mreg_id = this->claim_spill_mreg(spill_register_allocation, InstructionType::Mov, vreg, true);
    const auto spill_register =
        program.mreg(spill_slot.width(), spill_slot.sign_type().value_or(RegisterSignType::Unsigned), spill_mreg_id);

    this->_touched_registers.insert(spill_mreg_id);

    /// Flush the vreg that was in the spill register.
    auto flush_old_value = this->flush_if_dirty(program, spill_mreg_id, false, is_generate_inline_comment);
    if (flush_old_value.has_value())
    {
        code << std::move(flush_old_value.value());
    }

    /// Load the value from stack.
    code << RegisterAssigner::load_from_stack(program, vreg, stack_address, spill_register, is_generate_inline_comment);

    this->_spill_reg_state[spill_mreg_id] = SpillRegisterState{vreg, false};

    return spill_register;
}

Register RegisterAssigner::unspill_constant(
    flounder::Program &program, const flounder::Constant constant,
    flounder::RegisterAssigner::SpillRegisterAllocation &spill_register_allocation, flounder::InstructionSet &code,
    const bool is_generate_inline_comment)
{
    const auto spill_mreg_id = this->claim_spill_mreg(spill_register_allocation, InstructionType::Mov, constant, true);
    const auto spill_register = program.mreg(constant.width(), RegisterSignType::Signed, spill_mreg_id);

    this->_touched_registers.insert(spill_mreg_id);

    /// Flush the vreg that was in the spill register.
    auto flush_old_value = this->flush_if_dirty(program, spill_mreg_id, true, is_generate_inline_comment);
    if (flush_old_value.has_value())
    {
        code << std::move(flush_old_value.value());
    }

    code << program.mov(spill_register, constant);

    return spill_register;
}

RegisterAssigner::mreg_id_t RegisterAssigner::claim_spill_mreg(
    RegisterAssigner::SpillRegisterAllocation &spill_register_allocation, const InstructionType type,
    std::variant<Register, Constant> value, const bool is_load)
{
    if (spill_register_allocation.full() == false && ABI::has_mreg_dependency(type) == false)
    {
        const auto spill_mreg_ids = spill_register_allocation.mreg_ids();

        /// Try to use an empty spill register.
        for (const auto &[mreg, state] : this->_spill_reg_state)
        {
            if (state.empty())
            {
                if (spill_register_allocation.is_free(mreg))
                {
                    spill_register_allocation.allocate(mreg, value, is_load);
                    return mreg;
                }
            }
        }

        /// Try to use a non-dirty spill register.
        for (const auto &[mreg, state] : this->_spill_reg_state)
        {
            if (state.is_dirty() == false)
            {
                if (spill_register_allocation.is_free(mreg))
                {
                    spill_register_allocation.allocate(mreg, value, is_load);
                    return mreg;
                }
            }
        }

        const auto allocated_mreg_id = spill_register_allocation.allocate(value, is_load);
        if (allocated_mreg_id.has_value())
        {
            return allocated_mreg_id.value();
        }
    }

    auto register_dependencies = ABI::mreg_dependencies(type);
    if (register_dependencies.has_value())
    {
        for (const auto mreg_id : spill_register_allocation.mreg_ids())
        {
            if (spill_register_allocation.is_free(mreg_id) && register_dependencies.value().contains(mreg_id) == false)
            {
                spill_register_allocation.allocate(mreg_id, value, is_load);
                return mreg_id;
            }
        }
    }

    throw NotEnoughTemporaryRegistersException{};
}

std::optional<RegisterAssigner::mreg_id_t> RegisterAssigner::reuse_spill_mreg(
    RegisterAssigner::SpillRegisterAllocation &spill_register_allocation, const flounder::InstructionType /*type*/,
    const flounder::Register vreg, const bool is_instruction_writing)
{
    for (auto &[mreg_id, state] : this->_spill_reg_state)
    {
        /// Check if any spill register actually holds the variable.
        if (state.holds(vreg))
        {
            /// If the spill register is free within the instruction-scope:
            /// Take it.
            if (spill_register_allocation.is_free(mreg_id))
            {
                spill_register_allocation.allocate(mreg_id, vreg, true);
                state.is_dirty(state.is_dirty() || is_instruction_writing);
                return mreg_id;
            }
        }
    }

    return std::nullopt;
}

bool RegisterAssigner::is_live(const mreg_id_t mreg_id) const noexcept
{
    if (this->_live_machine_registers.contains(mreg_id))
    {
        return true;
    }

    if (auto iterator = this->_spill_reg_state.find(mreg_id); iterator != this->_spill_reg_state.end())
    {
        return iterator->second.vreg().has_value() && iterator->second.is_dirty();
    }

    return false;
}

void RegisterAssigner::flush_dirty_spill_regs(Program &program, InstructionSet &code, const bool is_clear_state,
                                              const bool is_generate_inline_comment)
{
    for (auto &[mreg_id, spill_state] : this->_spill_reg_state)
    {
        if (spill_state.is_dirty())
        {
            auto store = this->flush(program, mreg_id, spill_state, is_clear_state, is_generate_inline_comment);
            if (store.has_value())
            {
                code << std::move(store.value());
            }
        }
        else if (is_clear_state)
        {
            spill_state = SpillRegisterState{};
        }
    }
}

std::optional<MovInstruction> RegisterAssigner::flush_if_dirty(flounder::Program &program,
                                                               flounder::RegisterAssigner::mreg_id_t spill_mreg_id,
                                                               const bool is_clear_state,
                                                               const bool is_generate_inline_comment)
{
    if (auto iterator = this->_spill_reg_state.find(spill_mreg_id); iterator != this->_spill_reg_state.end())
    {
        if (iterator->second.is_dirty())
        {
            return this->flush(program, iterator->first, iterator->second, is_clear_state, is_generate_inline_comment);
        }

        if (is_clear_state)
        {
            iterator->second = SpillRegisterState{};
        }
    }

    return std::nullopt;
}

std::optional<MovInstruction> RegisterAssigner::flush(flounder::Program &program, const mreg_id_t mreg_id,
                                                      flounder::RegisterAssigner::SpillRegisterState &state,
                                                      const bool is_clear_state, const bool is_generate_inline_comment)
{
    const auto allocation = this->_vreg_schedule.schedule(state.vreg().value());
    if (allocation.has_value() && allocation->is_spill())
    {
        const auto &spill_slot = allocation->spill_slot();

        const auto spill_register =
            program.mreg(spill_slot.width(), spill_slot.sign_type().value_or(RegisterSignType::Unsigned), mreg_id);

        /// This is the address where the value is spilled on the stack.
        const auto stack_address = RegisterAssigner::access_stack(program, spill_slot);

        auto store = program.mov(stack_address, spill_register);

        if (is_generate_inline_comment) [[unlikely]]
        {
            store.inline_comment(fmt::format("RegSpill: Flush {}", state.vreg()->to_string()));
        }

        if (is_clear_state)
        {
            state = SpillRegisterState{};
        }
        else
        {
            state.is_dirty(false);
        }

        return std::move(store);
    }

    return std::nullopt;
}

InstructionSet RegisterAssigner::translate_function_call(flounder::Program &program,
                                                         const flounder::FcallInstruction &instruction)
{
    /// Push all preserved registers before call.
    for (const auto mreg_id : ABI::available_mreg_ids())
    {
        if (ABI::is_preserved_mreg(mreg_id))
        {
            this->_touched_registers.insert(mreg_id);
        }
    }

    /**
     * The call[ret] instruction will be replaced by a stack of wider instructions:
     *  - Instructions to save live registers on the stack
     *  - Instructions moving arguments to registers.
     *  - Instruction to move the stack pointer to a save region for the callee.
     *  - Simple call instruction containing just the fuction address.
     *  - Instruction to move the stack pointer back.
     *  - Instruction to get the return argument, if callret
     *  - Instructions to restore the saved registers.
     */

    auto code = InstructionSet{64U};

    /// Get all registers to save (scratch registers that are in use).
    constexpr auto available_mreg_ids = ABI::available_mreg_ids();
    auto mreg_ids_to_save = save_mreg_vector_t{};
    for (const auto mreg_id : available_mreg_ids)
    {
        /// Only save registers that need to be saved by the callee and are in use.
        if (ABI::is_scratch_mreg(mreg_id) && this->is_live(mreg_id))
        {
            mreg_ids_to_save.emplace_back(mreg_id);
        }
    }
    /// Detect the register used for return value (if any). This register does not need to be saved.
    if (instruction.has_return())
    {
        auto return_vreg = instruction.return_register()->reg();
        if (return_vreg.is_virtual())
        {
            const auto return_vreg_allocation = this->_vreg_schedule.schedule(return_vreg);
            if (return_vreg_allocation.has_value() && return_vreg_allocation->is_mreg())
            {
                mreg_ids_to_save.erase(std::remove(mreg_ids_to_save.begin(), mreg_ids_to_save.end(),
                                                   return_vreg_allocation->mreg().machine_register_id().value()),
                                       mreg_ids_to_save.end());
            }
        }
    }

    /// Create save registers on the stack.
    const auto stack_offset = RegisterAssigner::save_registers_on_stack(program, code, mreg_ids_to_save);

    /// Notice all registers that are used for function call arguments.
    auto argument_register_in_use = ecpp::static_vector<mreg_id_t, ABI::call_argument_register_ids().max_size()>{};

    /// Set argument registers for the function call.
    for (auto argument_index = 0U; argument_index < instruction.arguments().size(); ++argument_index)
    {
        auto argument = instruction.arguments()[argument_index];
        if (argument.is_reg() && argument.reg().is_virtual())
        {
            const auto call_argument_mreg_id = ABI::call_argument_register_ids()[argument_index];
            const auto call_argument_mreg = program.mreg64(call_argument_mreg_id);
            this->_touched_registers.insert(call_argument_mreg_id);

            auto argument_vreg = argument.reg();

            /// Check, if the value is present in a live register.
            const auto argument_allocation = this->_vreg_schedule.schedule(argument_vreg);
            if (argument_allocation.has_value())
            {
                if (argument_allocation->is_mreg())
                {
                    /// Skip if the register is already the function call register (z.B. rsi = rsi).
                    if (argument_allocation->mreg() == call_argument_mreg)
                    {
                        continue;
                    }

                    auto argument_operand = Operand{argument_allocation->mreg()};

                    /// Check if the machine register of the virtual register is already used by another argument.
                    /// Example:    call %a %b     with vreg_to_mreg_map: [%a] r9, [%b] = rdi
                    ///             will result in  call rdi rsi but %b was in rdi!
                    const auto is_argument_register_used_for_other_arguments =
                        std::find(argument_register_in_use.begin(), argument_register_in_use.end(),
                                  argument_allocation->mreg().machine_register_id().value()) !=
                        argument_register_in_use.end();
                    if (is_argument_register_used_for_other_arguments)
                    {
                        /// In this case the value is hold in a register that is overwritten
                        /// by the arguments before.
                        /// If so, the register was moved to the stack before.
                        /// Therefore, we need to load it from the stack.
                        auto *saved_argument_mreg_iterator =
                            std::find(mreg_ids_to_save.begin(), mreg_ids_to_save.end(),
                                      argument_allocation->mreg().machine_register_id().value());
                        if (saved_argument_mreg_iterator == mreg_ids_to_save.end())
                        {
                            throw CanNotFindSpilledValueException{argument_vreg};
                        }

                        /// Instead of using the arguments mreg, we access the stack saved before.
                        const auto argument_stack_index =
                            std::distance(mreg_ids_to_save.begin(), saved_argument_mreg_iterator);
                        auto argument_stack_offset = (argument_stack_index + 1U) * 8U;
                        argument_operand = program.mem(program.mreg64(ABI::stack_pointer_mreg_id()),
                                                       (argument_stack_offset * -1) + stack_offset);

                        /// The mov will be placed before the rsp sub.
                        if (argument_allocation->mreg().width().value() != RegisterWidth::r64)
                        {
                            /// The argument is loaded into a 64bit register, but the argument is not 64bit.
                            /// Reset the 64bit register to 0 and load the value.
                            code << program.xor_(call_argument_mreg, call_argument_mreg);
                        }
                    }

                    code << program.mov(call_argument_mreg, argument_operand);
                }
                else
                {
                    const auto &spill_slot = argument_allocation->spill_slot();

                    if (spill_slot.width() != RegisterWidth::r64)
                    {
                        /// The argument is loaded into a 64bit register, but the argument is not 64bit.
                        /// Reset the 64bit register to 0 and load the value.
                        code << program.xor_(call_argument_mreg, call_argument_mreg);
                    }

                    code << program.mov(call_argument_mreg,
                                        RegisterAssigner::access_stack(program, spill_slot, stack_offset));
                }
            }
            else
            {
                throw CanNotFindVirtualRegisterException{argument_vreg};
            }

            argument_register_in_use.emplace_back(call_argument_mreg.machine_register_id().value());
        }
    }

    /// Place the real call node without any parameter and return value.
    code << program.call(instruction.function_pointer());

    /// Restore registers saved on the stack.
    RegisterAssigner::restore_registers_from_stack(program, code, mreg_ids_to_save, stack_offset);

    /// Read the return value, if any. The mov instruction will be placed
    /// within the restore instructions code to firstly resize the stack before
    /// accessing spilled code.
    if (instruction.has_return())
    {
        auto return_vreg = instruction.return_register()->reg();

        if (return_vreg.is_virtual())
        {
            this->_touched_registers.insert(ABI::call_return_register_id());

            const auto return_allocation = this->_vreg_schedule.schedule(return_vreg);
            if (return_allocation.has_value())
            {
                if (return_allocation->is_mreg())
                {
                    code << program.mov(return_allocation->mreg(), program.mreg64(ABI::call_return_register_id()));
                }
                else
                {
                    code << program.mov(RegisterAssigner::access_stack(program, return_allocation->spill_slot()),
                                        program.mreg64(ABI::call_return_register_id()));
                }
            }
            else
            {
                throw CanNotFindVirtualRegisterException{return_vreg};
            }
        }
    }

    return code;
}

InstructionSet RegisterAssigner::translate_fdiv(flounder::Program &program,
                                                const flounder::InstructionInterface &instruction)
{
    const auto target_operand = instruction.operand(0U);
    const auto dividend_operand = instruction.operand(1U);
    auto divisor_operand = instruction.operand(2U);

    auto code = InstructionSet{16U};

    /// Spill registers to save/restore, used for fdiv.
    auto registers_to_save = save_mreg_vector_t{};
    if (this->is_live(0U)) [[unlikely]]
    {
        registers_to_save.emplace_back(0U);
    }
    if (this->is_live(2U)) [[unlikely]]
    {
        registers_to_save.emplace_back(2U);
    }

    /// Push registers to the stack.
    const auto stack_offset = RegisterAssigner::save_registers_on_stack(program, code, registers_to_save);

    /// Load the divisor into spill register, if constant.
    if (divisor_operand.is_constant())
    {
        auto divisor_reg =
            program.mreg(divisor_operand.constant().width(), RegisterSignType::Signed, ABI::spill_mreg_ids().front());
        code << program.mov(divisor_reg, divisor_operand.constant());
        divisor_operand = Operand{divisor_reg};
    }

    this->_touched_registers.insert(0U);
    this->_touched_registers.insert(2U);

    /// Return register ( for div, for mod)
    const auto return_reg = instruction.type() == InstructionType::Fdiv ? program.mreg64(0U) : program.mreg64(2U);

    /// Generate instructions for div.
    code << program.xor_(program.mreg64(2U), program.mreg64(2U)) << program.mov(program.mreg64(0U), dividend_operand)
         << program.cqo() << program.idiv(divisor_operand) << program.mov(target_operand, return_reg);

    /// Restore the stack.
    RegisterAssigner::restore_registers_from_stack(program, code, registers_to_save, stack_offset);

    return code;
}

std::uint16_t RegisterAssigner::save_registers_on_stack(
    flounder::Program &program, flounder::InstructionSet &code,
    const flounder::RegisterAssigner::save_mreg_vector_t &registers_to_save)
{
    if (registers_to_save.empty() == false)
    {
        auto stack_offset = 8U;

        for (const auto mreg_id : registers_to_save)
        {
            auto stack_target = program.mem(program.mreg64(ABI::stack_pointer_mreg_id()), stack_offset * -1);
            code << program.mov(stack_target, program.mreg64(mreg_id));
            stack_offset += 8;
        }
        stack_offset += stack_offset % 16U;
        code << program.sub(program.mreg64(ABI::stack_pointer_mreg_id()), program.constant16(stack_offset));

        return stack_offset;
    }

    return 0U;
}

void RegisterAssigner::restore_registers_from_stack(
    flounder::Program &program, flounder::InstructionSet &code,
    const flounder::RegisterAssigner::save_mreg_vector_t &registers_to_save, std::uint16_t stack_offset)
{
    if (registers_to_save.empty() == false)
    {
        code << program.add(program.mreg64(ABI::stack_pointer_mreg_id()), program.constant16(stack_offset));
        auto restore_stack_offset = 8;
        for (const auto mreg_id : registers_to_save)
        {
            auto stack_source = program.mem(program.mreg64(ABI::stack_pointer_mreg_id()), restore_stack_offset * -1);
            code << program.mov(program.mreg64(mreg_id), stack_source);
            restore_stack_offset += 8;
        }
    }
}