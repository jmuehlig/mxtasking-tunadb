#include "translator.h"
#include "flounder/ir/register.h"
#include <cmath>
#include <flounder/abi/x86_64.h>
#include <flounder/exception.h>

using namespace flounder;

OperandTranslator::OperandTranslator()
{
    _registers[RegisterWidth::r8] = {{asmjit::x86::al, asmjit::x86::cl, asmjit::x86::dl, asmjit::x86::bl,
                                      asmjit::x86::spl, asmjit::x86::bpl, asmjit::x86::sil, asmjit::x86::dil,
                                      asmjit::x86::r8b, asmjit::x86::r9b, asmjit::x86::r10b, asmjit::x86::r11b,
                                      asmjit::x86::r12b, asmjit::x86::r13b, asmjit::x86::r14b, asmjit::x86::r15b}};
    _registers[RegisterWidth::r16] = {{asmjit::x86::ax, asmjit::x86::cx, asmjit::x86::dx, asmjit::x86::bx,
                                       asmjit::x86::sp, asmjit::x86::bp, asmjit::x86::si, asmjit::x86::di,
                                       asmjit::x86::r8w, asmjit::x86::r9w, asmjit::x86::r10w, asmjit::x86::r11w,
                                       asmjit::x86::r12w, asmjit::x86::r13w, asmjit::x86::r14w, asmjit::x86::r15w}};
    _registers[RegisterWidth::r32] = {{asmjit::x86::eax, asmjit::x86::ecx, asmjit::x86::edx, asmjit::x86::ebx,
                                       asmjit::x86::esp, asmjit::x86::ebp, asmjit::x86::esi, asmjit::x86::edi,
                                       asmjit::x86::r8d, asmjit::x86::r9d, asmjit::x86::r10d, asmjit::x86::r11d,
                                       asmjit::x86::r12d, asmjit::x86::r13d, asmjit::x86::r14d, asmjit::x86::r15d}};
    _registers[RegisterWidth::r64] = {{asmjit::x86::rax, asmjit::x86::rcx, asmjit::x86::rdx, asmjit::x86::rbx,
                                       asmjit::x86::rsp, asmjit::x86::rbp, asmjit::x86::rsi, asmjit::x86::rdi,
                                       asmjit::x86::r8, asmjit::x86::r9, asmjit::x86::r10, asmjit::x86::r11,
                                       asmjit::x86::r12, asmjit::x86::r13, asmjit::x86::r14, asmjit::x86::r15}};
}

asmjit::x86::Mem OperandTranslator::translate(const MemoryAddress mem, std::optional<RegisterWidth> access_width)
{
    if (access_width.has_value() == false)
    {
        access_width = mem.width();
    }

    /// [reg]
    if (std::holds_alternative<Register>(mem.base()))
    {
        const auto base = this->translate(std::get<Register>(mem.base()));

        if (mem.has_index() == false && mem.has_scale() == false)
        {
            if (access_width.has_value())
            {
                switch (access_width.value())
                {
                case RegisterWidth::r8:
                    return asmjit::x86::byte_ptr(base, mem.displacement());
                case RegisterWidth::r16:
                    return asmjit::x86::word_ptr(base, mem.displacement());
                case RegisterWidth::r32:
                    return asmjit::x86::dword_ptr(base, mem.displacement());
                case RegisterWidth::r64:
                    return asmjit::x86::qword_ptr(base, mem.displacement());
                }
            }

            return asmjit::x86::ptr(base, mem.displacement());
        }

        /// [rax+rbx(*4+1337)]
        if (mem.has_index())
        {
            const auto index = this->translate(mem.index().value());
            auto shift = 0U;
            if (mem.has_scale())
            {
                shift = mem.scale() == 2U ? 1U : (mem.scale() == 4U ? 2U : (mem.scale() == 8U ? 3U : 0U));
            }

            if (access_width.has_value())
            {
                switch (access_width.value())
                {
                case RegisterWidth::r8:
                    return asmjit::x86::byte_ptr(base, index, shift, mem.displacement());
                case RegisterWidth::r16:
                    return asmjit::x86::word_ptr(base, index, shift, mem.displacement());
                case RegisterWidth::r32:
                    return asmjit::x86::dword_ptr(base, index, shift, mem.displacement());
                case RegisterWidth::r64:
                    return asmjit::x86::qword_ptr(base, index, shift, mem.displacement());
                }
            }

            return asmjit::x86::ptr(base, index, shift, mem.displacement());
        }
    }

    /// [1337]
    else if (std::holds_alternative<Constant>(mem.base()))
    {
        const auto base = std::get<Constant>(mem.base()).value_as_int64() + mem.displacement();

        if (mem.has_index() == false && mem.has_scale() == false)
        {
            if (access_width.has_value())
            {
                switch (access_width.value())
                {
                case RegisterWidth::r8:
                    return asmjit::x86::byte_ptr(base);
                case RegisterWidth::r16:
                    return asmjit::x86::word_ptr(base);
                case RegisterWidth::r32:
                    return asmjit::x86::dword_ptr(base);
                case RegisterWidth::r64:
                    return asmjit::x86::qword_ptr(base);
                }
            }

            return asmjit::x86::ptr(base);
        }
    }

    throw CanNotTranslateOperandException{Operand{mem}};
}

asmjit::Label OperandTranslator::translate(Label label, asmjit::x86::Assembler &assembler, const bool is_external)
{
    auto iterator = this->_labels.find(label.label());
    if (iterator == this->_labels.end())
    {
        if (is_external == false)
        {
            auto inserted = this->_labels.insert(std::make_pair(label.label(), assembler.newLabel()));
            return std::get<0>(inserted)->second;
        }

        auto inserted = this->_labels.insert(
            std::make_pair(label.label(), assembler.newExternalLabel(label.label().data(), label.label().size())));
        return std::get<0>(inserted)->second;
    }

    return iterator->second;
}

bool InstructionTranslator::translate(flounder::VregInstruction &instruction)
{
    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::ClearInstruction &instruction)
{
    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::GetArgumentInstruction &instruction)
{
    auto mov =
        MovInstruction{instruction.operand(),
                       Operand{Register{ABI::call_argument_register_ids()[instruction.index()], RegisterWidth::r64}}};
    return this->translate(mov);
}

bool InstructionTranslator::translate(flounder::SetReturnArgumentInstruction &instruction)
{
    auto mov =
        MovInstruction{Operand{Register{ABI::call_return_register_id(), RegisterWidth::r64, RegisterSignType::Signed}},
                       instruction.operand()};
    return this->translate(mov);
}

bool InstructionTranslator::translate(flounder::CommentInstruction &instruction)
{
    if (this->is_keep_comment()) [[unlikely]]
    {
        this->_assembler.comment(instruction.text().c_str(), instruction.text().size());
    }

    return false;
}

bool InstructionTranslator::translate(flounder::ContextBeginInstruction &instruction)
{
    if (this->_logger != nullptr) [[unlikely]]
    {
        this->_logger->begin_context(instruction.name());
    }

    return false;
}

bool InstructionTranslator::translate(flounder::ContextEndInstruction &instruction)
{
    if (this->_logger != nullptr) [[unlikely]]
    {
        this->_logger->end_context(instruction.name());
    }

    return false;
}

bool InstructionTranslator::translate(flounder::BranchBeginInstruction & /*instruction*/)
{
    return false;
}

bool InstructionTranslator::translate(flounder::BranchEndInstruction & /*instruction*/)
{
    return false;
}

bool InstructionTranslator::translate(flounder::RetInstruction & /*instruction*/)
{
    this->_assembler.ret();
    return true;
}

bool InstructionTranslator::translate(flounder::NopInstruction & /*instruction*/)
{
    this->_assembler.nop();
    return true;
}

bool InstructionTranslator::translate(flounder::CqoInstruction & /*instruction*/)
{
    this->_assembler.cqo();
    return true;
}

bool InstructionTranslator::translate(flounder::PopInstruction &instruction)
{
    this->_assembler.pop(this->_operand_translator.translate(instruction.reg()));
    return true;
}

bool InstructionTranslator::translate(flounder::PushInstruction &instruction)
{
    this->_assembler.push(this->_operand_translator.translate(instruction.reg()));
    return true;
}

bool InstructionTranslator::translate(flounder::JumpInstruction &instruction)
{
    auto label = this->_operand_translator.translate(instruction.label(), this->_assembler, false);

    switch (instruction.jump_type())
    {
    case JumpInstruction::Type::JMP:
        this->_assembler.jmp(label);
        break;
    case JumpInstruction::Type::JE:
        this->_assembler.je(label);
        break;
    case JumpInstruction::Type::JNE:
        this->_assembler.jne(label);
        break;
    case JumpInstruction::Type::JLE:
        this->_assembler.jle(label);
        break;
    case JumpInstruction::Type::JL:
        this->_assembler.jl(label);
        break;
    case JumpInstruction::Type::JGE:
        this->_assembler.jge(label);
        break;
    case JumpInstruction::Type::JG:
        this->_assembler.jg(label);
        break;
    case JumpInstruction::Type::JZ:
        this->_assembler.jz(label);
        break;
    case JumpInstruction::Type::JNZ:
        this->_assembler.jnz(label);
        break;
    case JumpInstruction::Type::JB:
        this->_assembler.jb(label);
        break;
    case JumpInstruction::Type::JBE:
        this->_assembler.jbe(label);
        break;
    case JumpInstruction::Type::JA:
        this->_assembler.ja(label);
        break;
    case JumpInstruction::Type::JAE:
        this->_assembler.jae(label);
        break;
    }

    return true;
}

bool InstructionTranslator::translate(flounder::SectionInstruction &instruction)
{
    auto label = this->_operand_translator.translate(instruction.label(), this->_assembler, false);
    this->_assembler.bind(label);

    return true;
}

bool InstructionTranslator::translate(flounder::IncInstruction &instruction)
{
    if (instruction.operand().is_reg())
    {
        this->_assembler.inc(this->_operand_translator.translate(instruction.operand().reg()));
        return true;
    }

    if (instruction.operand().is_mem())
    {
        this->_assembler.inc(this->_operand_translator.translate(instruction.operand().mem()));
        return true;
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::DecInstruction &instruction)
{
    if (instruction.operand().is_reg())
    {
        this->_assembler.dec(this->_operand_translator.translate(instruction.operand().reg()));
        return true;
    }

    if (instruction.operand().is_mem())
    {
        this->_assembler.dec(this->_operand_translator.translate(instruction.operand().mem()));
        return true;
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::SeteInstruction &instruction)
{
    if (instruction.operand().is_reg())
    {
        this->_assembler.sete(this->_operand_translator.translate(instruction.operand().reg()));
        return true;
    }

    if (instruction.operand().is_mem())
    {
        this->_assembler.sete(this->_operand_translator.translate(instruction.operand().mem()));
        return true;
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::SetneInstruction &instruction)
{
    if (instruction.operand().is_reg())
    {
        this->_assembler.setne(this->_operand_translator.translate(instruction.operand().reg()));
        return true;
    }

    if (instruction.operand().is_mem())
    {
        this->_assembler.setne(this->_operand_translator.translate(instruction.operand().mem()));
        return true;
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::PrefetchInstruction &instruction)
{
    if (instruction.operand().is_mem())
    {
        this->_assembler.prefetcht1(this->_operand_translator.translate(instruction.operand().mem()));
        return true;
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::IdivInstruction &instruction)
{
    if (instruction.operand().is_reg())
    {
        this->_assembler.idiv(this->_operand_translator.translate(instruction.operand().reg()));
        return true;
    }

    if (instruction.operand().is_mem())
    {
        this->_assembler.idiv(this->_operand_translator.translate(instruction.operand().mem()));
        return true;
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::CmpInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());

        /// cmp reg, reg
        if (right.is_reg())
        {
            this->_assembler.cmp(left_reg, this->_operand_translator.translate(right.reg()));
            return true;
        }

        /// cmp reg, [mem]
        if (right.is_mem())
        {
            this->_assembler.cmp(left_reg, this->_operand_translator.translate(
                                               right.mem(), right.mem().width().value_or(left.reg().width().value())));
            return true;
        }

        /// cmp reg, cons
        if (right.is_constant())
        {
            this->_assembler.cmp(left_reg, right.constant().value_as_int64());
            return true;
        }
    }

    if (left.is_mem())
    {
        /// cmp [mem], reg
        if (right.is_reg())
        {
            const auto mem_width = left.mem().width().value_or(right.reg().width().value());

            this->_assembler.cmp(this->_operand_translator.translate(left.mem(), mem_width),
                                 this->_operand_translator.translate(right.reg()));
            return true;
        }

        /// cmp [mem], cons
        if (right.is_constant())
        {
            const auto mem_width = left.mem().width().value_or(right.constant().width());
            this->_assembler.cmp(this->_operand_translator.translate(left.mem(), mem_width),
                                 right.constant().value_as_int64());
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::TestInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());

        /// test reg, reg
        if (right.is_reg())
        {
            this->_assembler.test(left_reg, this->_operand_translator.translate(right.reg()));
            return true;
        }

        /// test reg, cons
        if (right.is_constant())
        {
            this->_assembler.test(left_reg, right.constant().value_as_int64());
            return true;
        }
    }

    if (left.is_mem())
    {
        /// test [mem], reg
        if (right.is_reg())
        {
            const auto mem_width = left.mem().width().value_or(right.reg().width().value());

            this->_assembler.test(this->_operand_translator.translate(left.mem(), mem_width),
                                  this->_operand_translator.translate(right.reg()));
            return true;
        }

        /// test [mem], cons
        if (right.is_constant())
        {
            const auto mem_width = left.mem().width().value_or(right.constant().width());
            this->_assembler.test(this->_operand_translator.translate(left.mem(), mem_width),
                                  right.constant().value_as_int64());
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::MovInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();
        const auto left_sign_type = left.reg().sign_type().value();

        /// mov reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());
            const auto right_width = right.reg().width().value();

            if (left_width == right_width)
            {
                this->_assembler.mov(left_reg, right_reg);
                return true;
            }

            if (left_width < right_width)
            {
                if (left_sign_type == RegisterSignType::Signed)
                {
                    this->_assembler.movsx(left_reg, right_reg);
                    return true;
                }

                this->_assembler.mov(left_reg, right_reg);
                return true;
            }

            if (right_width < left_width)
            {
                if (left_sign_type == RegisterSignType::Signed)
                {
                    if (right_width == RegisterWidth::r32)
                    {
                        this->_assembler.movsxd(left_reg, right_reg);
                        return true;
                    }

                    this->_assembler.movsx(left_reg, right_reg);
                    return true;
                }

                if (right_width == RegisterWidth::r32)
                {
                    this->_assembler.mov(left_reg, right_reg);
                    return true;
                }

                this->_assembler.movzx(left_reg, right_reg);
                return true;
            }
        }

        /// mov reg, cons
        if (right.is_constant())
        {
            if (left_width == RegisterWidth::r64)
            {
                this->_assembler.movabs(left_reg, right.constant().value_as_int64());
                return true;
            }

            this->_assembler.mov(left_reg, right.constant().value_as_int64());
            return true;
        }

        /// mov reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            if (left_width == right_width || left_width < right_width)
            {
                this->_assembler.mov(left_reg, mem);
                return true;
            }

            if (right_width < left_width)
            {
                if (left_sign_type == RegisterSignType::Signed)
                {
                    if (right_width == RegisterWidth::r32)
                    {
                        this->_assembler.movsxd(left_reg, mem);
                        return true;
                    }

                    this->_assembler.movsx(left_reg, mem);
                    return true;
                }

                if (right_width == RegisterWidth::r32)
                {
                    this->_assembler.mov(left_reg, mem);
                    return true;
                }

                this->_assembler.movzx(left_reg, mem);
                return true;
            }
        }
    }

    if (left.is_mem())
    {
        const auto left_width = left.mem().width();

        /// mov [mem], reg
        if (right.is_reg())
        {
            const auto right_width = right.reg().width().value();
            const auto mem_width = left_width.value_or(right_width);

            this->_assembler.mov(this->_operand_translator.translate(left.mem(), mem_width),
                                 this->_operand_translator.translate(right.reg(), right_width));

            return true;
        }

        /// mov [mem], cons
        if (right.is_constant())
        {
            const auto constant = right.constant();
            if (constant.width() < RegisterWidth::r64)
            {
                const auto mem_width = left.mem().width().value_or(constant.width());
                this->_assembler.mov(this->_operand_translator.translate(left.mem(), mem_width),
                                     right.constant().value_as_int64());
                return true;
            }
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::CmovleInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();
        const auto left_sign_type = left.reg().sign_type().value();

        /// mov reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());

            this->_assembler.cmovle(left_reg, right_reg);
            return true;
        }

        /// mov reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            this->_assembler.cmovle(left_reg, mem);
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::CmovgeInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();
        const auto left_sign_type = left.reg().sign_type().value();

        /// mov reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());

            this->_assembler.cmovge(left_reg, right_reg);
            return true;
        }

        /// mov reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            this->_assembler.cmovge(left_reg, mem);
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::LeaInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// lea reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            this->_assembler.lea(left_reg, mem);
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::AddInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// add reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());
            this->_assembler.add(left_reg, right_reg);
            return true;
        }

        /// add reg, cons
        if (right.is_constant())
        {
            this->_assembler.add(left_reg, right.constant().value_as_int64());
            return true;
        }

        /// add reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            this->_assembler.add(left_reg, mem);
            return true;
        }
    }

    if (left.is_mem())
    {
        /// add [mem], reg
        if (right.is_reg())
        {
            const auto mem_width = left.mem().width().value_or(right.reg().width().value());

            this->_assembler.add(this->_operand_translator.translate(left.mem(), mem_width),
                                 this->_operand_translator.translate(right.reg()));
            return true;
        }

        /// add [mem], cons
        if (right.is_constant())
        {
            const auto constant = right.constant();
            const auto mem_width = left.mem().width().value_or(right.constant().width());
            this->_assembler.add(this->_operand_translator.translate(left.mem(), mem_width),
                                 right.constant().value_as_int64());
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::XaddInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (instruction.is_locked())
    {
        this->_assembler.lock();
    }

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// add reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());
            this->_assembler.xadd(left_reg, right_reg);
            return true;
        }
    }

    if (left.is_mem())
    {
        /// add [mem], reg
        if (right.is_reg())
        {
            const auto mem_width = left.mem().width().value_or(right.reg().width().value());

            this->_assembler.xadd(this->_operand_translator.translate(left.mem(), mem_width),
                                  this->_operand_translator.translate(right.reg()));
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::SubInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// sub reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());
            this->_assembler.sub(left_reg, right_reg);
            return true;
        }

        /// sub reg, cons
        if (right.is_constant())
        {
            this->_assembler.sub(left_reg, right.constant().value_as_int64());
            return true;
        }

        /// sub reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            this->_assembler.sub(left_reg, mem);
            return true;
        }
    }

    if (left.is_mem())
    {
        /// sub [mem], reg
        if (right.is_reg())
        {
            const auto mem_width = left.mem().width().value_or(right.reg().width().value());

            this->_assembler.sub(this->_operand_translator.translate(left.mem(), mem_width),
                                 this->_operand_translator.translate(right.reg()));
            return true;
        }

        /// sub [mem], cons
        if (right.is_constant())
        {
            const auto constant = right.constant();
            const auto mem_width = left.mem().width().value_or(right.constant().width());
            this->_assembler.sub(this->_operand_translator.translate(left.mem(), mem_width),
                                 right.constant().value_as_int64());
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::ImulInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// imul reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());
            this->_assembler.imul(left_reg, right_reg);
            return true;
        }

        /// imul reg, cons
        if (right.is_constant())
        {
            const auto constant = right.constant().value_as_int64();
            if (constant == 2U)
            {
                this->_assembler.add(left_reg, left_reg);
                return true;
            }

            if ((constant & (constant - 1)) == 0)
            {
                const auto power_of_two = std::log2<std::uint64_t>(constant);
                if (power_of_two <= 128U)
                {
                    this->_assembler.shl(left_reg, std::int8_t(power_of_two));
                    return true;
                }
            }

            if (constant == 3U)
            {
                this->_assembler.lea(left_reg, asmjit::x86::ptr(left_reg, left_reg, 1));
                return true;
            }

            if (constant == 5U)
            {
                this->_assembler.lea(left_reg, asmjit::x86::ptr(left_reg, left_reg, 2));
                return true;
            }

            if (constant == 9U)
            {
                this->_assembler.lea(left_reg, asmjit::x86::ptr(left_reg, left_reg, 3));
                return true;
            }

            this->_assembler.imul(left_reg, constant);
            return true;
        }

        /// imul reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            this->_assembler.imul(left_reg, mem);
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::AndInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// and reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());
            this->_assembler.and_(left_reg, right_reg);
            return true;
        }

        /// and reg, cons
        if (right.is_constant())
        {
            this->_assembler.and_(left_reg, right.constant().value_as_int64());
            return true;
        }

        /// and reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            this->_assembler.and_(left_reg, mem);
            return true;
        }
    }

    if (left.is_mem())
    {
        /// and [mem], reg
        if (right.is_reg())
        {
            const auto mem_width = left.mem().width().value_or(right.reg().width().value());

            this->_assembler.and_(this->_operand_translator.translate(left.mem(), mem_width),
                                  this->_operand_translator.translate(right.reg()));
            return true;
        }

        /// and [mem], cons
        if (right.is_constant())
        {
            const auto constant = right.constant();
            const auto mem_width = left.mem().width().value_or(right.constant().width());
            this->_assembler.and_(this->_operand_translator.translate(left.mem(), mem_width),
                                  right.constant().value_as_int64());
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::OrInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// or reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());
            this->_assembler.or_(left_reg, right_reg);
            return true;
        }

        /// or reg, cons
        if (right.is_constant())
        {
            this->_assembler.or_(left_reg, right.constant().value_as_int64());
            return true;
        }

        /// or reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            this->_assembler.or_(left_reg, mem);
            return true;
        }
    }

    if (left.is_mem())
    {
        /// or [mem], reg
        if (right.is_reg())
        {
            const auto mem_width = left.mem().width().value_or(right.reg().width().value());

            this->_assembler.or_(this->_operand_translator.translate(left.mem(), mem_width),
                                 this->_operand_translator.translate(right.reg()));
            return true;
        }

        /// or [mem], cons
        if (right.is_constant())
        {
            const auto constant = right.constant();
            const auto mem_width = left.mem().width().value_or(right.constant().width());
            this->_assembler.or_(this->_operand_translator.translate(left.mem(), mem_width),
                                 right.constant().value_as_int64());
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::XorInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// xor reg, reg
        if (right.is_reg())
        {
            auto right_reg = this->_operand_translator.translate(right.reg());
            this->_assembler.xor_(left_reg, right_reg);
            return true;
        }

        /// xor reg, cons
        if (right.is_constant())
        {
            this->_assembler.xor_(left_reg, right.constant().value_as_int64());
            return true;
        }

        /// xor reg, [mem]
        if (right.is_mem())
        {
            const auto right_width = right.mem().width().value_or(left_width);
            const auto mem = this->_operand_translator.translate(right.mem(), right_width);

            this->_assembler.xor_(left_reg, mem);
            return true;
        }
    }

    if (left.is_mem())
    {
        /// xor [mem], reg
        if (right.is_reg())
        {
            const auto mem_width = left.mem().width().value_or(right.reg().width().value());

            this->_assembler.xor_(this->_operand_translator.translate(left.mem(), mem_width),
                                  this->_operand_translator.translate(right.reg()));
            return true;
        }

        /// xor [mem], cons
        if (right.is_constant())
        {
            const auto constant = right.constant();
            const auto mem_width = left.mem().width().value_or(right.constant().width());
            this->_assembler.xor_(this->_operand_translator.translate(left.mem(), mem_width),
                                  right.constant().value_as_int64());
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::ShlInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// shl reg, reg
        if (right.is_reg())
        {
            this->_assembler.mov(asmjit::x86::cl, this->_operand_translator.translate(right.reg(), RegisterWidth::r8));
            this->_assembler.shl(left_reg, asmjit::x86::cl);
            return true;
        }

        /// shl reg, cons
        if (right.is_constant())
        {
            this->_assembler.shl(left_reg, right.constant().value_as_int64());
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::ShrInstruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        /// shr reg, reg
        if (right.is_reg())
        {
            this->_assembler.mov(asmjit::x86::cl, this->_operand_translator.translate(right.reg(), RegisterWidth::r8));
            this->_assembler.shr(left_reg, asmjit::x86::cl);
            return true;
        }

        /// shr reg, cons
        if (right.is_constant())
        {
            this->_assembler.shr(left_reg, right.constant().value_as_int64());
            return true;
        }
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::Crc32Instruction &instruction)
{
    const auto left = instruction.left();
    const auto right = instruction.right();

    if (left.is_reg() && right.is_reg())
    {
        const auto left_reg = this->_operand_translator.translate(left.reg());
        const auto left_width = left.reg().width().value();

        if (left.reg().width().value() == RegisterWidth::r64)
        {
            this->_assembler.crc32(left_reg, this->_operand_translator.translate(right.reg(), RegisterWidth::r64));
            return true;
        }

        this->_assembler.crc32(left_reg, this->_operand_translator.translate(right.reg()));
        return true;
    }

    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::FdivInstruction &instruction)
{
    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::FmodInstruction &instruction)
{
    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::FcallInstruction &instruction)
{
    throw CanNotTranslateInstructionException{instruction};
}

bool InstructionTranslator::translate(flounder::CallInstruction &instruction)
{
    this->_assembler.call(instruction.function_pointer());
    return true;
}

bool InstructionTranslator::translate(flounder::AlignInstruction &instruction)
{
    this->_assembler.align(asmjit::AlignMode::kCode, instruction.alignment());
    return true;
}