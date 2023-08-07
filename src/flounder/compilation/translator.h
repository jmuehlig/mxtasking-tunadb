#pragma once

#include <asmjit/x86.h>
#include <cstdint>
#include <flounder/exception.h>
#include <memory>
#include <unordered_map>

namespace flounder {
class OperandTranslator
{
public:
    OperandTranslator();
    ~OperandTranslator() = default;

    [[nodiscard]] asmjit::x86::Gp translate(const Register reg) { return translate(reg, reg.width().value()); }
    [[nodiscard]] asmjit::x86::Gp translate(const Register reg, const RegisterWidth width)
    {
        const auto &registers = _registers.at(width);
        if (reg.machine_register_id().value() >= registers.size()) [[unlikely]]
        {
            throw UnknownRegisterException{reg.machine_register_id().value(), width};
        }
        return registers[reg.machine_register_id().value()];
    }
    [[nodiscard]] asmjit::x86::Mem translate(const MemoryAddress mem,
                                             std::optional<RegisterWidth> access_width = std::nullopt);
    [[nodiscard]] asmjit::Label translate(Label label, asmjit::x86::Assembler &assembler, bool is_external);

private:
    std::unordered_map<std::string_view, asmjit::Label> _labels;
    std::unordered_map<RegisterWidth, std::array<asmjit::x86::Gp, 16U>> _registers;
};

class ContextLogger
{
public:
    ContextLogger() noexcept = default;
    virtual ~ContextLogger() noexcept = default;

    virtual void begin_context(const std::string &name) = 0;
    virtual void end_context(const std::string &name) = 0;
};

class InstructionTranslator
{
public:
    constexpr InstructionTranslator(OperandTranslator &operand_translator, asmjit::x86::Assembler &assembler,
                                    ContextLogger *logger, const bool is_keep_flounder_code)
        : _operand_translator(operand_translator), _assembler(assembler), _logger(logger),
          _is_keep_flounder_code(is_keep_flounder_code)
    {
    }

    ~InstructionTranslator() = default;

    [[nodiscard]] bool translate(VregInstruction &instruction);
    [[nodiscard]] bool translate(ClearInstruction &instruction);
    [[nodiscard]] bool translate(GetArgumentInstruction &instruction);
    [[nodiscard]] bool translate(SetReturnArgumentInstruction &instruction);
    [[nodiscard]] bool translate(CommentInstruction &instruction);
    [[nodiscard]] bool translate(ContextBeginInstruction &instruction);
    [[nodiscard]] bool translate(ContextEndInstruction &instruction);
    [[nodiscard]] bool translate(BranchBeginInstruction &instruction);
    [[nodiscard]] bool translate(BranchEndInstruction &instruction);
    [[nodiscard]] bool translate(RetInstruction &instruction);
    [[nodiscard]] bool translate(NopInstruction &instruction);
    [[nodiscard]] bool translate(CqoInstruction &instruction);
    [[nodiscard]] bool translate(PopInstruction &instruction);
    [[nodiscard]] bool translate(PushInstruction &instruction);
    [[nodiscard]] bool translate(JumpInstruction &instruction);
    [[nodiscard]] bool translate(SectionInstruction &instruction);
    [[nodiscard]] bool translate(IncInstruction &instruction);
    [[nodiscard]] bool translate(DecInstruction &instruction);
    [[nodiscard]] bool translate(SeteInstruction &instruction);
    [[nodiscard]] bool translate(SetneInstruction &instruction);
    [[nodiscard]] bool translate(LeaInstruction &instruction);
    [[nodiscard]] bool translate(PrefetchInstruction &instruction);
    [[nodiscard]] bool translate(IdivInstruction &instruction);
    [[nodiscard]] bool translate(CmpInstruction &instruction);
    [[nodiscard]] bool translate(TestInstruction &instruction);
    [[nodiscard]] bool translate(MovInstruction &instruction);
    [[nodiscard]] bool translate(CmovleInstruction &instruction);
    [[nodiscard]] bool translate(CmovgeInstruction &instruction);
    [[nodiscard]] bool translate(AddInstruction &instruction);
    [[nodiscard]] bool translate(XaddInstruction &instruction);
    [[nodiscard]] bool translate(SubInstruction &instruction);
    [[nodiscard]] bool translate(ImulInstruction &instruction);
    [[nodiscard]] bool translate(AndInstruction &instruction);
    [[nodiscard]] bool translate(OrInstruction &instruction);
    [[nodiscard]] bool translate(XorInstruction &instruction);
    [[nodiscard]] bool translate(ShlInstruction &instruction);
    [[nodiscard]] bool translate(ShrInstruction &instruction);
    [[nodiscard]] bool translate(Crc32Instruction &instruction);
    [[nodiscard]] bool translate(FdivInstruction &instruction);
    [[nodiscard]] bool translate(FmodInstruction &instruction);
    [[nodiscard]] bool translate(FcallInstruction &instruction);
    [[nodiscard]] bool translate(CallInstruction &instruction);
    [[nodiscard]] bool translate(AlignInstruction &instruction);

private:
    OperandTranslator &_operand_translator;
    asmjit::x86::Assembler &_assembler;
    ContextLogger *_logger;
    bool _is_keep_flounder_code;

    [[nodiscard]] bool is_keep_comment() const noexcept { return _is_keep_flounder_code; }
};
} // namespace flounder