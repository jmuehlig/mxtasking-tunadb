#pragma once

#include "label.h"
#include "operand.h"
#include <array>
#include <cstdint>
#include <fmt/core.h>
#include <functional>
#include <string>
#include <vector>

namespace flounder {
enum InstructionType : std::uint16_t
{
    RequestVreg,
    ClearVreg,
    GetArgument,
    SetReturnArgument,
    Comment,
    ContextBegin,
    ContextEnd,
    BranchBegin,
    BranchEnd,
    Ret,
    Nop,
    Cqo,
    Pop,
    Push,
    Jump,
    Section,
    Inc,
    Dec,
    Sete,
    Setne,
    Lea,
    Prefetch,
    Idiv,
    Cmp,
    Test,
    Mov,
    Cmovle,
    Cmovge,
    Add,
    Xadd,
    Sub,
    Imul,
    And,
    Or,
    Xor,
    Shl,
    Shr,
    Crc32,
    Fdiv,
    Fmod,
    Fcall,
    Call,
    Align
};

class InstructionInterface
{
public:
    constexpr InstructionInterface() noexcept = default;
    constexpr InstructionInterface(InstructionInterface &&) noexcept = default;
    constexpr InstructionInterface(const InstructionInterface &) = default;
    virtual ~InstructionInterface() noexcept = default;

    InstructionInterface &operator=(InstructionInterface &&) noexcept = default;

    [[nodiscard]] virtual InstructionType type() const noexcept = 0;

    [[nodiscard]] virtual std::string to_string() const = 0;

    [[nodiscard]] const std::optional<std::string> &inline_comment() const noexcept { return _inline_comment; }
    void inline_comment(std::string &&comment) noexcept { _inline_comment = std::move(comment); }

    [[nodiscard]] virtual std::uint8_t operands() const noexcept = 0;
    [[nodiscard]] virtual std::optional<std::reference_wrapper<Operand>> operand(std::uint8_t index) noexcept = 0;
    [[nodiscard]] virtual Operand operand(std::uint8_t index) const noexcept = 0;
    [[nodiscard]] virtual bool is_writing(std::uint8_t index) const noexcept = 0;

private:
    std::optional<std::string> _inline_comment{std::nullopt};
};

template <InstructionType I> class TypedInstruction : public InstructionInterface
{
public:
    constexpr TypedInstruction() noexcept = default;
    constexpr TypedInstruction(TypedInstruction &&) noexcept = default;
    constexpr TypedInstruction(const TypedInstruction &) = default;
    ~TypedInstruction() noexcept override = default;

    TypedInstruction &operator=(TypedInstruction &&) noexcept = default;

    [[nodiscard]] InstructionType type() const noexcept override { return I; }
};

template <InstructionType I> class NullaryOperandInstruction : public TypedInstruction<I>
{
public:
    constexpr NullaryOperandInstruction() noexcept = default;
    constexpr NullaryOperandInstruction(NullaryOperandInstruction &&) noexcept = default;
    constexpr NullaryOperandInstruction(const NullaryOperandInstruction &) = default;
    ~NullaryOperandInstruction() noexcept override = default;

    NullaryOperandInstruction &operator=(NullaryOperandInstruction &&) noexcept = default;

    [[nodiscard]] std::uint8_t operands() const noexcept override { return 0U; }
    [[nodiscard]] std::optional<std::reference_wrapper<Operand>> operand(const std::uint8_t /*index*/) noexcept override
    {
        return std::nullopt;
    }

    [[nodiscard]] Operand operand(const std::uint8_t /*index*/) const noexcept override { return Operand{Constant(0)}; }
    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return false; }
};

template <InstructionType I> class UnaryOperandInstruction : public TypedInstruction<I>
{
public:
    explicit UnaryOperandInstruction(Operand operand) noexcept : _operand(operand) {}

    UnaryOperandInstruction(UnaryOperandInstruction &&) noexcept = default;
    UnaryOperandInstruction(const UnaryOperandInstruction &) = default;

    ~UnaryOperandInstruction() noexcept override = default;

    UnaryOperandInstruction &operator=(UnaryOperandInstruction &&) noexcept = default;

    [[nodiscard]] Operand &operand() noexcept { return _operand; }
    [[nodiscard]] const Operand &operand() const noexcept { return _operand; }

    [[nodiscard]] std::uint8_t operands() const noexcept override { return 1U; }
    [[nodiscard]] std::optional<std::reference_wrapper<Operand>> operand(const std::uint8_t /*index*/) noexcept override
    {
        return std::ref(_operand);
    }

    [[nodiscard]] Operand operand(const std::uint8_t /*index*/) const noexcept override { return _operand; }

private:
    Operand _operand;
};

template <InstructionType I> class BinaryOperandInstruction : public TypedInstruction<I>
{
public:
    BinaryOperandInstruction(Operand left, Operand right) noexcept : _operands{left, right} {}

    BinaryOperandInstruction(BinaryOperandInstruction &&) noexcept = default;
    BinaryOperandInstruction(const BinaryOperandInstruction &) = default;

    ~BinaryOperandInstruction() noexcept override = default;

    BinaryOperandInstruction &operator=(BinaryOperandInstruction &&) noexcept = default;

    [[nodiscard]] Operand &left() noexcept { return _operands[0U]; }
    [[nodiscard]] const Operand &left() const noexcept { return _operands[0U]; }
    [[nodiscard]] Operand &right() noexcept { return _operands[1U]; }
    [[nodiscard]] const Operand &right() const noexcept { return _operands[1U]; }

    [[nodiscard]] std::uint8_t operands() const noexcept override { return 2U; }
    [[nodiscard]] std::optional<std::reference_wrapper<Operand>> operand(const std::uint8_t index) noexcept override
    {
        return std::ref(_operands[index]);
    }
    [[nodiscard]] Operand operand(const std::uint8_t index) const noexcept override { return _operands[index]; }

private:
    std::array<Operand, 2U> _operands;
};

template <InstructionType I> class TernaryOperandInstruction : public TypedInstruction<I>
{
public:
    TernaryOperandInstruction(Operand first, Operand second, Operand third) noexcept : _operands{first, second, third}
    {
    }

    TernaryOperandInstruction(TernaryOperandInstruction &&) noexcept = default;
    TernaryOperandInstruction(const TernaryOperandInstruction &) = default;

    ~TernaryOperandInstruction() noexcept override = default;

    TernaryOperandInstruction &operator=(TernaryOperandInstruction &&) noexcept = default;

    [[nodiscard]] Operand &first() noexcept { return _operands[0U]; }
    [[nodiscard]] const Operand &first() const noexcept { return _operands[0U]; }
    [[nodiscard]] Operand &second() noexcept { return _operands[1U]; }
    [[nodiscard]] const Operand &second() const noexcept { return _operands[1U]; }
    [[nodiscard]] Operand &third() noexcept { return _operands[2U]; }
    [[nodiscard]] const Operand &third() const noexcept { return _operands[2U]; }

    [[nodiscard]] std::uint8_t operands() const noexcept override { return 3U; }
    [[nodiscard]] std::optional<std::reference_wrapper<Operand>> operand(const std::uint8_t index) noexcept override
    {
        return std::ref(_operands[index]);
    }
    [[nodiscard]] Operand operand(const std::uint8_t index) const noexcept override { return _operands[index]; }

private:
    std::array<Operand, 3U> _operands;
};

class VregInstruction final : public NullaryOperandInstruction<InstructionType::RequestVreg>
{
public:
    VregInstruction(Register reg, const RegisterWidth width, const RegisterSignType sign_type) noexcept
        : _vreg(reg), _width(width), _sign_type(sign_type)
    {
    }

    VregInstruction(VregInstruction &&) noexcept = default;
    VregInstruction(const VregInstruction &) = default;

    ~VregInstruction() noexcept override = default;

    VregInstruction &operator=(VregInstruction &&) noexcept = default;

    [[nodiscard]] Register vreg() const noexcept { return _vreg; }
    [[nodiscard]] RegisterWidth width() const noexcept { return _width; }
    [[nodiscard]] RegisterSignType sign_type() const noexcept { return _sign_type; }

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("vreg{}{} {}", static_cast<std::uint16_t>(_width),
                           (_sign_type == RegisterSignType::Unsigned ? "u" : ""), _vreg.to_string());
    }

private:
    Register _vreg;
    RegisterWidth _width;
    RegisterSignType _sign_type;
};

class ClearInstruction final : public NullaryOperandInstruction<InstructionType::ClearVreg>
{
public:
    explicit ClearInstruction(Register reg) noexcept : _vreg(reg) {}

    ClearInstruction(ClearInstruction &&) noexcept = default;
    ClearInstruction(const ClearInstruction &) = default;

    ~ClearInstruction() noexcept override = default;

    ClearInstruction &operator=(ClearInstruction &&) noexcept = default;

    [[nodiscard]] Register vreg() const noexcept { return _vreg; }

    [[nodiscard]] std::string to_string() const override { return fmt::format("clear {}", _vreg.to_string()); }

private:
    Register _vreg;
};

class GetArgumentInstruction final : public UnaryOperandInstruction<InstructionType::GetArgument>
{
public:
    GetArgumentInstruction(const std::uint8_t index, Register reg) noexcept
        : UnaryOperandInstruction(Operand{reg}), _index(index)
    {
    }

    GetArgumentInstruction(GetArgumentInstruction &&) noexcept = default;
    GetArgumentInstruction(const GetArgumentInstruction &) = default;

    ~GetArgumentInstruction() noexcept override = default;

    GetArgumentInstruction &operator=(GetArgumentInstruction &&) noexcept = default;

    [[nodiscard]] std::uint8_t index() const noexcept { return _index; }
    [[nodiscard]] Register vreg() const noexcept { return operand().reg(); }

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("getarg {},{}", std::uint16_t(_index), vreg().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return true; }

private:
    std::uint8_t _index;
};

class SetReturnArgumentInstruction final : public UnaryOperandInstruction<InstructionType::SetReturnArgument>
{
public:
    explicit SetReturnArgumentInstruction(Operand operand) noexcept : UnaryOperandInstruction(operand) {}

    SetReturnArgumentInstruction(SetReturnArgumentInstruction &&) noexcept = default;
    SetReturnArgumentInstruction(const SetReturnArgumentInstruction &) = default;

    ~SetReturnArgumentInstruction() noexcept override = default;

    SetReturnArgumentInstruction &operator=(SetReturnArgumentInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return fmt::format("return {}", operand().to_string()); }
    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return false; }
};

class CommentInstruction final : public NullaryOperandInstruction<InstructionType::Comment>
{
public:
    explicit CommentInstruction(std::string &&text) noexcept : _text(std::move(text)) {}

    CommentInstruction(CommentInstruction &&) noexcept = default;
    CommentInstruction(const CommentInstruction &) = default;

    ~CommentInstruction() override = default;

    CommentInstruction &operator=(CommentInstruction &&) noexcept = default;

    [[nodiscard]] const std::string &text() const noexcept { return _text; }

    [[nodiscard]] std::string to_string() const override { return fmt::format("; {}", _text); }

private:
    std::string _text;
};

class ContextBeginInstruction final : public NullaryOperandInstruction<InstructionType::ContextBegin>
{
public:
    explicit ContextBeginInstruction(std::string &&name) noexcept : _name(std::move(name)) {}

    ContextBeginInstruction(ContextBeginInstruction &&) noexcept = default;
    ContextBeginInstruction(const ContextBeginInstruction &) = default;

    ~ContextBeginInstruction() override = default;

    ContextBeginInstruction &operator=(ContextBeginInstruction &&) noexcept = default;

    [[nodiscard]] const std::string &name() const noexcept { return _name; }

    [[nodiscard]] std::string to_string() const override { return fmt::format("@begin-context {}", _name); }

private:
    std::string _name;
};

class ContextEndInstruction final : public NullaryOperandInstruction<InstructionType::ContextEnd>
{
public:
    explicit ContextEndInstruction(std::string &&name) noexcept : _name(std::move(name)) {}

    ContextEndInstruction(ContextEndInstruction &&) noexcept = default;
    ContextEndInstruction(const ContextEndInstruction &) = default;

    ~ContextEndInstruction() override = default;

    ContextEndInstruction &operator=(ContextEndInstruction &&) noexcept = default;

    [[nodiscard]] const std::string &name() const noexcept { return _name; }

    [[nodiscard]] std::string to_string() const override { return fmt::format("@end-context {}", _name); }

private:
    std::string _name;
};

class BranchBeginInstruction final : public NullaryOperandInstruction<InstructionType::BranchBegin>
{
public:
    explicit BranchBeginInstruction(const std::uint64_t id) noexcept : _id(id) {}

    BranchBeginInstruction(BranchBeginInstruction &&) noexcept = default;
    BranchBeginInstruction(const BranchBeginInstruction &) = default;

    ~BranchBeginInstruction() override = default;

    BranchBeginInstruction &operator=(BranchBeginInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return fmt::format("@begin-branch #{}", _id); }

private:
    std::uint64_t _id;
};

class BranchEndInstruction final : public NullaryOperandInstruction<InstructionType::BranchEnd>
{
public:
    BranchEndInstruction() noexcept = default;

    BranchEndInstruction(BranchEndInstruction &&) noexcept = default;
    BranchEndInstruction(const BranchEndInstruction &) = default;

    ~BranchEndInstruction() override = default;

    BranchEndInstruction &operator=(BranchEndInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return "@end-branch"; }
};

class RetInstruction final : public NullaryOperandInstruction<InstructionType::Ret>
{
public:
    RetInstruction() noexcept = default;
    RetInstruction(RetInstruction &&) noexcept = default;
    RetInstruction(const RetInstruction &) = default;

    ~RetInstruction() override = default;

    RetInstruction &operator=(RetInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return "ret"; }
};

class NopInstruction final : public NullaryOperandInstruction<InstructionType::Nop>
{
public:
    NopInstruction() noexcept = default;
    NopInstruction(NopInstruction &&) noexcept = default;
    NopInstruction(const NopInstruction &) = default;
    ~NopInstruction() override = default;

    NopInstruction &operator=(NopInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return "nop"; }
};

class CqoInstruction final : public NullaryOperandInstruction<InstructionType::Cqo>
{
public:
    CqoInstruction() noexcept = default;
    CqoInstruction(CqoInstruction &&) noexcept = default;
    CqoInstruction(const CqoInstruction &) = default;
    ~CqoInstruction() override = default;

    CqoInstruction &operator=(CqoInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return "cqo"; }
};

class PopInstruction final : public NullaryOperandInstruction<InstructionType::Pop>
{
public:
    explicit PopInstruction(Register reg) noexcept : _reg(reg) {}
    PopInstruction(PopInstruction &&) noexcept = default;
    PopInstruction(const PopInstruction &) = default;

    ~PopInstruction() noexcept override = default;

    PopInstruction &operator=(PopInstruction &&) noexcept = default;

    [[nodiscard]] Register reg() const noexcept { return _reg; }

    [[nodiscard]] std::string to_string() const override { return fmt::format("pop {}", _reg.to_string()); }

private:
    Register _reg;
};

class PushInstruction final : public NullaryOperandInstruction<InstructionType::Push>
{
public:
    explicit PushInstruction(Register reg) noexcept : _reg(reg) {}
    PushInstruction(PushInstruction &&) noexcept = default;
    PushInstruction(const PushInstruction &) = default;

    ~PushInstruction() noexcept override = default;

    PushInstruction &operator=(PushInstruction &&) noexcept = default;

    [[nodiscard]] Register reg() const noexcept { return _reg; }

    [[nodiscard]] std::string to_string() const override { return fmt::format("push {}", _reg.to_string()); }

private:
    Register _reg;
};

class JumpInstruction final : public NullaryOperandInstruction<InstructionType::Jump>
{
public:
    enum Type : std::uint8_t
    {
        JMP,
        JE,
        JNE,
        JZ,
        JNZ,
        JLE,
        JL,
        JB,
        JBE,
        JGE,
        JG,
        JA,
        JAE,
    };

    JumpInstruction(const Type type, Label label) noexcept : _type(type), _label(label) {}
    JumpInstruction(JumpInstruction &&) noexcept = default;
    JumpInstruction(const JumpInstruction &) = default;

    ~JumpInstruction() noexcept override = default;

    JumpInstruction &operator=(JumpInstruction &&) noexcept = default;

    [[nodiscard]] Type jump_type() const noexcept { return _type; }
    [[nodiscard]] Label label() const noexcept { return _label; }
    void label(Label label) noexcept { _label = label; }

    [[nodiscard]] std::string to_string() const override
    {
        switch (_type)
        {
        case Type::JMP:
            return fmt::format("jmp {}", _label.label());
        case Type::JE:
            return fmt::format("je {}", _label.label());
        case Type::JNE:
            return fmt::format("jne {}", _label.label());
        case Type::JZ:
            return fmt::format("jz {}", _label.label());
        case Type::JNZ:
            return fmt::format("jnz {}", _label.label());
        case Type::JLE:
            return fmt::format("jle {}", _label.label());
        case Type::JL:
            return fmt::format("jl {}", _label.label());
        case Type::JB:
            return fmt::format("jb {}", _label.label());
        case Type::JBE:
            return fmt::format("jbe {}", _label.label());
        case Type::JGE:
            return fmt::format("jge {}", _label.label());
        case Type::JG:
            return fmt::format("jg {}", _label.label());
        case Type::JA:
            return fmt::format("ja {}", _label.label());
        case Type::JAE:
            return fmt::format("jae {}", _label.label());
        }
    }

    void inverse()
    {
        switch (_type)
        {
        case Type::JMP:
            _type = Type::JMP;
            break;
        case Type::JE:
            _type = Type::JNE;
            break;
        case Type::JNE:
            _type = Type::JE;
            break;
        case Type::JZ:
            _type = Type::JNZ;
            break;
        case Type::JNZ:
            _type = Type::JZ;
            break;
        case Type::JLE:
            _type = Type::JG;
            break;
        case Type::JL:
            _type = Type::JGE;
            break;
        case Type::JB:
            _type = Type::JAE;
            break;
        case Type::JBE:
            _type = Type::JA;
            break;
        case Type::JGE:
            _type = Type::JL;
            break;
        case Type::JG:
            _type = Type::JLE;
            break;
        case Type::JA:
            _type = Type::JBE;
            break;
        case Type::JAE:
            _type = Type::JB;
            break;
        }
    }

private:
    Type _type;
    Label _label;
};

class SectionInstruction final : public NullaryOperandInstruction<InstructionType::Section>
{
public:
    explicit SectionInstruction(Label label) noexcept : _label(label) {}
    SectionInstruction(SectionInstruction &&) noexcept = default;
    SectionInstruction(const SectionInstruction &) = default;

    ~SectionInstruction() noexcept override = default;

    SectionInstruction &operator=(SectionInstruction &&) noexcept = default;

    [[nodiscard]] Label label() const noexcept { return _label; }

    [[nodiscard]] std::string to_string() const override { return fmt::format("{}:", _label.label()); }

private:
    Label _label;
};

class IncInstruction final : public UnaryOperandInstruction<InstructionType::Inc>
{
public:
    IncInstruction(Operand operand) noexcept : UnaryOperandInstruction(operand) {}
    IncInstruction(IncInstruction &&) noexcept = default;
    IncInstruction(const IncInstruction &) = default;

    ~IncInstruction() noexcept override = default;

    IncInstruction &operator=(IncInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return fmt::format("inc {}", operand().to_string()); }
    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return true; }
};

class DecInstruction final : public UnaryOperandInstruction<InstructionType::Dec>
{
public:
    DecInstruction(Operand operand) noexcept : UnaryOperandInstruction(operand) {}
    DecInstruction(DecInstruction &&) noexcept = default;
    DecInstruction(const DecInstruction &) = default;

    ~DecInstruction() noexcept override = default;

    DecInstruction &operator=(DecInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return fmt::format("dec {}", operand().to_string()); }
    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return true; }
};

class SeteInstruction final : public UnaryOperandInstruction<InstructionType::Sete>
{
public:
    SeteInstruction(Operand operand) noexcept : UnaryOperandInstruction(operand) {}
    SeteInstruction(SeteInstruction &&) noexcept = default;
    SeteInstruction(const SeteInstruction &) = default;

    ~SeteInstruction() noexcept override = default;

    SeteInstruction &operator=(SeteInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return fmt::format("sete {}", operand().to_string()); }
    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return true; }
};

class SetneInstruction final : public UnaryOperandInstruction<InstructionType::Setne>
{
public:
    SetneInstruction(Operand operand) noexcept : UnaryOperandInstruction(operand) {}
    SetneInstruction(SetneInstruction &&) noexcept = default;
    SetneInstruction(const SetneInstruction &) = default;

    ~SetneInstruction() noexcept override = default;

    SetneInstruction &operator=(SetneInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return fmt::format("setne {}", operand().to_string()); }
    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return true; }
};

class PrefetchInstruction final : public UnaryOperandInstruction<InstructionType::Prefetch>
{
public:
    PrefetchInstruction(Operand operand) noexcept : UnaryOperandInstruction(operand) {}
    PrefetchInstruction(PrefetchInstruction &&) noexcept = default;
    PrefetchInstruction(const PrefetchInstruction &) = default;

    ~PrefetchInstruction() noexcept override = default;

    PrefetchInstruction &operator=(PrefetchInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return fmt::format("prefetch {}", operand().to_string()); }

    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return false; }
};

class IdivInstruction final : public UnaryOperandInstruction<InstructionType::Idiv>
{
public:
    IdivInstruction(Operand operand) noexcept : UnaryOperandInstruction(operand) {}
    IdivInstruction(IdivInstruction &&) noexcept = default;
    IdivInstruction(const IdivInstruction &) = default;

    ~IdivInstruction() noexcept override = default;

    IdivInstruction &operator=(IdivInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override { return fmt::format("idiv {}", operand().to_string()); }

    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return false; }
};

class CmpInstruction final : public BinaryOperandInstruction<InstructionType::Cmp>
{
public:
    CmpInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    CmpInstruction(Operand left, Operand right, const bool is_likely) noexcept
        : BinaryOperandInstruction(left, right), _is_likely(is_likely)
    {
    }
    CmpInstruction(CmpInstruction &&) noexcept = default;
    CmpInstruction(const CmpInstruction &) = default;

    ~CmpInstruction() noexcept override = default;

    CmpInstruction &operator=(CmpInstruction &&) noexcept = default;

    void is_likely(const bool is_likely) noexcept { _is_likely = is_likely; }
    [[nodiscard]] bool is_likely() const noexcept { return _is_likely; }

    void unrollable_iterations(const std::optional<std::uint8_t> unrollable_iterations) noexcept
    {
        _unrollable_iterations = unrollable_iterations;
    }
    [[nodiscard]] std::optional<std::uint8_t> unrollable_iterations() const noexcept { return _unrollable_iterations; }

    [[nodiscard]] std::string to_string() const override
    {
        if (_is_likely)
        {
            if (_unrollable_iterations.has_value())
            {
                return fmt::format("cmp {}, {} [[unroll={:d}]]", left().to_string(), right().to_string(),
                                   _unrollable_iterations.value());
            }

            return fmt::format("cmp {}, {}", left().to_string(), right().to_string());
        }

        return fmt::format("cmp {}, {} [[unlikely]]", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return false; }

private:
    bool _is_likely = true;
    std::optional<std::uint8_t> _unrollable_iterations{std::nullopt};
};

class TestInstruction final : public BinaryOperandInstruction<InstructionType::Test>
{
public:
    TestInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    TestInstruction(TestInstruction &&) noexcept = default;
    TestInstruction(const TestInstruction &) = default;

    ~TestInstruction() noexcept override = default;

    TestInstruction &operator=(TestInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("test {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return false; }
};

class MovInstruction final : public BinaryOperandInstruction<InstructionType::Mov>
{
public:
    MovInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    MovInstruction(MovInstruction &&) noexcept = default;
    MovInstruction(const MovInstruction &) = default;

    ~MovInstruction() noexcept override = default;

    MovInstruction &operator=(MovInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("mov {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class CmovleInstruction final : public BinaryOperandInstruction<InstructionType::Cmovle>
{
public:
    CmovleInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    CmovleInstruction(CmovleInstruction &&) noexcept = default;
    CmovleInstruction(const CmovleInstruction &) = default;

    ~CmovleInstruction() noexcept override = default;

    CmovleInstruction &operator=(CmovleInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("cmovle {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class CmovgeInstruction final : public BinaryOperandInstruction<InstructionType::Cmovge>
{
public:
    CmovgeInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    CmovgeInstruction(CmovgeInstruction &&) noexcept = default;
    CmovgeInstruction(const CmovgeInstruction &) = default;

    ~CmovgeInstruction() noexcept override = default;

    CmovgeInstruction &operator=(CmovgeInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("cmovge {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class LeaInstruction final : public BinaryOperandInstruction<InstructionType::Lea>
{
public:
    LeaInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    LeaInstruction(LeaInstruction &&) noexcept = default;
    LeaInstruction(const LeaInstruction &) = default;

    ~LeaInstruction() noexcept override = default;

    LeaInstruction &operator=(LeaInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("lea {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class AddInstruction final : public BinaryOperandInstruction<InstructionType::Add>
{
public:
    AddInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    AddInstruction(AddInstruction &&) noexcept = default;
    AddInstruction(const AddInstruction &) = default;

    ~AddInstruction() noexcept override = default;

    AddInstruction &operator=(AddInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("add {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class XaddInstruction final : public BinaryOperandInstruction<InstructionType::Xadd>
{
public:
    XaddInstruction(Operand left, Operand right, const bool is_locked) noexcept
        : BinaryOperandInstruction(left, right), _is_locked(is_locked)
    {
    }
    XaddInstruction(XaddInstruction &&) noexcept = default;
    XaddInstruction(const XaddInstruction &) = default;

    ~XaddInstruction() noexcept override = default;

    XaddInstruction &operator=(XaddInstruction &&) noexcept = default;

    [[nodiscard]] bool is_locked() const noexcept { return _is_locked; }
    [[nodiscard]] std::string to_string() const override
    {
        if (_is_locked)
        {
            return fmt::format("lock xadd {}, {}", left().to_string(), right().to_string());
        }

        return fmt::format("xadd {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t /*index*/) const noexcept override { return true; }

private:
    bool _is_locked;
};

class SubInstruction final : public BinaryOperandInstruction<InstructionType::Sub>
{
public:
    SubInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    SubInstruction(SubInstruction &&) noexcept = default;
    SubInstruction(const SubInstruction &) = default;

    ~SubInstruction() noexcept override = default;

    SubInstruction &operator=(SubInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("sub {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class ImulInstruction final : public BinaryOperandInstruction<InstructionType::Imul>
{
public:
    ImulInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    ImulInstruction(ImulInstruction &&) noexcept = default;
    ImulInstruction(const ImulInstruction &) = default;

    ~ImulInstruction() noexcept override = default;

    ImulInstruction &operator=(ImulInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("imul {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class AndInstruction final : public BinaryOperandInstruction<InstructionType::And>
{
public:
    AndInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    AndInstruction(AndInstruction &&) noexcept = default;
    AndInstruction(const AndInstruction &) = default;

    ~AndInstruction() noexcept override = default;

    AndInstruction &operator=(AndInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("and {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class OrInstruction final : public BinaryOperandInstruction<InstructionType::Or>
{
public:
    OrInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    OrInstruction(OrInstruction &&) noexcept = default;
    OrInstruction(const OrInstruction &) = default;

    ~OrInstruction() noexcept override = default;

    OrInstruction &operator=(OrInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("or {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class XorInstruction final : public BinaryOperandInstruction<InstructionType::Xor>
{
public:
    XorInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    XorInstruction(XorInstruction &&) noexcept = default;
    XorInstruction(const XorInstruction &) = default;

    ~XorInstruction() noexcept override = default;

    XorInstruction &operator=(XorInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("xor {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class ShlInstruction final : public BinaryOperandInstruction<InstructionType::Shl>
{
public:
    ShlInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    ShlInstruction(ShlInstruction &&) noexcept = default;
    ShlInstruction(const ShlInstruction &) = default;

    ~ShlInstruction() noexcept override = default;

    ShlInstruction &operator=(ShlInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("shl {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class ShrInstruction final : public BinaryOperandInstruction<InstructionType::Shr>
{
public:
    ShrInstruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    ShrInstruction(ShrInstruction &&) noexcept = default;
    ShrInstruction(const ShrInstruction &) = default;

    ~ShrInstruction() noexcept override = default;

    ShrInstruction &operator=(ShrInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("shr {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class Crc32Instruction final : public BinaryOperandInstruction<InstructionType::Crc32>
{
public:
    Crc32Instruction(Operand left, Operand right) noexcept : BinaryOperandInstruction(left, right) {}
    Crc32Instruction(Crc32Instruction &&) noexcept = default;
    Crc32Instruction(const Crc32Instruction &) = default;

    ~Crc32Instruction() noexcept override = default;

    Crc32Instruction &operator=(Crc32Instruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("crc32 {}, {}", left().to_string(), right().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index == 0U; }
};

class FdivInstruction final : public TernaryOperandInstruction<InstructionType::Fdiv>
{
public:
    FdivInstruction(Operand first, Operand second, Operand third) noexcept
        : TernaryOperandInstruction(first, second, third)
    {
    }
    FdivInstruction(FdivInstruction &&) noexcept = default;
    FdivInstruction(const FdivInstruction &) = default;

    ~FdivInstruction() noexcept override = default;

    FdivInstruction &operator=(FdivInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("fdiv {}, {}, {}", first().to_string(), second().to_string(), third().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index < 2U; }
};

class FmodInstruction final : public TernaryOperandInstruction<InstructionType::Fmod>
{
public:
    FmodInstruction(Operand first, Operand second, Operand third) noexcept
        : TernaryOperandInstruction(first, second, third)
    {
    }
    FmodInstruction(FmodInstruction &&) noexcept = default;
    FmodInstruction(const FmodInstruction &) = default;

    ~FmodInstruction() noexcept override = default;

    FmodInstruction &operator=(FmodInstruction &&) noexcept = default;

    [[nodiscard]] std::string to_string() const override
    {
        return fmt::format("fmod {}, {}, {}", first().to_string(), second().to_string(), third().to_string());
    }

    [[nodiscard]] bool is_writing(const std::uint8_t index) const noexcept override { return index < 2U; }
};

class FcallInstruction final : public NullaryOperandInstruction<InstructionType::Fcall>
{
public:
    explicit FcallInstruction(const std::uintptr_t function_pointer) : _function_pointer(function_pointer) {}

    FcallInstruction(Register return_register, const std::uintptr_t function_pointer)
        : _return_register(return_register), _function_pointer(function_pointer)
    {
    }

    FcallInstruction(FcallInstruction &&) noexcept = default;
    FcallInstruction(const FcallInstruction &) = default;

    ~FcallInstruction() override = default;

    FcallInstruction &operator=(FcallInstruction &&) noexcept = default;

    [[nodiscard]] std::optional<Operand> return_register() const noexcept { return _return_register; }
    [[nodiscard]] bool has_return() const noexcept { return _return_register.has_value(); }
    [[nodiscard]] std::uintptr_t function_pointer() const noexcept { return _function_pointer; }
    [[nodiscard]] const std::vector<Operand> &arguments() const noexcept { return _arguments; }
    [[nodiscard]] std::vector<Operand> &arguments() noexcept { return _arguments; }

    [[nodiscard]] std::string to_string() const override
    {
        std::string call;
        if (has_return())
        {
            call = fmt::format("call {},{}", _return_register->to_string(), _function_pointer);
        }
        else
        {
            call = fmt::format("call {}", _function_pointer);
        }

        for (auto argument : _arguments)
        {
            call = fmt::format("{},{}", std::move(call), argument.to_string());
        }

        return call;
    }

private:
    std::optional<Operand> _return_register{std::nullopt};
    std::uintptr_t _function_pointer;
    std::vector<Operand> _arguments;
};

class CallInstruction final : public NullaryOperandInstruction<InstructionType::Call>
{
public:
    explicit CallInstruction(const std::uintptr_t function_pointer) : _function_pointer(function_pointer) {}

    CallInstruction(CallInstruction &&) noexcept = default;
    CallInstruction(const CallInstruction &) = default;

    ~CallInstruction() override = default;

    CallInstruction &operator=(CallInstruction &&) noexcept = default;

    [[nodiscard]] std::uintptr_t function_pointer() const noexcept { return _function_pointer; }

    [[nodiscard]] std::string to_string() const override { return fmt::format("call {}", _function_pointer); }

private:
    std::optional<Operand> _return_register{std::nullopt};
    std::uintptr_t _function_pointer;
    std::vector<Operand> _arguments;
};

class AlignInstruction final : public NullaryOperandInstruction<InstructionType::Align>
{
public:
    explicit AlignInstruction(const std::uint8_t alignment) : _alignment(alignment) {}

    AlignInstruction(AlignInstruction &&) noexcept = default;
    AlignInstruction(const AlignInstruction &) = default;

    ~AlignInstruction() override = default;

    AlignInstruction &operator=(AlignInstruction &&) noexcept = default;

    [[nodiscard]] std::uint8_t alignment() const noexcept { return _alignment; }

    [[nodiscard]] std::string to_string() const override { return fmt::format("align {}", _alignment); }

private:
    std::uint8_t _alignment;
};

using Instruction =
    std::variant<VregInstruction, ClearInstruction, GetArgumentInstruction, SetReturnArgumentInstruction,
                 CommentInstruction, ContextBeginInstruction, ContextEndInstruction, BranchBeginInstruction,
                 BranchEndInstruction, RetInstruction, CqoInstruction, NopInstruction, PopInstruction, PushInstruction,
                 JumpInstruction, SectionInstruction, IncInstruction, DecInstruction, TestInstruction, SetneInstruction,
                 SeteInstruction, LeaInstruction, PrefetchInstruction, IdivInstruction, CmpInstruction, MovInstruction,
                 CmovleInstruction, CmovgeInstruction, AddInstruction, XaddInstruction, SubInstruction, ImulInstruction,
                 AndInstruction, OrInstruction, XorInstruction, ShlInstruction, ShrInstruction, Crc32Instruction,
                 FdivInstruction, FmodInstruction, FcallInstruction, CallInstruction, AlignInstruction>;

template <typename T, typename V> struct is_in_variant;

template <typename T> struct is_in_variant<T, std::variant<>> : std::false_type
{
};

template <typename T, typename U, typename... Ts>
struct is_in_variant<T, std::variant<U, Ts...>> : is_in_variant<T, std::variant<Ts...>>
{
};

template <typename T, typename... Ts> struct is_in_variant<T, std::variant<T, Ts...>> : std::true_type
{
};

template <class T>
concept is_instruction = is_in_variant<T, Instruction>::value;
} // namespace flounder