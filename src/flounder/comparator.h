#pragma once

#include "program.h"
#include <flounder/ir/instructions.h>
#include <utility>

namespace flounder {
class Comparator
{
public:
    Comparator(Operand left, Operand right, const bool is_likely) noexcept
        : _left(left), _right(right), _is_likely(is_likely)
    {
    }

    virtual ~Comparator() noexcept = default;
    virtual std::pair<CmpInstruction, JumpInstruction> emit(Program &program,
                                                            Label jump_destination) const noexcept = 0;
    [[nodiscard]] virtual Comparator &invert() noexcept = 0;

    [[nodiscard]] Operand left() const noexcept { return _left; }
    [[nodiscard]] Operand right() const noexcept { return _right; }
    [[nodiscard]] bool is_likely() const noexcept { return _is_likely; }

private:
    Operand _left;
    Operand _right;
    bool _is_likely;
};

class IsEquals final : public Comparator
{
public:
    IsEquals(Operand left, Operand right, const bool is_likely = true) noexcept : Comparator(left, right, is_likely) {}

    ~IsEquals() noexcept override = default;

    std::pair<CmpInstruction, JumpInstruction> emit(Program &program, Label jump_destination) const noexcept override
    {
        auto compare = program.cmp(left(), right(), Comparator::is_likely());
        auto jump = program.je(jump_destination);
        program << compare << jump;
        return std::make_pair(compare, jump);
    }

    [[nodiscard]] Comparator &invert() noexcept override;
};

class IsNotEquals final : public Comparator
{
public:
    IsNotEquals(Operand left, Operand right, const bool is_likely = true) noexcept : Comparator(left, right, is_likely)
    {
    }

    ~IsNotEquals() noexcept override = default;

    std::pair<CmpInstruction, JumpInstruction> emit(Program &program, Label jump_destination) const noexcept override
    {
        auto compare = program.cmp(left(), right(), Comparator::is_likely());
        auto jump = program.jne(jump_destination);
        program << compare << jump;
        return std::make_pair(compare, jump);
    }

    [[nodiscard]] Comparator &invert() noexcept override;
};

class IsLower final : public Comparator
{
public:
    IsLower(Operand left, Operand right, const bool is_likely = true) noexcept : Comparator(left, right, is_likely) {}

    ~IsLower() noexcept override = default;

    std::pair<CmpInstruction, JumpInstruction> emit(Program &program, Label jump_destination) const noexcept override
    {
        auto compare = program.cmp(left(), right(), Comparator::is_likely());
        auto jump = program.jl(jump_destination);
        program << compare << jump;
        return std::make_pair(compare, jump);
    }

    [[nodiscard]] Comparator &invert() noexcept override;
};

class IsLowerEquals final : public Comparator
{
public:
    IsLowerEquals(Operand left, Operand right, const bool is_likely = true) noexcept
        : Comparator(left, right, is_likely)
    {
    }

    ~IsLowerEquals() noexcept override = default;

    std::pair<CmpInstruction, JumpInstruction> emit(Program &program, Label jump_destination) const noexcept override
    {
        auto compare = program.cmp(left(), right(), Comparator::is_likely());
        auto jump = program.jle(jump_destination);
        program << compare << jump;
        return std::make_pair(compare, jump);
    }

    [[nodiscard]] Comparator &invert() noexcept override;
};

class IsGreater final : public Comparator
{
public:
    IsGreater(Operand left, Operand right, const bool is_likely = true) noexcept : Comparator(left, right, is_likely) {}

    ~IsGreater() noexcept override = default;

    std::pair<CmpInstruction, JumpInstruction> emit(Program &program, Label jump_destination) const noexcept override
    {
        auto compare = program.cmp(left(), right(), Comparator::is_likely());
        auto jump = program.jg(jump_destination);
        program << compare << jump;
        return std::make_pair(compare, jump);
    }

    [[nodiscard]] Comparator &invert() noexcept override;
};

class IsGreaterEquals final : public Comparator
{
public:
    IsGreaterEquals(Operand left, Operand right, const bool is_likely = true) noexcept
        : Comparator(left, right, is_likely)
    {
    }

    ~IsGreaterEquals() noexcept override = default;

    std::pair<CmpInstruction, JumpInstruction> emit(Program &program, Label jump_destination) const noexcept override
    {
        auto compare = program.cmp(left(), right(), Comparator::is_likely());
        auto jump = program.jge(jump_destination);
        program << compare << jump;
        return std::make_pair(compare, jump);
    }

    [[nodiscard]] Comparator &invert() noexcept override;
};

} // namespace flounder