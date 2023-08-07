#pragma once

#include "annotations.h"
#include "comparator.h"
#include "program.h"
#include <cstdint>
#include <fmt/core.h>
#include <iostream>
#include <variant>

namespace flounder {
class If
{
public:
    template <class C>
    If(Program &program, C &&comparator, std::string &&name = "if") noexcept
        : _program(program), _foot_label(program.label(fmt::format("end_{}_{}", std::move(name), program.next_id())))
    {
        /// Emit the condition.
        comparator.invert().emit(program, _foot_label);

        //        _annotation = BranchAnnotation{program.begin_branch(if_id), program.end_branch()};

        /// Mark the beginning of the branch.
        //        program << _annotation.begin_marker();
    }

    ~If() noexcept
    {
        _program /*<< _annotation.end_marker()*/ << _program.section(_foot_label);
        //        _program.annotate(_annotation.begin_marker(),
        //        std::make_unique<BranchAnnotation>(std::move(_annotation)));
    }

    [[nodiscard]] Label foot_label() const noexcept { return _foot_label; }

    //    void likeliness(const float likeliness) noexcept { _annotation.likeliness(likeliness); }
    //    void mark_likely() noexcept { _annotation.mark_likely(); }
    //    void mark_unlikely() noexcept { _annotation.mark_unlikely(); }

private:
    Program &_program;
    Label _foot_label;
    //    BranchAnnotation _annotation;
};

template <class C> class While
{
public:
    While(Program &program, C &&comparator, std::string &&name = "while_loop")
        : _program(program), _id(program.next_id()), _head_label(program.label(fmt::format("begin_{}_{}", name, _id))),
          _foot_label(program.label(fmt::format("end_{}_{}", name, _id))), _comparator(std::move(comparator))
    {
        _comparator.invert().emit(program, _foot_label);

        program << program.section(_head_label)
            //                << _annotation.begin_marker()
            ;
    }

    ~While() noexcept
    {
        _comparator.emit(_program, _head_label);
        _program << _program.section(_foot_label);
        //        _program.annotate(_annotation.begin_marker(),
        //        std::make_unique<LoopAnnotation>(std::move(_annotation)));
    }

private:
    Program &_program;
    std::uint64_t _id;
    Label _head_label;
    Label _foot_label;
    C _comparator;
};

template <class C> class DoWhile
{
public:
    DoWhile(Program &program, C &&comparator, std::string &&name = "while_loop")
        : _program(program), _id(program.next_id()), _head_label(program.label(fmt::format("begin_{}_{}", name, _id))),
          _foot_label(program.label(fmt::format("end_{}_{}", name, _id))), _comparator(std::move(comparator))
    {
        program << program.section(_head_label)
            //                << _annotation.begin_marker()
            ;
    }

    ~DoWhile() noexcept
    {
        _comparator.emit(_program, _head_label);
        _program << _program.section(_foot_label);
        //        _program.annotate(_annotation.begin_marker(),
        //        std::make_unique<LoopAnnotation>(std::move(_annotation)));
    }

    [[nodiscard]] flounder::Label foot_label() const noexcept { return _foot_label; }

private:
    Program &_program;
    std::uint64_t _id;
    Label _head_label;
    Label _foot_label;
    C _comparator;
};

class For
{
public:
    template <class C>
    For(Program &program, C &&comparator, Instruction &&step, std::string &&name = "for_loop")
        : _program(program), _id(program.next_id()), _step(std::move(step)),
          _head_label(program.label(fmt::format("begin_{}_{}", name, _id))),
          _step_label(program.label(fmt::format("step_{}_{}", name, _id))),
          _foot_label(program.label(fmt::format("end_{}_{}", std::move(name), _id)))
    {
        program << program.section(_head_label);
        comparator.invert().emit(program, _foot_label);
    }

    ~For() noexcept
    {
        _program << _program.section(_step_label) << std::move(_step) << _program.jmp(_head_label)
                 << _program.section(_foot_label);
    }

    [[nodiscard]] Label step_label() const noexcept { return _step_label; }
    [[nodiscard]] Label foot_label() const noexcept { return _foot_label; }

private:
    Program &_program;
    std::uint64_t _id;
    Instruction _step;
    Label _head_label;
    Label _step_label;
    Label _foot_label;
};

class ForEach
{
public:
    ForEach(Program &program, Register begin, Register end, const std::uint32_t item_size,
            std::string &&name = "foreach_loop")
        : _program(program), _id(program.next_id()), _head_label(program.label(fmt::format("begin_{}_{}", name, _id))),
          _step_label(program.label(fmt::format("step_{}_{}", name, _id))),
          _foot_label(program.label(fmt::format("end_{}_{}", std::move(name), _id))), _begin_vreg(begin),
          _end_vreg(end), _item_size(item_size)
    {
        /// First check, if the loop will not be entered.
        program << program.cmp(begin, end)
                << program.jge(_foot_label)

                /// Head of the loop (body follows after, conditional jump after step back here).
                << program.section(_head_label);
    }

    ~ForEach() noexcept
    {
        _program
            /// Increment the counter
            << _program.section(_step_label)
            << _program.add(_begin_vreg, _program.constant32(_item_size))

            /// Check if we go back to the body
            << _program.cmp(_begin_vreg, _end_vreg)
            << _program.jl(_head_label)

            /// If not, end the loop.
            << _program.section(_foot_label);
    }

    [[nodiscard]] Label step_label() const noexcept { return _step_label; }
    [[nodiscard]] Label foot_label() const noexcept { return _foot_label; }

private:
    Program &_program;
    std::uint64_t _id;
    Label _head_label;
    Label _step_label;
    Label _foot_label;
    Register _begin_vreg;
    Register _end_vreg;
    std::uint32_t _item_size;
};

class ForRange
{
public:
    ForRange(Program &program, const std::uint64_t init, Operand end, std::string &&name = "for_range",
             std::optional<std::uint8_t> unrollable_iterations = std::nullopt)
        : _program(program), _id(program.next_id()), _head_label(program.label(fmt::format("begin_{}_{}", name, _id))),
          _step_label(program.label(fmt::format("step_{}_{}", name, _id))),
          _foot_label(program.label(fmt::format("end_{}_{}", name, _id))),
          _counter_vreg(program.vreg(fmt::format("{}_counter_{}", std::move(name), _id))), _end_operand(end)
    {

        /// Initialize the counter.
        program << program.request_vreg64(_counter_vreg);
        if (init == 0U)
        {
            program << program.xor_(_counter_vreg, _counter_vreg);
        }
        else
        {
            program << program.mov(_counter_vreg, program.constant64(init));
        }

        /// First check, if the loop will not be entered.
        /// If the end is a constant, we can check that at
        /// query compile time and jump directly.
        /// If the end is a constant and the loop will be
        /// entered, we can skip that check.
        if (end.is_constant())
        {
            if (std::int64_t(init) >= end.constant().value_as_int64())
            {
                program << program.jmp(_foot_label);
            }
        }
        else
        {
            auto cmp = program.cmp(Operand{_counter_vreg}, end);
            if (unrollable_iterations.has_value())
            {
                cmp.unrollable_iterations(unrollable_iterations.value());
            }
            program << cmp << program.jge(_foot_label);
        }

        /// Head of the loop (body follows after, conditional jump after step back here).
        program << program.section(_head_label);
    }

    ForRange(Program &program, const std::uint64_t init, const std::uint64_t end, std::string &&name = "for_range")
        : ForRange(program, init, Operand{program.constant64(end)}, std::move(name))
    {
    }

    ~ForRange() noexcept
    {
        _program
            /// Increment the counter
            << _program.section(_step_label)
            << _program.add(_counter_vreg, _program.constant8(1))

            /// Check if we go back to the body
            << _program.cmp(Operand{_counter_vreg}, _end_operand)
            << _program.jl(_head_label)

            /// If not, end the loop.
            << _program.section(_foot_label) << _program.clear(_counter_vreg);
    }

    [[nodiscard]] Register counter_vreg() noexcept { return _counter_vreg; }
    [[nodiscard]] Label step_label() const noexcept { return _step_label; }
    [[nodiscard]] Label foot_label() const noexcept { return _foot_label; }

private:
    Program &_program;
    std::uint64_t _id;
    Label _head_label;
    Label _step_label;
    Label _foot_label;
    Register _counter_vreg;
    Operand _end_operand;
};

class FunctionCall
{
public:
    FunctionCall(Program &program, const std::uintptr_t function_pointer) noexcept
        : _program(program), _function_pointer(function_pointer)
    {
    }

    FunctionCall(Program &program, const std::uintptr_t function_pointer, std::string &&return_vreg_name) noexcept
        : _program(program), _function_pointer(function_pointer), _return_value(std::move(return_vreg_name))
    {
    }

    FunctionCall(Program &program, const std::uintptr_t function_pointer, Register return_vreg) noexcept
        : _program(program), _function_pointer(function_pointer), _return_value(return_vreg)
    {
    }

    ~FunctionCall() = default;

    std::optional<Register> call(std::vector<Operand> &&arguments = {})
    {
        auto return_register = std::optional<Register>{std::nullopt};

        if (std::holds_alternative<std::string>(_return_value))
        {
            return_register = _program.vreg(std::move(std::get<std::string>(_return_value)));
            _program << _program.request_vreg64(return_register.value());
        }
        else if (std::holds_alternative<Register>(_return_value))
        {
            return_register = std::get<Register>(_return_value);
        }

        auto call_node = return_register.has_value() ? _program.fcall(_function_pointer, return_register.value())
                                                     : _program.fcall(_function_pointer);

        for (auto &&argument : arguments)
        {
            call_node.arguments().emplace_back(std::move(argument));
        }

        _program << call_node;

        return return_register;
    }

private:
    Program &_program;
    std::uintptr_t _function_pointer;
    std::variant<Register, std::string, std::monostate> _return_value{std::monostate{}};
};
} // namespace flounder