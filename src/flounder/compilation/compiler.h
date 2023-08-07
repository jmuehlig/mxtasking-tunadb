#pragma once
#include "register_assigner.h"
#include "translator.h"
#include <cstdint>
#include <flounder/executable.h>
#include <flounder/optimization/optimizer.h>
#include <flounder/program.h>
#include <stack>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace flounder {

class CompilationLogger final : public asmjit::Logger, public ContextLogger
{
public:
    CompilationLogger(asmjit::x86::Assembler &assembler, Compilate &compilate) noexcept
        : _assembler(assembler), _compilate(compilate)
    {
    }

    ~CompilationLogger() noexcept override = default;

    ASMJIT_API asmjit::Error _log(const char *asm_instruction,
                                  const size_t asm_instruction_size = SIZE_MAX) noexcept override
    {
        _last_offsets.push(_assembler.offset());

        /// If we had two times the equal offset, the last instruction is
        /// a section that has no offset. Remove that offset emitted last time.
        if (_last_offsets.has_changed() == false)
        {
            _compilate.remove_last_offset(_last_offsets.front());
            if (_context_stack.empty() == false)
            {
                auto &offsets = std::get<1>(_context_stack.top());
                if (offsets.empty() == false)
                {
                    if (offsets.back() == _last_offsets.front())
                    {
                        offsets.erase(offsets.end() - 1U);
                    }
                }
            }
        }

        auto assembly_line = std::string{asm_instruction, asm_instruction_size};
        if (assembly_line.back() == '\n')
        {
            assembly_line.pop_back();
        }

        _compilate.emplace_back(_last_offsets.front(), std::move(assembly_line));

        /// Add the offset to the current context.
        if (_context_stack.empty() == false)
        {
            std::get<1>(_context_stack.top()).emplace_back(_last_offsets.front());
        }

        return asmjit::ErrorCode::kErrorOk;
    }

    void begin_context(const std::string &context) override
    {
        if (_context_stack.empty() == false)
        {
            auto &interrupted_context = _context_stack.top();
            if (std::get<1>(interrupted_context).empty() == false)
            {
                _compilate.emplace_back(std::get<0>(interrupted_context), std::get<1>(interrupted_context));
                std::get<1>(interrupted_context).clear();
            }
        }

        auto offsets = std::vector<std::uint32_t>{};
        offsets.reserve(128U);
        _context_stack.push(std::make_pair(context, std::move(offsets)));
    }

    void end_context(const std::string &context) override
    {
        if (_context_stack.empty() == false)
        {
            auto &current_context = _context_stack.top();
            if (std::get<0>(current_context) == context)
            {
                if (std::get<1>(current_context).empty() == false)
                {
                    _compilate.emplace_back(context, std::get<1>(current_context));
                }
            }

            _context_stack.pop();
        }
    }

private:
    class OffsetHistory
    {
    public:
        constexpr OffsetHistory() noexcept = default;
        ~OffsetHistory() noexcept = default;

        void push(const std::uint64_t offset) noexcept
        {
            _stack[1U] = _stack[0U];
            _stack[0U] = offset;
        }

        [[nodiscard]] std::uint64_t front() const noexcept { return _stack[0U]; }
        [[nodiscard]] std::uint64_t back() const noexcept { return _stack[1U]; }
        [[nodiscard]] bool has_changed() const noexcept { return front() != back(); }

    private:
        std::array<std::uint64_t, 2U> _stack{0U, 0U};
    };

    asmjit::x86::Assembler &_assembler;
    Compilate &_compilate;

    OffsetHistory _last_offsets;

    std::stack<std::pair<std::string, std::vector<std::uint32_t>>> _context_stack;
};

class ExceptionErrorHandler final : public asmjit::ErrorHandler
{
public:
    ExceptionErrorHandler() noexcept = default;
    ~ExceptionErrorHandler() noexcept override = default;

    void handleError(const asmjit::Error /*error*/, const char *message,
                     asmjit::BaseEmitter * /*base_emitter*/) override
    {
        throw CompilationException{std::string{message}};
    }
};

class Compiler
{
public:
    Compiler(const bool is_profile, const bool is_keep_compiled_code)
        : _is_profile(is_profile), _is_keep_compiled_code(is_keep_compiled_code)
    {
    }

    ~Compiler() noexcept = default;

    /**
     * Compiles the given (flounder) program to asm into the given executable.
     *
     * @param program Flounder program to compile.
     * @param executable Target executable.
     * @return True, when compilation was successfull.
     */
    [[nodiscard]] bool compile(Program &program, Executable &executable);

    /**
     * Translates the given (flounder) program to asm into the given executable.
     * The program must be register allocated; translation is one step within compilation.
     *
     * @param program Flounder program to translate.
     * @param executable Target executable.
     * @return  True, when translation was successfull.
     */
    [[nodiscard]] bool translate(Program &program, Executable &executable);

private:
    const bool _is_profile;
    const bool _is_keep_compiled_code;

    /// Register assigner.
    RegisterAssigner _register_assigner;

    /// Optimizer
    PostRegisterAllocationOptimizer _optimizer;

    void translate(InstructionSet &code, OperandTranslator &operand_translator, asmjit::x86::Assembler &assembler,
                   ContextLogger *context_logger);
};
} // namespace flounder