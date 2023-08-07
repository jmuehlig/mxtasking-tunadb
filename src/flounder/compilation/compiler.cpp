#include "compiler.h"
#include <iostream>

using namespace flounder;

bool Compiler::compile(Program &program, Executable &executable)
{
    /// Allocate registers.
    this->_register_assigner.process(program, this->_is_keep_compiled_code);

    /// Optimize allocated code.
    this->_optimizer.optimize(program);

    return this->translate(program, executable);
}

bool Compiler::translate(Program &program, Executable &executable)
{
    /// Init asmjit to prepare asm emitter.
    executable.runtime()._environment.setArch(asmjit::Arch::kX64);
    auto code = asmjit::CodeHolder{};
    code.init(executable.runtime().environment());

    auto error_handler = ExceptionErrorHandler{};
    code.setErrorHandler(&error_handler);

    auto assembler = asmjit::x86::Assembler{&code};

    auto logger = std::unique_ptr<asmjit::Logger>{nullptr};
    ContextLogger *context_logger = nullptr;
    if (this->_is_keep_compiled_code) [[unlikely]]
    {
        logger = std::make_unique<CompilationLogger>(assembler, executable.compilate());
        code.setLogger(logger.get());
        context_logger = dynamic_cast<ContextLogger *>(logger.get());
    }

    /// Compile all parts of the program.
    auto operand_translator = OperandTranslator{};
    this->translate(program.arguments(), operand_translator, assembler, context_logger);
    this->translate(program.header(), operand_translator, assembler, context_logger);
    this->translate(program.body(), operand_translator, assembler, context_logger);

    executable.code_size(code.codeSize());
    const auto error = executable.add(code);
    if (error > 0U)
    {
        return false;
    }

    /// Save the emitted code as string, if requested.
    if (this->_is_keep_compiled_code) [[unlikely]]
    {
        executable.compilate().align_to_base(executable.base());
    }

    return true;
}

void Compiler::translate(InstructionSet &code, OperandTranslator &operand_translator, asmjit::x86::Assembler &assembler,
                         ContextLogger *context_logger)
{
    auto translator =
        InstructionTranslator{operand_translator, assembler, context_logger, this->_is_keep_compiled_code};

    auto flounder_as_comment = std::optional<std::string>{std::nullopt};

    for (auto &instruction : code.lines())
    {
        std::visit(
            [&translator, &assembler, &flounder_as_comment, is_keep_code = this->_is_keep_compiled_code](auto &instr) {
                if (is_keep_code) [[unlikely]]
                {
                    flounder_as_comment = instr.inline_comment();
                    assembler.setInlineComment(flounder_as_comment.has_value() ? flounder_as_comment.value().c_str()
                                                                               : nullptr);
                }

                std::ignore = translator.translate(instr);
            },
            instruction);
    }
}
