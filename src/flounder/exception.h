#pragma once

#include <exception>
#include <flounder/ir/instructions.h>
#include <fmt/core.h>

namespace flounder {
class Exception : public std::exception
{
public:
    ~Exception() noexcept override = default;

    [[nodiscard]] const char *what() const noexcept override { return _message.c_str(); }

protected:
    explicit Exception(std::string &&message) noexcept : _message(std::move(message)) {}

private:
    const std::string _message;
};

class NotImplementedException final : public Exception
{
public:
    explicit NotImplementedException(std::string &&functionality)
        : Exception(fmt::format("'{}' is not implemented", std::move(functionality)))
    {
    }

    ~NotImplementedException() noexcept override = default;
};

class VirtualRegisterAlreadyInUseException final : public Exception
{
public:
    explicit VirtualRegisterAlreadyInUseException(Register reg)
        : Exception(fmt::format("The vreg '{}' is already in use.", reg.to_string()))
    {
    }

    ~VirtualRegisterAlreadyInUseException() noexcept override = default;
};

class CanNotFindVirtualRegisterException final : public Exception
{
public:
    explicit CanNotFindVirtualRegisterException(Register reg)
        : Exception(fmt::format("Could not find register for vreg '{}'.", reg.to_string()))
    {
    }

    ~CanNotFindVirtualRegisterException() noexcept override = default;
};

class CanNotFindSpilledValueException final : public Exception
{
public:
    explicit CanNotFindSpilledValueException(Register reg)
        : Exception(fmt::format("Could not find spilled value for vreg '{}'.", reg.to_string()))
    {
    }

    ~CanNotFindSpilledValueException() noexcept override = default;
};

class CompilationException final : public Exception
{
public:
    explicit CompilationException(std::string &&message)
        : Exception(fmt::format("Could not translate flounder into asm: {}", std::move(message)))
    {
    }

    ~CompilationException() noexcept override = default;
};

class UnknownRegisterException final : public Exception
{
public:
    UnknownRegisterException(const std::uint8_t register_id, const std::uint8_t width)
        : Exception(fmt::format("Unknown machine register (id: {}, width: {}).", std::uint16_t(register_id),
                                std::uint16_t(width)))
    {
    }

    ~UnknownRegisterException() noexcept override = default;
};

class NotEnoughTemporaryRegistersException final : public Exception
{
public:
    NotEnoughTemporaryRegistersException() : Exception("Not enough temporary registers for spilling.") {}

    ~NotEnoughTemporaryRegistersException() noexcept override = default;
};

class CanNotTranslateOperandException final : public Exception
{
public:
    explicit CanNotTranslateOperandException(Operand operand)
        : Exception(fmt::format("Can not translate operand: {}", operand.to_string()))
    {
    }

    ~CanNotTranslateOperandException() noexcept override = default;
};

class CanNotTranslateInstructionException final : public Exception
{
public:
    explicit CanNotTranslateInstructionException(const InstructionInterface &instruction)
        : Exception(fmt::format("Can not translate instruction: {}", instruction.to_string()))
    {
    }

    ~CanNotTranslateInstructionException() noexcept override = default;
};
} // namespace flounder