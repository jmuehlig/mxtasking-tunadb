#pragma once

#include "constant.h"
#include "memory.h"
#include "register.h"
#include <string>
#include <variant>

namespace flounder {
class Operand
{
public:
    explicit Operand(Register reg) noexcept : _operand(reg) {}
    explicit Operand(MemoryAddress mem) noexcept : _operand(mem) {}
    explicit Operand(Constant constant) noexcept : _operand(constant) {}
    Operand(Operand &&) noexcept = default;
    Operand(const Operand &) = default;

    ~Operand() noexcept = default;

    Operand &operator=(Operand &&) noexcept = default;
    Operand &operator=(const Operand &) noexcept = default;
    Operand &operator=(MemoryAddress memory_address) noexcept
    {
        _operand = memory_address;
        return *this;
    }
    Operand &operator=(Register reg) noexcept
    {
        _operand = reg;
        return *this;
    }

    [[nodiscard]] bool is_reg() const noexcept { return std::holds_alternative<Register>(_operand); }
    [[nodiscard]] bool is_mem() const noexcept { return std::holds_alternative<MemoryAddress>(_operand); }
    [[nodiscard]] bool is_constant() const noexcept { return std::holds_alternative<Constant>(_operand); }

    [[nodiscard]] const Register &reg() const noexcept { return std::get<Register>(_operand); }
    [[nodiscard]] Register &reg() noexcept { return std::get<Register>(_operand); }
    [[nodiscard]] const MemoryAddress &mem() const noexcept { return std::get<MemoryAddress>(_operand); }
    [[nodiscard]] MemoryAddress &mem() noexcept { return std::get<MemoryAddress>(_operand); }
    [[nodiscard]] Constant &constant() noexcept { return std::get<Constant>(_operand); }
    [[nodiscard]] const Constant &constant() const noexcept { return std::get<Constant>(_operand); }

    [[nodiscard]] std::string to_string() const
    {
        if (is_reg())
        {
            return reg().to_string();
        }

        if (is_mem())
        {
            return mem().to_string();
        }

        return constant().to_string();
    }

private:
    std::variant<Register, MemoryAddress, Constant> _operand;
};
} // namespace flounder