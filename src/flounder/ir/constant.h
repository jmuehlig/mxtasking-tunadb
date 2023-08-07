#pragma once

#include "register.h"
#include <cstdint>
#include <variant>

namespace flounder {
class Constant
{
public:
    using value_t = std::variant<std::int64_t, std::int32_t, std::int16_t, std::int8_t, std::uintptr_t>;

    constexpr explicit Constant(const std::int64_t value) noexcept : _constant(value) {}
    constexpr explicit Constant(const std::uintptr_t address) noexcept : _constant(address) {}
    constexpr explicit Constant(const std::int32_t value) noexcept : _constant(value) {}
    constexpr explicit Constant(const std::int16_t value) noexcept : _constant(value) {}
    constexpr explicit Constant(const std::int8_t value) noexcept : _constant(value) {}
    constexpr explicit Constant(const value_t value) noexcept : _constant(value) {}

    ~Constant() noexcept = default;

    [[nodiscard]] value_t value() const noexcept { return _constant; }
    [[nodiscard]] std::int64_t value_as_int64() const noexcept
    {
        auto value = std::int64_t(0U);
        std::visit([&value](const auto v) { value = v; }, _constant);
        return value;
    }
    void value(const value_t value) noexcept { _constant = value; }

    [[nodiscard]] RegisterWidth width() const noexcept
    {
        if (std::holds_alternative<std::int32_t>(_constant))
        {
            return RegisterWidth::r32;
        }

        if (std::holds_alternative<std::int16_t>(_constant))
        {
            return RegisterWidth::r16;
        }

        if (std::holds_alternative<std::int8_t>(_constant))
        {
            return RegisterWidth::r8;
        }

        return RegisterWidth::r64;
    }

    [[nodiscard]] std::string to_string() const
    {
        auto str = std::string{};
        std::visit([&str](const auto &constant) { str = std::to_string(constant); }, _constant);

        return str;
    }

private:
    value_t _constant;
};
} // namespace flounder