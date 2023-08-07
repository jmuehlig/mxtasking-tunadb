#pragma once

#include "constant.h"
#include "register.h"
#include <cstdint>
#include <fmt/core.h>
#include <optional>
#include <string>
#include <variant>

namespace flounder {
class MemoryAddress
{
public:
    MemoryAddress(Register base_register, std::optional<Register> index_register, const std::uint8_t scale,
                  const std::int32_t displacement, const std::optional<RegisterWidth> access_width) noexcept
        : _base(base_register), _index(index_register), _scale(scale), _displacement(displacement),
          _access_width(access_width)
    {
    }

    MemoryAddress(Register base_register, const std::int32_t displacement) noexcept
        : MemoryAddress(base_register, std::nullopt, 0, displacement, std::nullopt)
    {
    }

    MemoryAddress(Register base_register, const std::int32_t displacement, const RegisterWidth width) noexcept
        : MemoryAddress(base_register, std::nullopt, 0, displacement, width)
    {
    }

    MemoryAddress(const Constant base_address, std::optional<Register> index_register, const std::uint8_t scale,
                  const std::int32_t displacement, const std::optional<RegisterWidth> access_width) noexcept
        : _base(base_address), _index(index_register), _scale(scale), _displacement(displacement),
          _access_width(access_width)
    {
    }

    MemoryAddress(const Constant base_address) noexcept : MemoryAddress(base_address, std::nullopt, 0, 0, std::nullopt)
    {
    }

    MemoryAddress(const Constant base_address, const RegisterWidth width) noexcept
        : MemoryAddress(base_address, std::nullopt, 0, 0, width)
    {
    }

    MemoryAddress(MemoryAddress &&) noexcept = default;
    MemoryAddress(const MemoryAddress &) noexcept = default;

    ~MemoryAddress() noexcept = default;

    MemoryAddress &operator=(MemoryAddress &&) noexcept = default;
    MemoryAddress &operator=(const MemoryAddress &) noexcept = default;

    [[nodiscard]] const std::variant<Register, Constant> &base() const noexcept { return _base; }
    [[nodiscard]] std::variant<Register, Constant> &base() noexcept { return _base; }
    [[nodiscard]] bool has_index() const noexcept { return _index.has_value(); }
    [[nodiscard]] const std::optional<Register> &index() const noexcept { return _index; }
    [[nodiscard]] std::optional<Register> &index() noexcept { return _index; }
    [[nodiscard]] bool has_scale() const noexcept { return _scale > 0U; }
    [[nodiscard]] std::uint8_t scale() const noexcept { return _scale; }
    [[nodiscard]] bool has_displacement() const noexcept { return _displacement != 0; }
    [[nodiscard]] std::int32_t displacement() const noexcept { return _displacement; }
    [[nodiscard]] std::optional<RegisterWidth> width() const noexcept { return _access_width; }

    [[nodiscard]] std::string to_string() const
    {
        std::string mem;
        if (std::holds_alternative<Register>(_base))
        {
            mem = std::get<Register>(_base).to_string();
        }
        else if (std::holds_alternative<Constant>(_base))
        {
            mem = std::get<Constant>(_base).to_string();
        }

        if (_index.has_value())
        {
            if (_scale > 0U)
            {
                mem += fmt::format("+{}*{}", _index->to_string(), std::uint16_t(_scale));
            }
            else
            {
                mem += fmt::format("+{}", _index->to_string());
            }
        }

        if (_displacement > 0)
        {
            mem += fmt::format("+{}", _displacement);
        }
        else if (_displacement < 0)
        {
            mem += fmt::format("{}", _displacement);
        }

        if (_access_width.has_value())
        {
            return fmt::format("[{}]::{}", std::move(mem), static_cast<std::uint16_t>(_access_width.value()));
        }

        return fmt::format("[{}]", std::move(mem));
    }

private:
    std::variant<Register, Constant> _base;
    std::optional<Register> _index;
    std::uint8_t _scale{0U};
    std::int32_t _displacement{0};
    std::optional<RegisterWidth> _access_width;
};
} // namespace flounder