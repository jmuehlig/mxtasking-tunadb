#pragma once

#include <cstdint>
#include <fmt/core.h>
#include <optional>
#include <string>

namespace flounder {
enum RegisterWidth : std::uint8_t
{
    r8 = 8U,
    r16 = 16U,
    r32 = 32U,
    r64 = 64U
};

enum RegisterSignType : std::uint8_t
{
    Signed,
    Unsigned
};

template <typename T> struct register_width_t
{
    constexpr static RegisterWidth value() { return RegisterWidth::r64; }
};

template <> struct register_width_t<std::uint8_t>
{
    constexpr static RegisterWidth value() { return RegisterWidth::r8; }
};

template <> struct register_width_t<std::int8_t>
{
    constexpr static RegisterWidth value() { return RegisterWidth::r8; }
};

template <> struct register_width_t<std::uint16_t>
{
    constexpr static RegisterWidth value() { return RegisterWidth::r16; }
};

template <> struct register_width_t<std::int16_t>
{
    constexpr static RegisterWidth value() { return RegisterWidth::r16; }
};

template <> struct register_width_t<std::uint32_t>
{
    constexpr static RegisterWidth value() { return RegisterWidth::r32; }
};

template <> struct register_width_t<std::int32_t>
{
    constexpr static RegisterWidth value() { return RegisterWidth::r32; }
};

class Register
{
public:
    Register(std::string_view name, const bool is_accessed_frequently) noexcept
        : _virtual_name(name), _is_accessed_frequently(is_accessed_frequently)
    {
    }

    Register(std::string_view name, const bool is_accessed_frequently, const RegisterSignType sign_type) noexcept
        : _virtual_name(name), _is_accessed_frequently(is_accessed_frequently), _sign_type(sign_type)
    {
    }

    Register(std::string_view name) noexcept : Register(name, true) {}

    Register(std::string_view name, const RegisterSignType sign_type) noexcept : Register(name, true, sign_type) {}

    Register(const std::uint8_t machine_register_id, const RegisterWidth width)
        : _machine_register_id(machine_register_id), _width(width)
    {
    }

    Register(const std::uint8_t machine_register_id, const RegisterWidth width, const RegisterSignType sign_type)
        : _machine_register_id(machine_register_id), _width(width), _sign_type(sign_type)
    {
    }

    Register(Register &&) noexcept = default;
    Register(const Register &) noexcept = default;

    ~Register() = default;

    Register &operator=(Register &&) noexcept = default;
    Register &operator=(const Register &) noexcept = default;

    //    void assign(const std::uint8_t machine_register_id, const RegisterWidth width) noexcept
    //    {
    //        _machine_register_id = machine_register_id;
    //        _width = width;
    //    }

    void assign(const std::uint8_t machine_register_id, const RegisterWidth width,
                const RegisterSignType sign_type) noexcept
    {
        _machine_register_id = machine_register_id;
        _width = width;
        _sign_type = sign_type;
    }

    void assign(Register machine_register) noexcept
    {
        _machine_register_id = machine_register._machine_register_id.value();
        _width = machine_register._width;
        _sign_type = machine_register._sign_type;
    }

    [[nodiscard]] const std::optional<std::string_view> &virtual_name() const noexcept { return _virtual_name; }
    [[nodiscard]] bool is_accessed_frequently() const noexcept { return _is_accessed_frequently; }
    [[nodiscard]] std::optional<std::uint8_t> machine_register_id() const noexcept { return _machine_register_id; }
    [[nodiscard]] std::optional<RegisterWidth> width() const noexcept { return _width; }
    [[nodiscard]] std::optional<RegisterSignType> sign_type() const noexcept { return _sign_type; }

    [[nodiscard]] bool is_virtual() const noexcept { return _machine_register_id.has_value() == false; }

    [[nodiscard]] std::string to_string() const noexcept
    {
        if (_machine_register_id.has_value() && _width.has_value())
        {
            if (_sign_type.has_value() && _sign_type.value() == RegisterSignType::Unsigned)
            {
                return fmt::format("reg{}::{}u", std::uint16_t(_machine_register_id.value()),
                                   static_cast<std::uint16_t>(_width.value()));
            }
            return fmt::format("reg{}::{}", std::uint16_t(_machine_register_id.value()),
                               static_cast<std::uint16_t>(_width.value()));
        }

        if (_virtual_name.has_value())
        {
            return fmt::format("%{}", _virtual_name.value());
        }

        return "unknown reg";
    }

    bool operator==(const Register other) const noexcept
    {
        return _virtual_name == other._virtual_name && _machine_register_id == other._machine_register_id &&
               _width == other._width && _sign_type == other._sign_type;
    }

    bool operator!=(const Register other) const noexcept
    {
        return _virtual_name != other._virtual_name || _machine_register_id != other._machine_register_id ||
               _width != other._width || _sign_type != other._sign_type;
    }

private:
    /// Name of the virtual register.
    std::optional<std::string_view> _virtual_name{std::nullopt};

    /// Access annotation of the virtual register.
    bool _is_accessed_frequently{true};

    /// Id of the machine register.
    std::optional<std::uint8_t> _machine_register_id{std::nullopt};

    /// Width of the register.
    std::optional<RegisterWidth> _width{std::nullopt};

    /// Unsigned or Signed value?
    std::optional<RegisterSignType> _sign_type{std::nullopt};
};

class RegisterHash
{
public:
    std::size_t operator()(const Register &value) const
    {
        if (value.machine_register_id().has_value())
        {
            return std::hash<std::uint8_t>{}(value.machine_register_id().value());
        }

        return std::hash<std::string_view>{}(value.virtual_name().value());
    }
};
} // namespace flounder