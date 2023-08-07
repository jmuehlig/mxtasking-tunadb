#pragma once

#include "program.h"
#include <cstdint>
#include <optional>
#include <string>

namespace flounder {
class String
{
public:
    class Descriptor
    {
    public:
        Descriptor(Operand data, const std::size_t size, const bool is_fixed_size, const bool is_pointer) noexcept
            : _data(data), _size(size), _is_fixed_size(is_fixed_size), _is_pointer(is_pointer)
        {
        }

        Descriptor(Register data, const std::size_t size, const bool is_fixed_size, const bool is_pointer) noexcept
            : Descriptor(Operand{data}, size, is_fixed_size, is_pointer)
        {
        }

        Descriptor(Operand data, const std::int32_t offset, const std::size_t size, const bool is_fixed_size,
                   const bool is_pointer) noexcept
            : _data(data), _offset(offset), _size(size), _is_fixed_size(is_fixed_size), _is_pointer(is_pointer)
        {
        }

        Descriptor(Register data, const std::int32_t offset, const std::size_t size, const bool is_fixed_size,
                   const bool is_pointer) noexcept
            : Descriptor(Operand{data}, offset, size, is_fixed_size, is_pointer)
        {
        }

        ~Descriptor() noexcept = default;

        [[nodiscard]] Operand data() const noexcept { return _data; }
        [[nodiscard]] std::optional<std::int32_t> offset() const noexcept { return _offset; }
        [[nodiscard]] std::size_t size() const noexcept { return _size; }
        [[nodiscard]] bool is_fixed_size() const noexcept { return _is_fixed_size; }
        [[nodiscard]] bool is_pointer() const noexcept { return _is_pointer; }

    private:
        Operand _data;
        const std::optional<std::int32_t> _offset{std::nullopt};
        const std::size_t _size;
        const bool _is_fixed_size;
        const bool _is_pointer;
    };

    [[nodiscard]] static Register is_equals(Program &program, std::string &&name, Descriptor left, Descriptor right);

private:
    [[nodiscard]] static Register is_equals_inlined_and_pointer(Program &program, std::string &&name, Descriptor left,
                                                                Descriptor right);

    [[nodiscard]] static Register is_equals_fixed_and_variable_size(Program &program, std::string &&name,
                                                                    Descriptor fixed_size_descriptor,
                                                                    Descriptor variable_size_descriptor);

    [[nodiscard]] static MemoryAddress access(Program &program, const Operand operand, const std::int32_t offset,
                                              const RegisterWidth width)
    {
        if (operand.is_reg())
        {
            return program.mem(operand.reg(), offset, width);
        }

        return program.mem(program.constant64(operand.constant().value_as_int64() + offset), width);
    }
};
} // namespace flounder