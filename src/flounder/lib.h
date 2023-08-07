#pragma once

#include "program.h"
#include <cstdint>
#include <fmt/core.h>
#include <optional>

namespace flounder {
class Lib
{
public:
    /**
     * Emits code to copy the memory pointed by source to destination.
     *
     * @param program Program to allocate nodes.
     * @param destination VREG holding the destination address.
     * @param source VREG holding the source address.
     * @param size Size of the content to copy.
     */
    static void memcpy(Program &program, Register destination, Register source, std::size_t size)
    {
        memcpy(program, destination, 0U, source, size);
    }

    static void memcpy(Program &program, Register destination, std::uint32_t destination_offset, Register source,
                       std::size_t size)
    {
        memcpy(program, destination, destination_offset, source, 0U, size);
    }

    /**
     * Emits code to copy the memory pointed by source to destination.
     *
     * @param program Program to allocate nodes.
     * @param destination VREG holding the destination address.
     * @param destination_offset Offset to the destination address.
     * @param source VREG holding the source address.
     * @param source_offset Offset to the source address.
     * @param size Size of the content to copy.
     */
    static void memcpy(Program &program, Register destination, std::uint32_t destination_offset, Register source,
                       std::uint32_t source_offset, std::size_t size);

private:
    template <std::uint8_t BYTE>
    static void bytewise_memcpy(Program &program, Register destination, const std::uint32_t destination_offset,
                                Register source, const std::uint32_t source_offset, std::uint32_t &remaining,
                                std::uint32_t &offset)
    {
        if (remaining >= BYTE)
        {
            auto copy_vreg = program.vreg(fmt::format("memcpy{}", BYTE));
            program << program.request_vreg(copy_vreg, static_cast<RegisterWidth>(BYTE * 8U));

            while (remaining >= BYTE)
            {
                auto source_address = program.mem(source, offset + source_offset);
                auto destination_address = program.mem(destination, offset + destination_offset);
                program << program.mov(copy_vreg, source_address) << program.mov(destination_address, copy_vreg);

                offset += BYTE;
                remaining -= BYTE;
            }

            program << program.clear(copy_vreg);
        }
    }
};
} // namespace flounder