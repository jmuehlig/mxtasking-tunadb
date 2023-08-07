#include "lib.h"

using namespace flounder;

void Lib::memcpy(Program &program, Register destination, const std::uint32_t destination_offset, Register source,
                 const std::uint32_t source_offset, const std::size_t size)
{
    auto remaining = std::uint32_t(size);
    auto offset = std::uint32_t{0U};

    Lib::bytewise_memcpy<8U>(program, destination, destination_offset, source, source_offset, remaining, offset);
    Lib::bytewise_memcpy<4U>(program, destination, destination_offset, source, source_offset, remaining, offset);
    Lib::bytewise_memcpy<2U>(program, destination, destination_offset, source, source_offset, remaining, offset);
    Lib::bytewise_memcpy<1U>(program, destination, destination_offset, source, source_offset, remaining, offset);
}