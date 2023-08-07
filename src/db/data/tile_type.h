#pragma once

#include <cstdint>
namespace db::data {
enum AllocationType : std::uint8_t
{
    Resource = 0b0,
    TemporaryResource = 0b1,
    TemporaryForClient = 0b11,
};
}