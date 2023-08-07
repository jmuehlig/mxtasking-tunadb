#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mx/memory/alignment_helper.h>

namespace application::rjbenchmark {
class HashTable
{
public:
    [[nodiscard]] static std::uint64_t size_in_bytes(std::uint64_t items)
    {
        items = mx::memory::alignment_helper::next_power_of_two(items);

        return sizeof(HashTable) + (items * sizeof(std::int64_t)) + (items * sizeof(std::uint64_t));
    }

    HashTable(const std::uint8_t radix_bits, const std::uint64_t slots) noexcept
        : _radix_bits(radix_bits), _mask((slots - 1U) << radix_bits), _slots(slots)
    {
        std::memset(this + 1U, 0, sizeof(std::int64_t) * slots);
    }

    ~HashTable() = default;

    void insert(const std::int64_t key)
    {
        const auto index = (key & _mask) >> _radix_bits;

        auto *next = reinterpret_cast < std::int
    }

private:
    const std::uint8_t _radix_bits;
    const std::uint32_t _mask;
    const std::uint64_t _slots;
};
} // namespace application::rjbenchmark