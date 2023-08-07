#pragma once

#include <cstdint>
#include <cstdlib>
#include <utility>
#include <vector>

namespace db::execution::compilation::hashtable {
class ChainEntryAllocator
{
public:
    ChainEntryAllocator() { _allocated_chunks.emplace_back(Chunk{}); }
    ChainEntryAllocator(ChainEntryAllocator &&) noexcept = default;
    ~ChainEntryAllocator() = default;

    ChainEntryAllocator &operator=(ChainEntryAllocator &&) noexcept = default;

    [[nodiscard]] void *allocate(const std::size_t size)
    {
        if (_allocated_chunks.back().can_allocate(size) == false)
        {
            _allocated_chunks.template emplace_back(Chunk{});
        }

        return _allocated_chunks.back().allocate(size);
    }

private:
    class Chunk
    {
    public:
        Chunk() : _memory(std::uintptr_t(std::aligned_alloc(64U, _capacity_in_bytes))) {}

        Chunk(Chunk &&other) noexcept
            : _size_in_bytes(std::exchange(other._size_in_bytes, 0U)), _memory(std::exchange(other._memory, 0U))
        {
        }

        Chunk(const Chunk &) = delete;

        ~Chunk()
        {
            if (_memory != 0U)
            {
                std::free(reinterpret_cast<void *>(_memory));
            }
        }

        [[nodiscard]] void *allocate(const std::size_t size)
        {
            const auto entry_address = _memory + std::exchange(_size_in_bytes, _size_in_bytes + size);

            /// Return the address of the entry.
            return reinterpret_cast<void *>(entry_address);
        }

        [[nodiscard]] bool can_allocate(const std::size_t size) const noexcept
        {
            /// Need space for the entry.
            return (_size_in_bytes + size) <= _capacity_in_bytes;
        }

    private:
        static inline constexpr std::size_t _capacity_in_bytes{1U << 20U};
        std::size_t _size_in_bytes{0U};
        std::uintptr_t _memory{0U};
    };

    std::vector<Chunk> _allocated_chunks;
};
} // namespace db::execution::compilation::hashtable