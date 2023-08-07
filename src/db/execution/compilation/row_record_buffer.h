#pragma once

#include <cstdint>
#include <db/topology/physical_schema.h>

namespace db::execution::compilation {
class RowRecordBuffer
{
public:
    ~RowRecordBuffer() noexcept = default;

    [[nodiscard]] static RowRecordBuffer *make(const topology::PhysicalSchema &schema, const std::uint64_t capacity)
    {
        return new (std::aligned_alloc(64U, sizeof(RowRecordBuffer) + schema.row_size() * capacity))
            RowRecordBuffer(capacity, schema.row_size());
    }

    [[nodiscard]] std::uint64_t capacity() const noexcept { return _capacity; }

    [[nodiscard]] void *begin() noexcept { return static_cast<void *>(this + 1U); }
    [[nodiscard]] void *end() noexcept
    {
        return reinterpret_cast<void *>(std::uintptr_t(begin()) + (_size * _record_size));
    }

    [[nodiscard]] static std::uint32_t size_offset() noexcept { return offsetof(RowRecordBuffer, _size); }

private:
    constexpr RowRecordBuffer(const std::uint64_t capacity, const std::uint32_t record_size)
        : _capacity(capacity), _record_size(record_size)
    {
    }

    std::uint64_t _size{0U};

    const std::uint64_t _capacity;

    const std::uint32_t _record_size;
};
} // namespace db::execution::compilation