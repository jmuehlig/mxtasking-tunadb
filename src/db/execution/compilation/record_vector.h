#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>

namespace db::execution::compilation {
class RecordVector
{
public:
    RecordVector(const std::uint16_t record_size, const std::size_t capacity)
        : _record_size(record_size), _capacity(capacity), _data(allocate(capacity))
    {
    }

    RecordVector(RecordVector &&other) noexcept
        : _record_size(other._record_size), _size(other._size), _capacity(other._capacity),
          _data(std::exchange(other._data, nullptr))
    {
    }

    ~RecordVector()
    {
        if (_data != nullptr)
        {
            std::free(_data);
        }
    }

    [[nodiscard]] std::uintptr_t allocate() { return at(allocate_index()); }

    [[nodiscard]] std::size_t allocate_index()
    {
        if (_size == _capacity)
        {
            resize(_capacity << 1U);
        }

        return _size++;
    }

    [[nodiscard]] std::uintptr_t at(const std::size_t index) const noexcept
    {
        return std::uintptr_t(_data) + index * _record_size;
    }

    [[nodiscard]] static std::uint16_t size_offset() noexcept { return offsetof(RecordVector, _size); }
    [[nodiscard]] static std::uint16_t data_offset() noexcept { return offsetof(RecordVector, _data); }
    [[nodiscard]] std::uint16_t size() const noexcept { return _size; }
    [[nodiscard]] bool full() const noexcept { return _size == _capacity; }

private:
    const std::uint16_t _record_size;
    std::size_t _size{0U};
    std::size_t _capacity;
    std::byte *_data{nullptr};

    [[nodiscard]] std::byte *allocate(const std::size_t capacity) const
    {
        auto *address = std::aligned_alloc(64U, capacity * _record_size);
        std::memset(address, 0, capacity * _record_size);
        return static_cast<std::byte *>(address);
    }

    void resize(const std::size_t capacity)
    {
        auto *old_data = std::exchange(_data, allocate(capacity));
        std::memmove(static_cast<void *>(_data), static_cast<void *>(old_data), _size * _record_size);
        _capacity = capacity;
        std::free(old_data);
    }
};
} // namespace db::execution::compilation