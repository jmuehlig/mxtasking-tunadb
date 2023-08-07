#pragma once

#include <cassert>
#include <cstdint>

namespace db::execution::compilation::hashtable {
class Descriptor
{
public:
    enum Type : std::uint8_t
    {
        LinearProbing,
        Chained
    };

    constexpr Descriptor(const Type type, const std::uint64_t capacity, const std::uint32_t key_width,
                         const std::uint32_t entry_width, const bool is_multiple_entries_per_key,
                         const std::uint32_t bucket_capacity) noexcept
        : _table_type(type), _capacity(capacity), _key_width(key_width), _entry_width(entry_width),
          _is_multiple_entries_per_key(is_multiple_entries_per_key), _bucket_capacity(bucket_capacity)
    {
        assert((capacity & (capacity - 1)) == 0 && "Hash table capacity is not aligned.");
    }

    constexpr Descriptor(const Type type, const std::uint64_t capacity, const std::uint32_t key_width,
                         const std::uint32_t entry_width) noexcept
        : Descriptor(type, capacity, key_width, entry_width, false, 1U)
    {
    }

    Descriptor(Descriptor &&) noexcept = default;
    Descriptor(const Descriptor &) noexcept = default;

    Descriptor(const Descriptor &other, const std::uint64_t capacity) noexcept
        : Descriptor(other._table_type, capacity, other._key_width, other._entry_width,
                     other._is_multiple_entries_per_key, other._bucket_capacity)
    {
    }

    ~Descriptor() noexcept = default;

    [[nodiscard]] Type table_type() const noexcept { return _table_type; }
    [[nodiscard]] std::uint64_t capacity() const noexcept { return _capacity; }
    [[nodiscard]] std::uint32_t key_width() const noexcept { return _key_width; }
    [[nodiscard]] std::uint32_t entry_width() const noexcept { return _entry_width; }
    [[nodiscard]] std::uint8_t bucket_capacity() const noexcept { return _bucket_capacity; }
    [[nodiscard]] bool is_multiple_entries_per_key() const noexcept { return _is_multiple_entries_per_key; }

private:
    /// Type of the table (linear probing, chaining, ...).
    Type _table_type;

    /// Capacity of the hash table.
    std::uint64_t _capacity;

    /// Size in bytes for every key.
    std::uint32_t _key_width;

    /// Size in bytes for every entry.
    std::uint32_t _entry_width;

    /// Indicator if we store more than one entry per key.
    bool _is_multiple_entries_per_key;

    /// Capacity of inlined entries (only if multiple entries per key).
    std::uint8_t _bucket_capacity{1U};
};
} // namespace db::execution::compilation::hashtable