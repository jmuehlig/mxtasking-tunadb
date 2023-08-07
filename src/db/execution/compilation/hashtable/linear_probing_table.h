#pragma once

#include "abstract_table.h"
#include "chain_entry_allocator.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <db/config.h>
#include <string>

namespace db::execution::compilation::hashtable {
class LinearProbingTable final : public AbstractTable
{
public:
    LinearProbingTable(Descriptor descriptor) noexcept : AbstractTable(descriptor) {}

    ~LinearProbingTable() noexcept override = default;

    [[nodiscard]] static std::uint64_t is_used_indicator_width(const Descriptor &descriptor) noexcept
    {
        return descriptor.capacity() * sizeof(char);
    }

    [[nodiscard]] static std::uint64_t header_width(const Descriptor &descriptor) noexcept
    {
        /// Size of the descriptor + size of "is_used" indicators.
        return sizeof(LinearProbingTable) + is_used_indicator_width(descriptor);
    }

    [[nodiscard]] static std::uint64_t slot_width(const Descriptor &descriptor) noexcept
    {
        return EntryHeader::width(descriptor.key_width()) + Entry::width(descriptor.entry_width(),
                                                                         descriptor.bucket_capacity(),
                                                                         descriptor.is_multiple_entries_per_key());
    }

    [[nodiscard]] static std::uint64_t size(const Descriptor &descriptor) noexcept
    {
        /// Size of the header + size for all entries.
        return header_width(descriptor) + descriptor.capacity() * slot_width(descriptor);
    }

    [[nodiscard]] static constexpr std::uint32_t is_used_indicator_offset() noexcept
    {
        return sizeof(LinearProbingTable);
    }

    [[nodiscard]] static std::uint64_t begin_offset(const Descriptor &descriptor) noexcept
    {
        return header_width(descriptor);
    }

    void initialize_empty() override
    {
        const auto used_indicator_size = is_used_indicator_width(descriptor());
        std::memset(reinterpret_cast<void *>(std::uintptr_t(this) + is_used_indicator_offset()), '\0',
                    used_indicator_size);
    }

    /**
     * Inserts an entry in the hash table.
     *
     * @param program
     * @param hash_table_descriptor
     * @param hash_table_vreg
     * @param hash_vreg
     * @param compare_key_callback
     * @param write_key_callback
     * @param write_entry_callback
     */
    static void insert(flounder::Program &program, const Descriptor &hash_table_descriptor,
                       flounder::Register hash_table_vreg, flounder::Register hash_vreg,
                       insert_compare_key_callback_t &&compare_key_callback, write_key_callback_t &&write_key_callback,
                       write_entry_callback_t &&write_entry_callback)
    {
        insert_or_update(program, hash_table_descriptor, hash_table_vreg, hash_vreg, std::move(compare_key_callback),
                         std::move(write_key_callback), std::move(write_entry_callback), std::nullopt);
    }

    static void insert_or_update(flounder::Program &program, const Descriptor &hash_table_descriptor,
                                 flounder::Register hash_table_vreg, flounder::Register hash_vreg,
                                 insert_compare_key_callback_t &&compare_key_callback,
                                 write_key_callback_t &&write_key_callback,
                                 write_entry_callback_t &&write_entry_callback,
                                 std::optional<write_entry_callback_t> &&update_entry_callback);

    /**
     * Finds an entry in the hash table.
     *
     * @param program
     * @param hash_table_identifier
     * @param hash_table_descriptor
     * @param hash_table_vreg
     * @param hash_vreg
     * @param compare_key_callback
     * @return
     */
    static void find(flounder::Program &program, std::string &&hash_table_identifier,
                     const Descriptor &hash_table_descriptor, flounder::Register hash_table_vreg,
                     flounder::Register hash_vreg, find_compare_key_callback_t &&compare_key_callback,
                     find_entry_callback_t &&find_callback);

    /**
     * Iterates over all entries in the hash table.
     *
     * @param program
     * @param hash_table_identifier
     * @param hash_table_descriptor
     * @param hash_table_vreg
     * @param iterate_callback
     */
    static void for_each(flounder::Program &program, std::string &&hash_table_identifier,
                         const Descriptor &hash_table_descriptor, flounder::Register hash_table_vreg,
                         iterate_callback_t &&iterate_callback);

    static __attribute__((noinline)) std::uintptr_t allocate_spill_entry(const std::uintptr_t hash_table_ptr)
    {
        auto *hash_table = reinterpret_cast<LinearProbingTable *>(hash_table_ptr);
        auto *entry = new (hash_table->_spill_entry_allocator.allocate(
            Entry::width(hash_table->descriptor().entry_width(), hash_table->descriptor().bucket_capacity(),
                         hash_table->descriptor().is_multiple_entries_per_key()))) Entry{};
        return std::uintptr_t(entry);
    }

private:
    ChainEntryAllocator _spill_entry_allocator;

    class EntryHeader
    {
    public:
        [[nodiscard]] static std::uint32_t width(const std::uint32_t key_width) noexcept
        {
            return sizeof(EntryHeader) + key_width;
        }

        [[nodiscard]] static constexpr std::uint32_t hash_offset() noexcept { return offsetof(EntryHeader, _hash); }
        [[nodiscard]] static constexpr std::uint32_t begin_offset() noexcept { return sizeof(EntryHeader); }

    private:
        std::int64_t _hash;
    };

    class Entry
    {
    public:
        constexpr Entry() noexcept = default;
        ~Entry() noexcept = default;

        [[nodiscard]] static std::uint32_t width(const std::uint32_t entry_width, const std::uint8_t entry_capacity,
                                                 const bool is_multiple_entries) noexcept
        {
            if (is_multiple_entries == false && entry_capacity == 1U)
            {
                return entry_width;
            }

            return sizeof(Entry) + entry_capacity * entry_width;
        }
        [[nodiscard]] static constexpr std::uint32_t size_offset() noexcept { return offsetof(Entry, _size); }
        [[nodiscard]] static constexpr std::uint32_t next_offset() noexcept { return offsetof(Entry, _next); }
        [[nodiscard]] static constexpr std::uint32_t begin_offset(const bool is_multiple_entries) noexcept
        {
            return is_multiple_entries ? sizeof(Entry) : 0U;
        }

    private:
        std::uint8_t _size{0U};
        Entry *_next{nullptr};
    };

    static void insert_only_single_entry(flounder::Program &program, const Descriptor &hash_table_descriptor,
                                         flounder::Register hash_table_vreg, flounder::Register hash_vreg,
                                         insert_compare_key_callback_t &&compare_key_callback,
                                         write_key_callback_t &&write_key_callback,
                                         write_entry_callback_t &&write_entry_callback);
};
} // namespace db::execution::compilation::hashtable