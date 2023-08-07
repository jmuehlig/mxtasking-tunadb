#pragma once

#include "abstract_table.h"
#include "chain_entry_allocator.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <db/config.h>
#include <string>

namespace db::execution::compilation::hashtable {
class ChainedTable final : public AbstractTable
{
public:
    ChainedTable(Descriptor descriptor) noexcept : AbstractTable(descriptor), _capacity(descriptor.capacity()) {}

    ~ChainedTable() noexcept override;

    [[nodiscard]] static std::uint64_t header_width() noexcept
    {
        /// Size of the descriptor.
        return sizeof(ChainedTable);
    }

    [[nodiscard]] static std::uint64_t slot_width(const Descriptor &descriptor) noexcept
    {
        return Entry::width(descriptor.key_width(), descriptor.entry_width());
    }

    [[nodiscard]] static std::uint64_t size(const Descriptor &descriptor) noexcept
    {
        const auto is_used_bytes = descriptor.capacity();
        const auto entries_bytes = descriptor.capacity() * slot_width(descriptor);
        const auto overflow_bytes = descriptor.capacity() * slot_width(descriptor);

        /// Size of the header + size for all entries.
        return header_width() + is_used_bytes + entries_bytes + overflow_bytes;
    }

    [[nodiscard]] static std::uint64_t capacity_offset() noexcept { return offsetof(ChainedTable, _capacity); }

    [[nodiscard]] static std::uint64_t resized_table_offset() noexcept
    {
        return offsetof(ChainedTable, _resized_table);
    }

    [[nodiscard]] static std::uint64_t base_table_offset() noexcept { return offsetof(ChainedTable, _base_table); }

    [[nodiscard]] static std::uint64_t is_used_offset() noexcept { return header_width(); }

    [[nodiscard]] static std::uint64_t next_overflow_index_offset() noexcept
    {
        return offsetof(ChainedTable, _next_overflow_offset);
    }

    void initialize_empty() override
    {
        /// Set all is_used bytes to zero.
        std::memset(reinterpret_cast<void *>(this + 1U), '\0', _capacity);
    }

    /**
     * Creates a resized table with pointers set.
     */
    [[nodiscard]] ChainedTable *reallocate();

    [[nodiscard]] static std::uintptr_t create_resized_table(const std::uintptr_t hash_table)
    {
        return std::uintptr_t(reinterpret_cast<ChainedTable *>(hash_table)->reallocate());
    }

    [[nodiscard]] static Descriptor resize_descriptor(const Descriptor &old) noexcept
    {
        return Descriptor{old, old.capacity() * 2U};
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
    static void insert(flounder::Program &program, std::string &&hash_table_identifier,
                       const Descriptor &hash_table_descriptor, flounder::Register hash_table_vreg,
                       flounder::Register hash_vreg, insert_compare_key_callback_t &&compare_key_callback,
                       write_key_callback_t &&write_key_callback, write_entry_callback_t &&write_entry_callback)
    {
        insert_or_update(program, std::move(hash_table_identifier), hash_table_descriptor, hash_table_vreg, hash_vreg,
                         std::move(compare_key_callback), std::move(write_key_callback),
                         std::move(write_entry_callback), std::nullopt);
    }

    static void insert_or_update(flounder::Program &program, std::string &&hash_table_identifier,
                                 const Descriptor &hash_table_descriptor, flounder::Register hash_table_vreg,
                                 flounder::Register hash_vreg, insert_compare_key_callback_t &&compare_key_callback,
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

    static void replace_hash_table_address_with_resized_hash_table(flounder::Program &program,
                                                                   std::string &&hash_table_identifier,
                                                                   flounder::Register hash_table_vreg);

    static void resize_if_required(flounder::Program &program, const Descriptor &hash_table_descriptor,
                                   flounder::Register hash_table_vreg, create_hash_callback_t &&create_hash_callback);

    static void dump(std::uintptr_t hash_table_ptr);

private:
    /// Resized table when this is full.
    ChainedTable *_resized_table{nullptr};

    /// Pointer from a resized table to the base table.
    ChainedTable *_base_table{nullptr};

    /// Capacity for that hash table, may vary from the capacity in the descriptor when resizing.
    std::uint64_t _capacity;

    /// Size of the overflow buffer.
    std::uint32_t _next_overflow_offset{0U};

    class Entry
    {
    public:
        constexpr Entry() noexcept = default;
        ~Entry() noexcept = default;

        [[nodiscard]] static std::uint32_t width(const std::uint32_t key_width,
                                                 const std::uint32_t entry_width) noexcept
        {
            return sizeof(Entry) + key_width + entry_width;
        }

        [[nodiscard]] static constexpr std::uint32_t overflow_index_offset() noexcept
        {
            return offsetof(Entry, _overflow_index);
        }
        [[nodiscard]] static constexpr std::uint32_t key_offset() noexcept { return sizeof(Entry); }
        [[nodiscard]] static constexpr std::uint32_t entry_offset(const std::uint32_t key_size) noexcept
        {
            return key_offset() + key_size;
        }

        [[nodiscard]] std::uint32_t overflow_index() const noexcept { return _overflow_index; }

    private:
        std::uint32_t _overflow_index;
    };
};
} // namespace db::execution::compilation::hashtable