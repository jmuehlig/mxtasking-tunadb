#pragma once

#include "chained_table.h"
#include "linear_probing_table.h"
#include <cstdint>
#include <db/config.h>

namespace db::execution::compilation::hashtable {
class TableProxy
{
public:
    [[nodiscard]] static std::uint64_t size(const Descriptor &descriptor) noexcept
    {
        if (descriptor.table_type() == Descriptor::LinearProbing)
        {
            return LinearProbingTable::size(descriptor);
        }

        if (descriptor.table_type() == Descriptor::Chained)
        {
            return ChainedTable::size(descriptor);
        }

        return 0U;
    }

    [[nodiscard]] static std::uint64_t allocation_capacity(const std::uint64_t expected_capacity,
                                                           const Descriptor::Type type)
    {
        if (type == Descriptor::Type::LinearProbing)
        {
            return std::max(64UL, mx::memory::alignment_helper::next_power_of_two(expected_capacity * 1.5));
        }

        return std::max<std::uint64_t>(
            mx::memory::alignment_helper::next_power_of_two(config::tuples_per_tile() * 2U - 1U),
            mx::memory::alignment_helper::next_power_of_two(expected_capacity));
    }

    [[nodiscard]] static std::uint64_t allocation_capacity(const std::uint64_t expected_capacity,
                                                           const Descriptor &descriptor)
    {
        return allocation_capacity(expected_capacity, descriptor.table_type());
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
                       AbstractTable::insert_compare_key_callback_t &&compare_key_callback,
                       AbstractTable::write_key_callback_t &&write_key_callback,
                       AbstractTable::write_entry_callback_t &&write_entry_callback)
    {
        if (hash_table_descriptor.table_type() == Descriptor::LinearProbing)
        {
            LinearProbingTable::insert(program, hash_table_descriptor, hash_table_vreg, hash_vreg,
                                       std::move(compare_key_callback), std::move(write_key_callback),
                                       std::move(write_entry_callback));
        }

        else if (hash_table_descriptor.table_type() == Descriptor::Chained)
        {
            ChainedTable::insert(program, "", hash_table_descriptor, hash_table_vreg, hash_vreg,
                                 std::move(compare_key_callback), std::move(write_key_callback),
                                 std::move(write_entry_callback));
        }
    }

    static void insert_or_update(flounder::Program &program, const Descriptor &hash_table_descriptor,
                                 flounder::Register hash_table_vreg, flounder::Register hash_vreg,
                                 AbstractTable::insert_compare_key_callback_t &&compare_key_callback,
                                 AbstractTable::write_key_callback_t &&write_key_callback,
                                 AbstractTable::write_entry_callback_t &&write_entry_callback,
                                 std::optional<AbstractTable::write_entry_callback_t> &&update_entry_callback)
    {
        if (hash_table_descriptor.table_type() == Descriptor::LinearProbing)
        {
            LinearProbingTable::insert_or_update(program, hash_table_descriptor, hash_table_vreg, hash_vreg,
                                                 std::move(compare_key_callback), std::move(write_key_callback),
                                                 std::move(write_entry_callback), std::move(update_entry_callback));
        }

        else if (hash_table_descriptor.table_type() == Descriptor::Chained)
        {
            ChainedTable::insert_or_update(program, "", hash_table_descriptor, hash_table_vreg, hash_vreg,
                                           std::move(compare_key_callback), std::move(write_key_callback),
                                           std::move(write_entry_callback), std::move(update_entry_callback));
        }
    }

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
                     flounder::Register hash_vreg, AbstractTable::find_compare_key_callback_t &&compare_key_callback,
                     AbstractTable::find_entry_callback_t &&find_callback)
    {
        if (hash_table_descriptor.table_type() == Descriptor::LinearProbing)
        {
            LinearProbingTable::find(program, std::move(hash_table_identifier), hash_table_descriptor, hash_table_vreg,
                                     hash_vreg, std::move(compare_key_callback), std::move(find_callback));
        }

        else if (hash_table_descriptor.table_type() == Descriptor::Chained)
        {
            ChainedTable::find(program, std::move(hash_table_identifier), hash_table_descriptor, hash_table_vreg,
                               hash_vreg, std::move(compare_key_callback), std::move(find_callback));
        }
    }

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
                         AbstractTable::iterate_callback_t &&iterate_callback)
    {
        if (hash_table_descriptor.table_type() == Descriptor::LinearProbing)
        {
            LinearProbingTable::for_each(program, std::move(hash_table_identifier), hash_table_descriptor,
                                         hash_table_vreg, std::move(iterate_callback));
        }

        else if (hash_table_descriptor.table_type() == Descriptor::Chained)
        {
            ChainedTable::for_each(program, std::move(hash_table_identifier), hash_table_descriptor, hash_table_vreg,
                                   std::move(iterate_callback));
        }
    }

    static void replace_hash_table_address_with_resized_hash_table(flounder::Program &program,
                                                                   std::string &&hash_table_identifier,
                                                                   const Descriptor &hash_table_descriptor,
                                                                   flounder::Register hash_table_vreg)
    {
        if (hash_table_descriptor.table_type() == Descriptor::Chained)
        {
            ChainedTable::replace_hash_table_address_with_resized_hash_table(program, std::move(hash_table_identifier),
                                                                             hash_table_vreg);
        }
    }

    static void resize_if_required(flounder::Program &program, const Descriptor &hash_table_descriptor,
                                   flounder::Register hash_table_vreg,
                                   AbstractTable::create_hash_callback_t &&create_hash_callback)
    {
        if (hash_table_descriptor.table_type() == Descriptor::Chained)
        {
            ChainedTable::resize_if_required(program, hash_table_descriptor, hash_table_vreg,
                                             std::move(create_hash_callback));
        }
    }
};
} // namespace db::execution::compilation::hashtable