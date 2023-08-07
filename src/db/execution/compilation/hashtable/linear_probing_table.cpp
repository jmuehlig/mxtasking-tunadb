#include "linear_probing_table.h"
#include <db/exception/execution_exception.h>
#include <fmt/core.h>

using namespace db::execution::compilation::hashtable;

void LinearProbingTable::insert_or_update(
    flounder::Program &program, const db::execution::compilation::hashtable::Descriptor &hash_table_descriptor,
    flounder::Register hash_table_vreg, flounder::Register hash_vreg,
    db::execution::compilation::hashtable::AbstractTable::insert_compare_key_callback_t &&compare_key_callback,
    db::execution::compilation::hashtable::AbstractTable::write_key_callback_t &&write_key_callback,
    db::execution::compilation::hashtable::AbstractTable::write_entry_callback_t &&write_entry_callback,
    std::optional<write_entry_callback_t> &&update_entry_callback)
{
    //    if (update_entry_callback.has_value() == false && hash_table_descriptor.is_multiple_entries_per_key() ==
    //    false)
    //    {
    //        HashTable::insert_only_single_entry(program, hash_table_descriptor, hash_table_vreg, hash_vreg,
    //                                            std::move(compare_key_callback), std::move(write_key_callback),
    //                                            std::move(write_entry_callback));
    //        return;
    //    }

    auto compare_key_label = program.label("ht_compare_key");
    auto insert_entry_label = program.label("ht_insert_entry");
    auto append_entry_label = program.label("ht_append_entry");
    auto update_entry_label = program.label("ht_update_entry");

    /// This is the index in the hash table where the hash should be located.
    auto hashed_index_vreg = program.vreg("hashed_index");
    program << program.request_vreg64(hashed_index_vreg) << program.mov(hashed_index_vreg, hash_vreg)
            << program.and_(hashed_index_vreg, program.constant64(hash_table_descriptor.capacity() - 1));

    {
        /// Since the entry may be in use, we scan over all entries and find an empty slot.
        auto for_loop = flounder::ForRange{program, 0U, hash_table_descriptor.capacity(), "ht_insert_entry"};
        auto index_vreg = program.vreg("ht_index");
        program << program.request_vreg64(index_vreg)
                << program.lea(index_vreg, program.mem(hashed_index_vreg, for_loop.counter_vreg()))
                << program.and_(index_vreg, program.constant64(hash_table_descriptor.capacity() - 1));

        /// Load the address where the is_used flag for this index is located.
        auto is_used_address_vreg = program.vreg("ht_is_used_address");
        program << program.request_vreg64(is_used_address_vreg)
                << program.lea(is_used_address_vreg, program.mem(hash_table_vreg, index_vreg,
                                                                 LinearProbingTable::is_used_indicator_offset()));

        /// Load the address of the slot.
        auto slot_address_vreg = program.vreg("ht_slot_address");
        program << program.request_vreg64(slot_address_vreg) << program.mov(slot_address_vreg, index_vreg)
                << program.imul(slot_address_vreg,
                                program.constant32(LinearProbingTable::slot_width(hash_table_descriptor)))
                << program.lea(slot_address_vreg, program.mem(slot_address_vreg, hash_table_vreg,
                                                              LinearProbingTable::begin_offset(hash_table_descriptor)))
                << program.clear(index_vreg);

        program << program.test(program.mem(is_used_address_vreg, flounder::RegisterWidth::r8), program.constant8(1))
                << program.jz(insert_entry_label);

        /// Compare keys
        program << program.section(compare_key_label);
        /// First, compare hashes
        program << program.cmp(hash_vreg,
                               program.mem(slot_address_vreg, EntryHeader::hash_offset(), flounder::RegisterWidth::r64))
                << program.jne(for_loop.step_label());

        /// If we found a matching entry, update the value; probe otherwise.
        if (update_entry_callback.has_value())
        {
            compare_key_callback(program, slot_address_vreg, LinearProbingTable::EntryHeader::begin_offset(),
                                 update_entry_label, for_loop.foot_label());
        }

        /// If we found a matching entry and store multiple entries per key, append; probe otherwise.
        else if (hash_table_descriptor.is_multiple_entries_per_key())
        {
            compare_key_callback(program, slot_address_vreg, LinearProbingTable::EntryHeader::begin_offset(),
                                 append_entry_label, for_loop.foot_label());
        }

        /// If we found a matching entry and have only one entry, jump out of the loop.
        else
        {
            compare_key_callback(program, slot_address_vreg, LinearProbingTable::EntryHeader::begin_offset(),
                                 for_loop.foot_label(), for_loop.foot_label());
        }

        /// Insert.
        {
            program << program.section(insert_entry_label);

            /// Is used.
            program << program.mov(program.mem(is_used_address_vreg, flounder::RegisterWidth::r8), program.constant8(1))
                    << program.clear(is_used_address_vreg);

            /// Hash.
            program << program.mov(
                program.mem(slot_address_vreg, EntryHeader::hash_offset(), flounder::RegisterWidth::r64), hash_vreg);

            /// Key.
            const auto key_offset = EntryHeader::begin_offset();
            write_key_callback(program, slot_address_vreg, key_offset);

            auto entry_offset = EntryHeader::begin_offset() + hash_table_descriptor.key_width();

            /// Set number of entries and next-pointer if necessary.
            if (hash_table_descriptor.is_multiple_entries_per_key())
            {
                /// Set the size if needed.
                if (hash_table_descriptor.bucket_capacity() > 1U)
                {
                    program << program.mov(program.mem(slot_address_vreg, entry_offset + Entry::size_offset(),
                                                       flounder::RegisterWidth::r8),
                                           program.constant8(1));
                }

                /// Set the next pointer if needed.
                if (hash_table_descriptor.is_multiple_entries_per_key())
                {
                    program << program.mov(program.mem(slot_address_vreg, entry_offset + Entry::next_offset(),
                                                       flounder::RegisterWidth::r64),
                                           program.constant8(0));
                }

                entry_offset += Entry::begin_offset(hash_table_descriptor.is_multiple_entries_per_key());
            }

            write_entry_callback(program, slot_address_vreg, entry_offset);

            program << program.jmp(for_loop.foot_label());
        }

        /// Append.
        if (hash_table_descriptor.is_multiple_entries_per_key())
        {
            program << program.section(append_entry_label)
                    << program.add(slot_address_vreg,
                                   program.constant32(EntryHeader::width(hash_table_descriptor.key_width())));

            /// while slot_address.next != nullptr: slot_address = slot_address.next;
            auto entry_address_next_vreg = program.vreg("ht_entry_address_next");
            program << program.request_vreg64(entry_address_next_vreg)
                    << program.mov(entry_address_next_vreg, program.mem(slot_address_vreg, Entry::next_offset()));
            {
                auto while_entry_has_next =
                    flounder::While{program,
                                    flounder::IsNotEquals{flounder::Operand{entry_address_next_vreg},
                                                          flounder::Operand{program.constant32(0U)}},
                                    "ht_entry_has_next"};
                program << program.mov(slot_address_vreg, entry_address_next_vreg)
                        << program.mov(entry_address_next_vreg, program.mem(slot_address_vreg, Entry::next_offset()));
                ;
            }
            program << program.clear(entry_address_next_vreg);

            /// Check if the entry at slot_address has space < entry_capacity. If not, allocate a new spill entry.
            auto entry_offset = 0U;
            if (hash_table_descriptor.bucket_capacity() > 1U)
            {
                {
                    auto if_has_not_enough_space = flounder::If{
                        program, flounder::IsGreaterEquals{
                                     flounder::Operand{program.mem(slot_address_vreg, Entry::size_offset(),
                                                                   flounder::RegisterWidth::r8)},
                                     flounder::Operand{program.constant8(hash_table_descriptor.bucket_capacity())}}};
                    auto new_slot_address_vreg =
                        flounder::FunctionCall{program, std::uintptr_t(&LinearProbingTable::allocate_spill_entry),
                                               "ht_new_spill_entry"}
                            .call({flounder::Operand{hash_table_vreg}});

                    program << program.mov(program.mem(slot_address_vreg, Entry::next_offset()),
                                           new_slot_address_vreg.value())
                            << program.mov(slot_address_vreg, new_slot_address_vreg.value())
                            << program.clear(new_slot_address_vreg.value());
                }

                auto slot_offset_vreg = program.vreg("ht_slot_offset");
                program << program.request_vreg64(slot_offset_vreg)
                        << program.mov(slot_offset_vreg, program.mem(slot_address_vreg, Entry::size_offset(),
                                                                     flounder::RegisterWidth::r8))
                        << program.imul(slot_offset_vreg, program.constant32(hash_table_descriptor.entry_width()))
                        << program.add(slot_offset_vreg, program.constant32(Entry::begin_offset(true)))
                        << program.add(
                               program.mem(slot_address_vreg, Entry::size_offset(), flounder::RegisterWidth::r8),
                               program.constant8(1))
                        << program.add(slot_address_vreg, slot_offset_vreg) << program.clear(slot_offset_vreg);
            }
            else
            {
                auto new_slot_address_vreg =
                    flounder::FunctionCall{program, std::uintptr_t(&LinearProbingTable::allocate_spill_entry),
                                           "ht_new_spill_entry"}
                        .call({flounder::Operand{hash_table_vreg}});

                program << program.mov(program.mem(slot_address_vreg, Entry::next_offset()),
                                       new_slot_address_vreg.value())
                        << program.mov(slot_address_vreg, new_slot_address_vreg.value())
                        << program.clear(new_slot_address_vreg.value());

                program << program.add(
                    program.mem(slot_address_vreg, Entry::size_offset(), flounder::RegisterWidth::r8),
                    program.constant8(1));

                entry_offset = Entry::begin_offset(true);
            }

            write_entry_callback(program, slot_address_vreg, entry_offset);

            program << program.jmp(for_loop.foot_label());
        }

        /// Update.
        if (update_entry_callback.has_value())
        {
            program << program.section(update_entry_label);
            const auto entry_offset = EntryHeader::width(hash_table_descriptor.key_width()) +
                                      Entry::begin_offset(hash_table_descriptor.is_multiple_entries_per_key());
            update_entry_callback.value()(program, slot_address_vreg, entry_offset);
            program << program.jmp(for_loop.foot_label());
        }

        program << program.clear(slot_address_vreg);
    }

    program << program.clear(hashed_index_vreg);
}

void LinearProbingTable::insert_only_single_entry(
    flounder::Program &program, const db::execution::compilation::hashtable::Descriptor &hash_table_descriptor,
    flounder::Register hash_table_vreg, flounder::Register hash_vreg,
    db::execution::compilation::hashtable::AbstractTable::insert_compare_key_callback_t &&compare_key_callback,
    db::execution::compilation::hashtable::AbstractTable::write_key_callback_t &&write_key_callback,
    db::execution::compilation::hashtable::AbstractTable::write_entry_callback_t &&write_entry_callback)
{
    auto compare_key_label = program.label("ht_compare_key");

    /// This is the index in the hash table where the hash should be located.
    auto hashed_index_vreg = program.vreg("hashed_index");
    program << program.request_vreg64(hashed_index_vreg) << program.mov(hashed_index_vreg, hash_vreg)
            << program.and_(hashed_index_vreg, program.constant64(hash_table_descriptor.capacity() - 1));

    {
        /// Since the entry may be in use, we scan over all entries and find an empty slot.
        auto for_loop = flounder::ForRange{program, 0U, hash_table_descriptor.capacity(), "ht_insert_entry"};
        auto index_vreg = program.vreg("ht_index");
        program << program.request_vreg64(index_vreg)
                << program.lea(index_vreg, program.mem(hashed_index_vreg, for_loop.counter_vreg()))
                << program.and_(index_vreg, program.constant64(hash_table_descriptor.capacity() - 1));

        /// Load the address where the is_used flag for this index is located.
        auto is_used_address_vreg = program.vreg("ht_is_used_address");
        program << program.request_vreg64(is_used_address_vreg)
                << program.lea(is_used_address_vreg,
                               program.mem(hash_table_vreg, index_vreg, LinearProbingTable::is_used_indicator_offset()))

                /// Check, if the slot at [index] is used; if yes, check if it is the same key.
                << program.cmp(program.mem(is_used_address_vreg, flounder::RegisterWidth::r8), program.constant8(0))
                << program.jne(compare_key_label)

                /// Otherwhise, if the slot is not used, insert:

                /// Set "is_used" to 1.
                << program.mov(program.mem(is_used_address_vreg, flounder::RegisterWidth::r8), program.constant8(1))
                << program.clear(is_used_address_vreg)

                /// Load the address where to insert
                << program.imul(index_vreg, program.constant32(LinearProbingTable::slot_width(hash_table_descriptor)))
                << program.lea(index_vreg, program.mem(index_vreg, hash_table_vreg,
                                                       LinearProbingTable::begin_offset(hash_table_descriptor)))

                /// Write the hash.
                << program.mov(program.mem(index_vreg, EntryHeader::hash_offset(), flounder::RegisterWidth::r64),
                               hash_vreg);

        /// Write the key.
        write_key_callback(program, index_vreg, EntryHeader::begin_offset());

        /// Write the entry.
        write_entry_callback(program, index_vreg, EntryHeader::begin_offset() + hash_table_descriptor.key_width());

        program
            /// Return if inserted.
            << program.jmp(for_loop.foot_label())

            /// Compare the hash and the key (if address is used).
            << program.section(compare_key_label)
            << program.imul(index_vreg, program.constant32(LinearProbingTable::slot_width(hash_table_descriptor)))
            << program.lea(index_vreg, program.mem(index_vreg, hash_table_vreg,
                                                   LinearProbingTable::begin_offset(hash_table_descriptor)))
            << program.cmp(hash_vreg, program.mem(index_vreg, EntryHeader::hash_offset(), flounder::RegisterWidth::r64))

            /// Jump to the next slot address, if the hashes differ (its onther record)
            << program.jne(for_loop.step_label());

        /// Compare the key and return, if the keys are equals (record almost exists)
        /// or go to the next slot, if there sits another entry in the slot.
        compare_key_callback(program, index_vreg, LinearProbingTable::EntryHeader::begin_offset(),
                             for_loop.foot_label(), for_loop.step_label());

        program << program.clear(index_vreg);
    }

    program << program.clear(hashed_index_vreg);
}

void LinearProbingTable::find(
    flounder::Program &program, std::string &&hash_table_identifier,
    const db::execution::compilation::hashtable::Descriptor &hash_table_descriptor, flounder::Register hash_table_vreg,
    flounder::Register hash_vreg,
    db::execution::compilation::hashtable::AbstractTable::find_compare_key_callback_t &&compare_key_callback,
    db::execution::compilation::hashtable::AbstractTable::find_entry_callback_t &&find_callback)
{
    /// This is the index in the hash table where the hash should be located.
    auto hashed_index_vreg = program.vreg(fmt::format("ht_{}_hashed_index", hash_table_identifier));
    program << program.request_vreg64(hashed_index_vreg) << program.mov(hashed_index_vreg, hash_vreg)
            << program.and_(hashed_index_vreg, program.constant64(hash_table_descriptor.capacity() - 1));

    {
        /// Since the entry may be in use, we scan over all entries and find an empty slot.
        auto for_loop = flounder::ForRange{program, 0U, hash_table_descriptor.capacity(),
                                           fmt::format("ht_{}_find_entry", hash_table_identifier)};
        auto index_vreg = program.vreg(fmt::format("ht_{}_index", hash_table_identifier));
        program << program.request_vreg64(index_vreg)
                << program.lea(index_vreg, program.mem(hashed_index_vreg, for_loop.counter_vreg()))
                << program.and_(index_vreg, program.constant64(hash_table_descriptor.capacity() - 1));

        /// Check the is_used flag for this index.
        program << program.cmp(program.mem(hash_table_vreg, index_vreg, LinearProbingTable::is_used_indicator_offset(),
                                           flounder::RegisterWidth::r8),
                               program.constant8(1))
                << program.jne(for_loop.foot_label());

        /// Load the address of the slot.
        program << program.imul(index_vreg, program.constant32(LinearProbingTable::slot_width(hash_table_descriptor)))
                << program.lea(index_vreg, program.mem(index_vreg, hash_table_vreg, 0U,
                                                       LinearProbingTable::begin_offset(hash_table_descriptor)));

        /// Compare keys
        /// First, compare hashes
        program << program.cmp(hash_vreg,
                               program.mem(index_vreg, EntryHeader::hash_offset(), flounder::RegisterWidth::r64))
                << program.jne(for_loop.step_label());

        /// program << program.add(slot_address_vreg, program.constant32(HashTable::EntryHeader::begin_offset()));
        const auto key_offset = LinearProbingTable::EntryHeader::begin_offset();
        compare_key_callback(program, index_vreg, key_offset, for_loop.step_label());

        /// Call the callback for every entry.
        if (hash_table_descriptor.is_multiple_entries_per_key())
        {
            /// When iterating over multiple entries,
            /// we need the address of the key that is stored only once.
            auto key_address_vreg = program.vreg(fmt::format("ht_{}_key_address", hash_table_identifier));
            program << program.request_vreg64(key_address_vreg)
                    << program.lea(key_address_vreg, program.mem(index_vreg, key_offset))
                    << program.add(index_vreg, program.constant32(key_offset + hash_table_descriptor.key_width()));
            {
                auto while_slot = flounder::While{
                    program,
                    flounder::IsNotEquals{flounder::Operand{index_vreg}, flounder::Operand{program.constant32(0)}},
                    fmt::format("ht_{}_slot", hash_table_identifier)};

                if (hash_table_descriptor.bucket_capacity() > 1U)
                {
                    auto slot_iterator_vreg = program.vreg(fmt::format("ht_{}_slot_iterator", hash_table_identifier));
                    program << program.request_vreg64(slot_iterator_vreg)
                            << program.lea(slot_iterator_vreg, program.mem(index_vreg, Entry::begin_offset(true)));
                    auto slot_end_vreg = program.vreg(fmt::format("ht_{}_slot_end", hash_table_identifier));
                    program << program.request_vreg64(slot_end_vreg)
                            << program.mov(slot_end_vreg,
                                           program.mem(index_vreg, Entry::size_offset(), flounder::RegisterWidth::r8))
                            << program.imul(slot_end_vreg, program.constant32(hash_table_descriptor.entry_width()))
                            << program.add(slot_end_vreg, slot_iterator_vreg);

                    {
                        auto for_each_record = flounder::ForEach{
                            program, slot_iterator_vreg, slot_end_vreg, hash_table_descriptor.entry_width(),
                            fmt::format("ht_{}_slot_entries", hash_table_identifier)};
                        find_callback(program, key_address_vreg, 0U, slot_iterator_vreg, 0U);
                    }

                    program << program.clear(slot_iterator_vreg) << program.clear(slot_end_vreg);
                }
                else
                {
                    find_callback(program, key_address_vreg, 0U, index_vreg, Entry::begin_offset(true));
                }

                program << program.mov(index_vreg, program.mem(index_vreg, Entry::next_offset()));
            }

            program << program.clear(key_address_vreg);
        }
        else
        {
            const auto entry_offset = key_offset + hash_table_descriptor.key_width() +
                                      Entry::begin_offset(hash_table_descriptor.is_multiple_entries_per_key());
            find_callback(program, index_vreg, key_offset, index_vreg, entry_offset);
        }

        program << program.clear(index_vreg) << program.jmp(for_loop.foot_label());
    }

    program << program.clear(hashed_index_vreg);
}

void LinearProbingTable::for_each(
    flounder::Program &program, std::string &&hash_table_identifier,
    const db::execution::compilation::hashtable::Descriptor &hash_table_descriptor, flounder::Register hash_table_vreg,
    db::execution::compilation::hashtable::AbstractTable::iterate_callback_t &&iterate_callback)
{
    {
        /// Since the entry may be in use, we scan over all entries and find an empty slot.
        auto for_loop = flounder::ForRange{program, 0U, hash_table_descriptor.capacity(),
                                           fmt::format("ht_{}_for_each", hash_table_identifier)};

        /// Load the address where the is_used flag for this index is located.
        auto is_used_address_vreg = program.vreg(fmt::format("ht_{}_is_used_address", hash_table_identifier));
        program << program.request_vreg64(is_used_address_vreg)
                << program.lea(is_used_address_vreg, program.mem(hash_table_vreg, for_loop.counter_vreg(), 0U,
                                                                 LinearProbingTable::is_used_indicator_offset()));

        program << program.cmp(program.mem(is_used_address_vreg, flounder::RegisterWidth::r8), program.constant8(1))
                << program.jne(for_loop.step_label()) << program.clear(is_used_address_vreg);

        /// Load the address of the slot.
        auto slot_address_vreg = program.vreg(fmt::format("ht_{}_slot_address", hash_table_identifier));
        program << program.request_vreg64(slot_address_vreg) << program.mov(slot_address_vreg, for_loop.counter_vreg())
                << program.imul(slot_address_vreg,
                                program.constant32(LinearProbingTable::slot_width(hash_table_descriptor)))
                << program.lea(slot_address_vreg, program.mem(slot_address_vreg, hash_table_vreg, 0U,
                                                              LinearProbingTable::begin_offset(hash_table_descriptor)));

        /// Call the callback for every entry.
        if (hash_table_descriptor.is_multiple_entries_per_key())
        {
            // TODO
            throw exception::NotImplementedException{"Foreach on multiple entries."};
        }
        else
        {
            const auto hash_offset = EntryHeader::hash_offset();
            const auto key_offset = EntryHeader::begin_offset();
            const auto entry_offset = EntryHeader::width(hash_table_descriptor.key_width()) +
                                      Entry::begin_offset(hash_table_descriptor.is_multiple_entries_per_key());

            iterate_callback(program, for_loop.step_label(), for_loop.foot_label(), slot_address_vreg, hash_offset,
                             key_offset, slot_address_vreg, entry_offset);
        }

        program << program.clear(slot_address_vreg);
    }
}