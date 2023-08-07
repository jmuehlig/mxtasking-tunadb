#include "chained_table.h"
#include <db/exception/execution_exception.h>
#include <flounder/lib.h>
#include <fmt/core.h>
#include <mx/tasking/runtime.h>

using namespace db::execution::compilation::hashtable;

ChainedTable::~ChainedTable() noexcept
{
    if (this->_resized_table != nullptr)
    {
        mx::tasking::runtime::delete_squad<ChainedTable>(mx::resource::ptr{this->_resized_table});
    }
}

void ChainedTable::insert_or_update(
    flounder::Program &program, std::string &&hash_table_identifier,
    const db::execution::compilation::hashtable::Descriptor &hash_table_descriptor, flounder::Register hash_table_vreg,
    flounder::Register hash_vreg,
    db::execution::compilation::hashtable::AbstractTable::insert_compare_key_callback_t && /*compare_key_callback*/,
    db::execution::compilation::hashtable::AbstractTable::write_key_callback_t &&write_key_callback,
    db::execution::compilation::hashtable::AbstractTable::write_entry_callback_t &&write_entry_callback,
    std::optional<write_entry_callback_t> &&update_entry_callback)
{
    auto insert_entry_label = program.label(fmt::format("ht_{}_insert_entry", hash_table_identifier));
    auto update_entry_label = program.label(fmt::format("ht_{}_update_entry", hash_table_identifier));
    auto end_label = program.label(fmt::format("ht_{}_inserted", hash_table_identifier));

    auto capacity_vreg = program.vreg(fmt::format("ht_{}_capacity", hash_table_identifier));
    auto capacity_mask_vreg = program.vreg(fmt::format("ht_{}_capacity_mask", hash_table_identifier));
    auto index_vreg = program.vreg(fmt::format("entry_{}_index", hash_table_identifier));
    auto entry_address_vreg = program.vreg(fmt::format("entry_{}_address", hash_table_identifier));

    program
        /// Load the capacity
        << program.request_vreg64(capacity_vreg)
        << program.mov(capacity_vreg, program.mem(hash_table_vreg, ChainedTable::capacity_offset()))

        /// Load the index of the hash: index = (hash & (capacity - 1))
        << program.request_vreg64(index_vreg) << program.request_vreg64(capacity_mask_vreg)
        << program.lea(capacity_mask_vreg, program.mem(capacity_vreg, -1)) << program.mov(index_vreg, hash_vreg)
        << program.and_(index_vreg, capacity_mask_vreg)
        << program.clear(capacity_mask_vreg)

        /// Load the address of the entry: index * slot_width + hashtable_address + begin_offset + capacity
        << program.request_vreg64(entry_address_vreg) << program.mov(entry_address_vreg, index_vreg)
        << program.imul(entry_address_vreg, program.constant32(ChainedTable::slot_width(hash_table_descriptor)))
        << program.lea(entry_address_vreg,
                       program.mem(entry_address_vreg, hash_table_vreg, ChainedTable::is_used_offset()))
        << program.add(entry_address_vreg, capacity_vreg);

    /// Append only.
    if (update_entry_callback.has_value() == false)
    {
        /// If the entry is not used.
        {
            /**
             * if is_used == 0:
             *      write key
             *      write entry
             *      write is used bit
             */
            auto is_used_mem =
                program.mem(index_vreg, hash_table_vreg, ChainedTable::is_used_offset(), flounder::RegisterWidth::r8);

            auto if_is_not_used = flounder::If{
                program, flounder::IsEquals{flounder::Operand{is_used_mem}, flounder::Operand{program.constant8(0)}},
                fmt::format("if_entry_{}_is_not_used", hash_table_identifier)};

            program
                /// Mark entry as used.
                << program.mov(is_used_mem, program.constant8(1))
                << program.clear(index_vreg)

                /// Set overflow to zero
                << program.mov(
                       program.mem(entry_address_vreg, Entry::overflow_index_offset(), flounder::RegisterWidth::r32),
                       program.constant32(0));

            /// Persist the entry.
            write_key_callback(program, entry_address_vreg, Entry::key_offset());
            write_entry_callback(program, entry_address_vreg, Entry::entry_offset(hash_table_descriptor.key_width()));

            /// Jump to the end.
            program << program.jmp(end_label);
        }

        /// If the entry is used, append an overflow index.
        {
            /**
             * else:
             *      idx = ++hash_table->_next_overflow_index
             *      overflows[idx].next = entry.next
             *      entry.next = idx
             *      entry = overflows[idx]
             *      write key
             *      write entry
             */

            auto overflow_index_vreg = program.vreg(fmt::format("ht_{}_overflow_index", hash_table_identifier));
            auto overflow_entry_address_vreg =
                program.vreg(fmt::format("ht_{}_overflow_entry_address", hash_table_identifier));
            auto entry_overflow_index_vreg =
                program.vreg(fmt::format("ht_{}_entry_overflow_index", hash_table_identifier));
            auto overflow_next_index_mem =
                program.mem(hash_table_vreg, ChainedTable::next_overflow_index_offset(), flounder::RegisterWidth::r32);
            auto entry_overflow_index_mem =
                program.mem(entry_address_vreg, Entry::overflow_index_offset(), flounder::RegisterWidth::r32);
            program
                /// Calculate next overflow index and write back.
                << program.request_vreg32(overflow_index_vreg)
                << program.mov(overflow_index_vreg, overflow_next_index_mem)
                << program.lea(overflow_index_vreg, program.mem(overflow_index_vreg, 1))
                << program.mov(overflow_next_index_mem, overflow_index_vreg)

                /// Load the overflow entry: (capacity + index) * slot_width + hash_table + begin + capacity
                << program.request_vreg64(overflow_entry_address_vreg)
                << program.lea(overflow_entry_address_vreg, program.mem(capacity_vreg, overflow_index_vreg))
                << program.imul(overflow_entry_address_vreg,
                                program.constant32(ChainedTable::slot_width(hash_table_descriptor)))
                << program.lea(overflow_entry_address_vreg, program.mem(overflow_entry_address_vreg, hash_table_vreg,
                                                                        ChainedTable::is_used_offset()))
                << program.add(overflow_entry_address_vreg, capacity_vreg)
                << program.clear(capacity_vreg)

                /// Remember the entries overflow index and write the new overflow index into the entry.
                << program.request_vreg32(entry_overflow_index_vreg)
                << program.mov(entry_overflow_index_vreg, entry_overflow_index_mem)
                << program.mov(entry_overflow_index_mem, overflow_index_vreg) << program.clear(entry_address_vreg)
                << program.clear(overflow_index_vreg)

                /// Write the entries overflow into the overflow entry.
                << program.mov(program.mem(overflow_entry_address_vreg, Entry::overflow_index_offset(),
                                           flounder::RegisterWidth::r32),
                               entry_overflow_index_vreg)
                << program.clear(entry_overflow_index_vreg);

            write_key_callback(program, overflow_entry_address_vreg, Entry::key_offset());
            write_entry_callback(program, overflow_entry_address_vreg,
                                 Entry::entry_offset(hash_table_descriptor.key_width()));
            program << program.clear(overflow_entry_address_vreg);
        }
    }

    /// Try to find a matching key, first.
    else
    {
        program << program.section(update_entry_label);
        assert(false);
        // TODO
    }

    program << program.section(end_label);
}

void ChainedTable::find(
    flounder::Program &program, std::string &&hash_table_identifier,
    const db::execution::compilation::hashtable::Descriptor &hash_table_descriptor, flounder::Register hash_table_vreg,
    flounder::Register hash_vreg,
    db::execution::compilation::hashtable::AbstractTable::find_compare_key_callback_t &&compare_key_callback,
    db::execution::compilation::hashtable::AbstractTable::find_entry_callback_t &&find_callback)
{
    auto next_overflow_entry_label = program.label(fmt::format("ht_{}_next_overflow", hash_table_identifier));

    /// Load the address of the slot.
    /// index = (hash & (capacity - 1))
    auto capacity_vreg = program.vreg(fmt::format("ht_{}_capacity", hash_table_identifier));
    auto capacity_mask_vreg = program.vreg(fmt::format("ht_{}_capacity_mask", hash_table_identifier));
    auto index_vreg = program.vreg(fmt::format("entry_{}_index", hash_table_identifier));
    auto entry_address_vreg = program.vreg(fmt::format("entry_{}_address", hash_table_identifier));

    program
        /// Load the capacity
        << program.request_vreg64(capacity_vreg)
        << program.mov(capacity_vreg, program.mem(hash_table_vreg, ChainedTable::capacity_offset()))
        /// Load the index of the hash: index = (hash & (capacity - 1))
        << program.request_vreg64(index_vreg) << program.request_vreg64(capacity_mask_vreg)
        << program.lea(capacity_mask_vreg, program.mem(capacity_vreg, -1))

        << program.mov(index_vreg, hash_vreg) << program.and_(index_vreg, capacity_mask_vreg)

        << program.clear(capacity_mask_vreg)

        /// Load the address of the entry: index * slot_width + hashtable_address + begin_offset + capacity
        << program.request_vreg64(entry_address_vreg) << program.mov(entry_address_vreg, index_vreg)
        << program.imul(entry_address_vreg, program.constant32(ChainedTable::slot_width(hash_table_descriptor)))
        << program.lea(entry_address_vreg,
                       program.mem(entry_address_vreg, hash_table_vreg, ChainedTable::is_used_offset()))
        << program.add(entry_address_vreg, capacity_vreg);

    /**
     * if entry->is_used:
     *      compare // if not matching, jmp to overflow
     *      find
     *      jmp end
     *
     *      overflow:
     *      idx = entry->overflow_index
     *      while idx > 0:
     *          entry = overflow[idx]
     *          compare // if not matching, jmp to next_overflow
     *          find
     *          jmp end
     *          next_overflow:
     *              idx = entry->overflow_index
     */
    auto is_used_mem =
        program.mem(index_vreg, hash_table_vreg, ChainedTable::is_used_offset(), flounder::RegisterWidth::r8);
    {
        auto if_entry_is_used = flounder::If{
            program, flounder::IsEquals{flounder::Operand{is_used_mem}, flounder::Operand{program.constant8(1)}},
            fmt::format("entry_{}_is_used", hash_table_identifier)};

        auto overflow_index_vreg = program.vreg(fmt::format("ht_{}_overflow_index", hash_table_identifier));
        program << program.clear(index_vreg) << program.request_vreg32(overflow_index_vreg);

        {
            auto do_while_has_next = flounder::DoWhile(
                program,
                flounder::IsGreater{flounder::Operand{overflow_index_vreg}, flounder::Operand{program.constant32(0)}},
                fmt::format("while_ht_{}_has_overflow", hash_table_identifier));

            /// Check the key.
            compare_key_callback(program, entry_address_vreg, Entry::key_offset(), next_overflow_entry_label);

            /// Consume the found entry.
            find_callback(program, entry_address_vreg, Entry::key_offset(), entry_address_vreg,
                          Entry::entry_offset(hash_table_descriptor.key_width()));

            /// If only a single entry per key is possible, we are finished.
            if (hash_table_descriptor.is_multiple_entries_per_key() == false)
            {
                program << program.jmp(if_entry_is_used.foot_label());
            }

            /// If we did not match the key, go to the overflow table.
            program << program.section(next_overflow_entry_label)

                    /// Load the overflow index
                    << program.mov(overflow_index_vreg, program.mem(entry_address_vreg, Entry::overflow_index_offset(),
                                                                    flounder::RegisterWidth::r32))

                    /// Calculate the next entry
                    << program.lea(entry_address_vreg, program.mem(overflow_index_vreg, capacity_vreg))
                    << program.imul(entry_address_vreg,
                                    program.constant32(ChainedTable::slot_width(hash_table_descriptor)))
                    << program.lea(entry_address_vreg,
                                   program.mem(hash_table_vreg, entry_address_vreg, ChainedTable::is_used_offset()))
                    << program.add(entry_address_vreg, capacity_vreg);
            ;
        }

        // TODO: Optimizations:
        //  - DoWhile until entry_address == first overflow offset address
        //    (this way, we do not need the overflow_index_vreg and can use the first overflow offset address for
        //    entry_address calculation)

        program << program.clear(entry_address_vreg) << program.clear(overflow_index_vreg)
                << program.clear(capacity_vreg);
    }
}

void ChainedTable::for_each(flounder::Program &program, std::string &&hash_table_identifier,
                            const db::execution::compilation::hashtable::Descriptor &hash_table_descriptor,
                            flounder::Register hash_table_vreg,
                            db::execution::compilation::hashtable::AbstractTable::iterate_callback_t &&iterate_callback)
{
    auto capacity_vreg = program.vreg(fmt::format("ht_{}_capacity", hash_table_identifier));
    /// Load the capacity
    program << program.request_vreg64(capacity_vreg)
            << program.mov(capacity_vreg, program.mem(hash_table_vreg, ChainedTable::capacity_offset()));

    {
        auto for_index = flounder::ForRange{program, 0U, flounder::Operand{capacity_vreg},
                                            fmt::format("for_ht_{}_index", hash_table_identifier)};

        {
            auto is_used_mem = program.mem(hash_table_vreg, for_index.counter_vreg(), ChainedTable::is_used_offset(),
                                           flounder::RegisterWidth::r8);

            auto if_is_used = flounder::If{
                program, flounder::IsEquals{flounder::Operand{is_used_mem}, flounder::Operand{program.constant8(1)}},
                fmt::format("if_ht_{}_index_is_used", hash_table_identifier)};

            auto entry_address_vreg = program.vreg(fmt::format("entry_{}_address", hash_table_identifier));
            auto overflow_index_vreg = program.vreg(fmt::format("ht_{}_overflow_index", hash_table_identifier));
            program << program.request_vreg64(entry_address_vreg)
                    << program.mov(entry_address_vreg, for_index.counter_vreg())
                    << program.imul(entry_address_vreg,
                                    program.constant32(ChainedTable::slot_width(hash_table_descriptor)))
                    << program.lea(entry_address_vreg,
                                   program.mem(entry_address_vreg, hash_table_vreg, ChainedTable::is_used_offset()))
                    << program.add(entry_address_vreg, capacity_vreg)

                    << program.request_vreg32(overflow_index_vreg);

            auto next_overflow_label = program.label(fmt::format("next_{}_entry_overflow", hash_table_identifier));

            {
                auto while_has_overflow_index =
                    flounder::DoWhile{program,
                                      flounder::IsGreater{flounder::Operand{overflow_index_vreg},
                                                          flounder::Operand{program.constant32(0)}},
                                      fmt::format("while_entry_{}_has_overflow_index", hash_table_identifier)};

                iterate_callback(program, next_overflow_label, while_has_overflow_index.foot_label(),
                                 entry_address_vreg, Entry::key_offset(), Entry::key_offset(), entry_address_vreg,
                                 Entry::entry_offset(hash_table_descriptor.key_width()));

                program << program.section(next_overflow_label)
                        /// Load the overflow index
                        << program.mov(overflow_index_vreg,
                                       program.mem(entry_address_vreg, Entry::overflow_index_offset(),
                                                   flounder::RegisterWidth::r32))

                        /// Calculate the next entry
                        << program.lea(entry_address_vreg, program.mem(overflow_index_vreg, capacity_vreg))
                        << program.imul(entry_address_vreg,
                                        program.constant32(ChainedTable::slot_width(hash_table_descriptor)))
                        << program.lea(entry_address_vreg,
                                       program.mem(hash_table_vreg, entry_address_vreg, ChainedTable::is_used_offset()))
                        << program.add(entry_address_vreg, capacity_vreg);
            }

            program << program.clear(entry_address_vreg) << program.clear(overflow_index_vreg);
        }
    }

    program << program.clear(capacity_vreg);
}

void ChainedTable::replace_hash_table_address_with_resized_hash_table(flounder::Program &program,
                                                                      std::string &&hash_table_identifier,
                                                                      flounder::Register hash_table_vreg)
{
    auto reallocated_table_mem = program.mem(hash_table_vreg, ChainedTable::resized_table_offset(), flounder::r64);
    {
        auto if_has_reallocated_table = flounder::If{
            program,
            flounder::IsNotEquals{flounder::Operand{reallocated_table_mem}, flounder::Operand{program.constant32(0)}},
            fmt::format("if_ht_{}_has_resized_table", hash_table_identifier)};
        program << program.mov(hash_table_vreg, reallocated_table_mem);
    }
}

void ChainedTable::resize_if_required(flounder::Program &program,
                                      const db::execution::compilation::hashtable::Descriptor &hash_table_descriptor,
                                      flounder::Register hash_table_vreg, create_hash_callback_t &&create_hash_callback)
{
    auto overflow_size_vreg = program.vreg("ht_overflow_size");
    auto capacity_vreg = program.vreg("ht_capacity");
    program << program.request_vreg64(overflow_size_vreg)
            << program.mov(overflow_size_vreg, program.mem(hash_table_vreg, ChainedTable::next_overflow_index_offset(),
                                                           flounder::RegisterWidth::r32))
            << program.request_vreg64(capacity_vreg)
            << program.mov(capacity_vreg, program.mem(hash_table_vreg, ChainedTable::capacity_offset()))
            << program.sub(capacity_vreg, program.constant32(config::tuples_per_tile()));

    {
        auto if_resize_required = flounder::If{
            program,
            flounder::IsGreaterEquals{flounder::Operand{overflow_size_vreg}, flounder::Operand{capacity_vreg}, false},
            "if_realloc_required"};
        program << program.clear(overflow_size_vreg) << program.clear(capacity_vreg);

        /// Allocate a new hash table.
        auto resized_hash_table_vreg =
            flounder::FunctionCall{program, reinterpret_cast<const uintptr_t>(&ChainedTable::create_resized_table),
                                   "resized_hash_table_addr"}
                .call({flounder::Operand{hash_table_vreg}});

        ChainedTable::for_each(
            program, "to_resize_table", hash_table_descriptor, hash_table_vreg,
            [resized_hash_table_vreg, &hash_table_descriptor,
             &create_hash_callback](flounder::Program &program_, flounder::Label /*next_step_label*/,
                                    flounder::Label /*foot_label*/, flounder::Register key_address_vreg,
                                    const std::uint32_t /*hash_offset*/, const std::uint32_t key_offset,
                                    flounder::Register entry_address_vreg, const std::uint32_t entry_offset) {
                /// Rehash the key.
                auto hash_vreg = create_hash_callback(program_, key_address_vreg, key_offset);

                ChainedTable::insert(
                    program_, "resize", hash_table_descriptor, resized_hash_table_vreg.value(), hash_vreg,
                    insert_compare_key_callback_t{},
                    [key_address_vreg, key_offset, key_size = hash_table_descriptor.key_width()](
                        flounder::Program &program__, flounder::Register target_key_vreg,
                        const std::uint32_t target_key_offset) {
                        flounder::Lib::memcpy(program__, target_key_vreg, target_key_offset, key_address_vreg,
                                              key_offset, key_size);
                    },
                    [entry_address_vreg, entry_offset, entry_size = hash_table_descriptor.entry_width()](
                        flounder::Program &program__, flounder::Register target_entry_vreg,
                        const std::uint32_t target_entry_offset) {
                        flounder::Lib::memcpy(program__, target_entry_vreg, target_entry_offset, entry_address_vreg,
                                              entry_offset, entry_size);
                    });

                program_ << program_.clear(hash_vreg);
            });

        /// Move the address for the new hash table into the register.
        program << program.mov(hash_table_vreg, resized_hash_table_vreg.value())
                << program.clear(resized_hash_table_vreg.value());
    }
}

ChainedTable *ChainedTable::reallocate()
{
    auto resized_descriptor = ChainedTable::resize_descriptor(this->descriptor());

    /// Create a new table with doubled capacity.
    auto resized_table = mx::tasking::runtime::new_squad<execution::compilation::hashtable::ChainedTable>(
        ChainedTable::size(resized_descriptor), mx::tasking::runtime::worker_id(), resized_descriptor);
    auto *resized_chained_table = resized_table.get<ChainedTable>();

    /// Set pointer to resized table and base table.
    if (this->_base_table != nullptr)
    {
        /// TODO: In this case, the old table has to be deleted.
        resized_chained_table->_base_table = this->_base_table;
        this->_base_table->_resized_table = resized_chained_table;
    }
    else
    {
        resized_chained_table->_base_table = this;
    }
    this->_resized_table = resized_chained_table;

    resized_chained_table->initialize_empty();

    return resized_chained_table;
}

void ChainedTable::dump(const std::uintptr_t hash_table_ptr)
{
    auto *hash_table = reinterpret_cast<ChainedTable *>(hash_table_ptr);
    std::cout << "Capacity: " << hash_table->_capacity << std::endl;
    std::cout << "Next Overflow ID: " << hash_table->_next_overflow_offset << std::endl;

    auto chain_length = std::unordered_map<std::uint64_t, std::uint64_t>{};
    auto count_in_first_bucket = 0ULL;
    auto count_overflow_entries = 0ULL;

    for (auto i = 0U; i < hash_table->descriptor().capacity(); ++i)
    {
        auto *is_used = reinterpret_cast<std::int8_t *>(hash_table_ptr + ChainedTable::is_used_offset() + i);

        if (*is_used)
        {
            auto *entry = reinterpret_cast<ChainedTable::Entry *>(
                hash_table_ptr + ChainedTable::is_used_offset() + hash_table->_capacity +
                i * ChainedTable::slot_width(hash_table->descriptor()));

            ++count_in_first_bucket;

            auto length = 0U;
            auto overflow_index = entry->overflow_index();
            while (overflow_index > 0U)
            {
                ++length;
                ++count_overflow_entries;
                auto *overflow_entry = reinterpret_cast<ChainedTable::Entry *>(
                    hash_table_ptr + ChainedTable::is_used_offset() + hash_table->_capacity +
                    (overflow_index + hash_table->_capacity) * ChainedTable::slot_width(hash_table->descriptor()));
                overflow_index = overflow_entry->overflow_index();
            }

            if (chain_length.contains(length) == false)
            {
                chain_length.insert(std::make_pair(length, 0U));
            }

            chain_length.at(length)++;
        }
    }

    std::cout << "Entries in Buckets / In Overflow: " << count_in_first_bucket << " / " << count_overflow_entries
              << std::endl;
    for (const auto &[length, count] : chain_length)
    {
        std::cout << "    " << length << " = " << count << std::endl;
    }

    std::cout << std::endl;
}