#pragma once

#include "descriptor.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <db/config.h>
#include <flounder/program.h>
#include <flounder/statement.h>
#include <functional>
#include <mx/tasking/task.h>
#include <mx/tasking/task_squad.h>

namespace db::execution::compilation::hashtable {
class AbstractTable : public mx::tasking::TaskSquad
{
public:
    /**
     * Callback that compares the keys in the hash table located at the address in the register with
     * keys from the record. Jump to the first label keys are equals, otherwhise else.
     */
    using insert_compare_key_callback_t =
        std::function<void(flounder::Program &, flounder::Register, std::uint32_t, flounder::Label, flounder::Label)>;

    /**
     * Callback that compares the keys in the hash table located at the address in the register with
     * keys from the record and jumps to the given label when the keys are not eqal.
     */
    using find_compare_key_callback_t =
        std::function<void(flounder::Program &, flounder::Register, std::uint32_t, flounder::Label)>;

    /**
     * Callback that writes the keys to the address in the given register + the given offset.
     */
    using write_key_callback_t = std::function<void(flounder::Program &, flounder::Register, std::uint32_t)>;

    /**
     * Callback that writes the entry to the address in the given register.
     */
    using write_entry_callback_t = std::function<void(flounder::Program &, flounder::Register, std::uint32_t)>;

    /**
     * Callback that is called for every entry when using find.
     */
    using find_entry_callback_t =
        std::function<void(flounder::Program &, flounder::Register, std::uint32_t, flounder::Register, std::uint32_t)>;

    /**
     * Callback that is called for every entry in the hash table with the next step label, the foot label, the slot
     * address register, hash offset, key offset, and the entry address register and the offset within the entry.
     */
    using iterate_callback_t =
        std::function<void(flounder::Program &, flounder::Label, flounder::Label, flounder::Register, std::uint32_t,
                           std::uint32_t, flounder::Register, std::uint32_t)>;

    /**
     * Callback to create a hash from key(s) with the register to the key and the offset. Returns the register of the
     * hash.
     */
    using create_hash_callback_t =
        std::function<flounder::Register(flounder::Program &, flounder::Register, std::uint32_t)>;

    AbstractTable(Descriptor descriptor) noexcept : _descriptor(descriptor) {}

    ~AbstractTable() noexcept override = default;

    [[nodiscard]] const Descriptor &descriptor() const noexcept { return _descriptor; }

    virtual void initialize_empty() = 0;

private:
    Descriptor _descriptor;
};

class InitializeTableTask final : public mx::tasking::TaskInterface
{
public:
    explicit InitializeTableTask(AbstractTable *hash_table) noexcept : _hash_table(hash_table) {}

    ~InitializeTableTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t /*worker_id*/) override
    {
        this->_hash_table->initialize_empty();
        return mx::tasking::TaskResult::make_remove();
    }

    [[nodiscard]] std::uint64_t trace_id() const noexcept override { return config::task_id_hash_table_memset(); }

private:
    AbstractTable *_hash_table;
};
} // namespace db::execution::compilation::hashtable