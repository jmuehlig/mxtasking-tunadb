#pragma once

#include <cstdint>
#include <db/execution/compilation/context.h>
#include <db/execution/compilation/hashtable/abstract_table.h>

namespace db::execution::compilation {
template <bool USE_TOKEN> class HashtableOutputProvider final : public OutputProviderInterface
{
public:
    explicit HashtableOutputProvider(const std::vector<mx::resource::ptr> &hash_tables) noexcept
        : _hash_tables(hash_tables)
    {
    }

    explicit HashtableOutputProvider(std::vector<mx::resource::ptr> &&hash_tables) noexcept
        : _hash_tables(std::move(hash_tables))
    {
    }

    ~HashtableOutputProvider() override
    {
        for (const auto hash_table : _hash_tables)
        {
            // TODO: This case differentation is because RH-HashTables are allocated as
            //  squads for Radix Join and by default global heap for grouped aggregation.
            //  Maybe this can be aligned for both cases (e.g., use resource allocator either way).
            if (hash_table.synchronization_primitive() == mx::synchronization::primitive::Batched)
            {
                auto *ht = hash_table.template get<hashtable::AbstractTable>();
                mx::tasking::runtime::delete_squad<hashtable::AbstractTable>(hash_table);
            }
            else
            {
                auto *ht = hash_table.template get<hashtable::AbstractTable>();
                const auto size = hashtable::TableProxy::size(ht->descriptor());
                ht->~AbstractTable();
                mx::memory::GlobalHeap::free(hash_table.template get(), size);
            }
        }
    }

    std::uintptr_t get(const std::uint16_t worker_id, std::optional<std::reference_wrapper<const RecordToken>> token,
                       mx::tasking::dataflow::EmitterInterface<execution::RecordSet> & /*graph*/,
                       mx::tasking::dataflow::NodeInterface<execution::RecordSet> * /*node*/) override
    {
        if constexpr (USE_TOKEN)
        {
            /// Some operators will annotate the hash table (radix join).
            return std::uintptr_t(token.value().get().data().secondary_input().template get());
        }
        else
        {
            /// Other operators have one hash table per worker (grouped aggregation).
            return std::uintptr_t(_hash_tables[worker_id].get());
        }
    }

private:
    /// List of hash tables.
    /// The hash tables may be used per worker (e.g., for grouped aggregation)
    /// or just for clean up, when the hash tables are annotated (radix join).
    std::vector<mx::resource::ptr> _hash_tables;
};
} // namespace db::execution::compilation