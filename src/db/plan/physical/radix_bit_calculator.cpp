#include "radix_bit_calculator.h"
#include <db/execution/compilation/hashtable/table_proxy.h>
#include <mx/system/cache.h>

using namespace db::plan::physical;

std::vector<std::uint8_t> RadixBitCalculator::calculate(
    const execution::compilation::hashtable::Descriptor::Type ht_type, const std::uint16_t count_workers,
    const std::uint64_t expected_cardinality, const topology::PhysicalSchema &stored_schema,
    const std::uint32_t keys_size, const std::uint8_t entries_per_slot)
{
    constexpr auto min_radix_bits = 3U;
    constexpr auto max_radix_bits = 12U;

    /// Size of the L2 cache; each partition should fit into the L2 cache.
    const auto l2_cache_in_bytes = std::uint64_t(mx::system::cache::size<mx::system::cache::L2>() * .75);

    const auto record_size = stored_schema.row_size();

    /// Try to use only a single partition step from 3 to 5 bits.
    /// If that breaks hash table into cache-fitting-sizes and utilizes all workers; we are done.
    auto radix_bits = std::vector<std::uint8_t>{min_radix_bits};
    for (auto i = 0U; i <= (max_radix_bits - min_radix_bits); ++i)
    {
        const auto fits_into_cache = RadixBitCalculator::fits_into_cache(
            ht_type, l2_cache_in_bytes, radix_bits, expected_cardinality, keys_size, record_size, entries_per_slot);
        const auto utilizes_all_workers = RadixBitCalculator::count_partitions(radix_bits) >= count_workers;

        if (fits_into_cache && utilizes_all_workers)
        {
            return radix_bits;
        }

        radix_bits.back() += 1U;
    }

    /// If a single partition step is not enough, use a second one.
    radix_bits = {min_radix_bits, min_radix_bits};
    for (auto i = 0U; i < (max_radix_bits - min_radix_bits); ++i)
    {
        const auto fits_into_cache = RadixBitCalculator::fits_into_cache(
            ht_type, l2_cache_in_bytes, radix_bits, expected_cardinality, keys_size, record_size, entries_per_slot);
        const auto utilizes_all_workers = RadixBitCalculator::count_partitions(radix_bits) >= count_workers;

        if (fits_into_cache && utilizes_all_workers)
        {
            return radix_bits;
        }

        radix_bits[0U] += 1U;
        radix_bits[1U] += 1U;
    }

    return radix_bits;
}

bool RadixBitCalculator::fits_into_cache(const execution::compilation::hashtable::Descriptor::Type ht_type,
                                         const std::uint64_t l2_cache_size, const std::vector<std::uint8_t> &radix_bits,
                                         const std::uint64_t expected_cardinality, const std::uint32_t key_size,
                                         const std::uint32_t record_size, const std::uint8_t entries_per_slot)
{
    const auto count_partitions = RadixBitCalculator::count_partitions(radix_bits);
    const auto expected_records_per_partition = std::uint64_t(expected_cardinality / count_partitions);
    const auto allocation_capacity =
        execution::compilation::hashtable::TableProxy::allocation_capacity(expected_records_per_partition, ht_type);

    const auto size_in_bytes_per_ht =
        execution::compilation::hashtable::TableProxy::size(execution::compilation::hashtable::Descriptor{
            ht_type, allocation_capacity, key_size, record_size, entries_per_slot > 1U, entries_per_slot});

    return size_in_bytes_per_ht <= l2_cache_size;
}
