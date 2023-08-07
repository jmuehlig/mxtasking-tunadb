#pragma once

#include <cstdint>
#include <db/execution/compilation/hashtable/descriptor.h>
#include <db/topology/physical_schema.h>
#include <vector>

namespace db::plan::physical {
class RadixBitCalculator
{
public:
    [[nodiscard]] static std::vector<std::uint8_t> calculate(
        execution::compilation::hashtable::Descriptor::Type ht_type, std::uint16_t count_workers,
        std::uint64_t expected_cardinality, const topology::PhysicalSchema &stored_schema, std::uint32_t keys_size,
        std::uint8_t entries_per_slot);

    /**
     * Calculates the number of partitions for a specific phase specified
     * by the given radix bits.
     *
     * @param radix_bits Bits used for partitioning.
     * @param phase Phase to calculate the number of partitions for.
     * @return Number of partitions.
     */
    [[nodiscard]] static std::uint16_t count_partitions(const std::vector<std::uint8_t> &radix_bits,
                                                        const std::uint8_t phase) noexcept
    {
        if (phase == 0U)
        {
            return std::pow(2U, radix_bits.front());
        }

        return std::pow(2U, radix_bits[phase]) * count_partitions(radix_bits, phase - 1U);
    }

    /**
     * Calculates the number of partitions for a specific phase specified
     * by the given radix bits.
     *
     * @param radix_bits Bits used for partitioning.
     * @return Number of partitions.
     */
    [[nodiscard]] static std::uint16_t count_partitions(const std::vector<std::uint8_t> &radix_bits) noexcept
    {
        return count_partitions(radix_bits, radix_bits.size() - 1U);
    }

private:
    [[nodiscard]] static bool fits_into_cache(execution::compilation::hashtable::Descriptor::Type ht_type,
                                              std::uint64_t l2_cache_size, const std::vector<std::uint8_t> &radix_bits,
                                              std::uint64_t expected_cardinality, std::uint32_t key_size,
                                              std::uint32_t record_size, std::uint8_t entries_per_slot);
};
} // namespace db::plan::physical