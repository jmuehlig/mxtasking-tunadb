#pragma once

#include <cstdint>
#include <db/topology/physical_schema.h>

namespace db::execution::compilation {
class ScanAccessCharacteristic
{
public:
    [[nodiscard]] constexpr static ScanAccessCharacteristic from_to(const std::uint32_t min,
                                                                    const std::uint32_t max) noexcept
    {
        return ScanAccessCharacteristic{min, max};
    }

    [[nodiscard]] static ScanAccessCharacteristic everything(const topology::PhysicalSchema &schema) noexcept
    {
        return ScanAccessCharacteristic{0U, schema.row_size() - 1U};
    }

    constexpr ScanAccessCharacteristic(const std::uint32_t min_accessed_byte,
                                       const std::uint32_t max_accessed_byte) noexcept
        : _min_accessed_byte(min_accessed_byte), _max_accessed_byte(max_accessed_byte)
    {
    }

    ScanAccessCharacteristic(ScanAccessCharacteristic &&) noexcept = default;
    ScanAccessCharacteristic(const ScanAccessCharacteristic &) noexcept = default;

    constexpr ScanAccessCharacteristic() noexcept = default;

    ScanAccessCharacteristic &operator=(ScanAccessCharacteristic &&) noexcept = default;

    ~ScanAccessCharacteristic() noexcept = default;

    [[nodiscard]] std::uint32_t min_accessed_byte() const noexcept { return _min_accessed_byte; }
    [[nodiscard]] std::uint32_t max_accessed_byte() const noexcept { return _max_accessed_byte; }

private:
    std::uint32_t _min_accessed_byte{0U};
    std::uint32_t _max_accessed_byte{0U};
};
} // namespace db::execution::compilation