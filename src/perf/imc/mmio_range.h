#pragma once
#include <cstdint>

namespace perf {
class MMIORange
{
public:
    MMIORange(std::uint64_t base_address, std::uint64_t size);
    ~MMIORange();

    [[nodiscard]] std::uint32_t read32u(std::uint64_t offset);
    [[nodiscard]] std::uint64_t read64u(std::uint64_t offset);

private:
    std::int32_t _file_descriptor{-1};
    char *_mmap_address{nullptr};
    const std::uint64_t _size;
};
} // namespace perf