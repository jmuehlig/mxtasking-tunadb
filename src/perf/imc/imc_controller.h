#pragma once
#include "config.h"
#include "mmio_range.h"

namespace perf {
class IMCController
{
public:
    IMCController() : _mmio((config::base_address_register() & 0x0007FFFFF8000) + 0x5000, 0x6000 - 0x5000) {}
    ~IMCController() = default;

    [[nodiscard]] std::uint32_t dram_data_reads() { return _mmio.read32u(0x5050 - 0x5000); }

    [[nodiscard]] std::uint32_t dram_data_writes() { return _mmio.read32u(0x5054 - 0x5000); }

private:
    MMIORange _mmio;
};
} // namespace perf