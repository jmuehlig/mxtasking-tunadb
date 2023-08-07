#pragma once

namespace perf {
class config
{
public:
    /**
     * @return Address of the Base Address Register (setpci -s 0:0.0 0x48.l)
     */
    [[nodiscard]] static constexpr auto base_address_register() { return 0xfed10001; }
};
} // namespace perf