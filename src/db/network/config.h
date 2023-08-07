#pragma once

namespace db::network {
class config
{
public:
    static constexpr auto max_connections() noexcept { return 64U; }
};
} // namespace db::network