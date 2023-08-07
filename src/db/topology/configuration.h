#pragma once

#include <cstdint>
#include <mx/util/core_set.h>

namespace db::topology {
class Configuration
{
public:
    constexpr Configuration() noexcept = default;
    ~Configuration() noexcept = default;

    void count_cores(const std::uint16_t cores) noexcept { _count_cores = cores; }
    void cores_order(const mx::util::core_set::Order order) noexcept { _cores_order = order; }

    [[nodiscard]] std::uint16_t count_cores() const noexcept { return _count_cores; }
    [[nodiscard]] mx::util::core_set::Order cores_order() const noexcept { return _cores_order; }

private:
    std::uint16_t _count_cores{0U};
    mx::util::core_set::Order _cores_order{mx::util::core_set::Order::NUMAAware};
};
} // namespace db::topology