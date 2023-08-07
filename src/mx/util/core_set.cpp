#include "core_set.h"
#include <algorithm>
#include <mx/system/cpu.h>
#include <mx/tasking/config.h>
#include <numeric>
#include <vector>

using namespace mx::util;

core_set core_set::build(std::uint16_t count_cores, const Order order)
{
    count_cores =
        std::min(count_cores, std::min(std::uint16_t(tasking::config::max_cores()), system::cpu::count_cores()));

    auto set = core_set{};
    if (order == Ascending)
    {
        for (auto i = 0U; i < count_cores; ++i)
        {
            set.emplace_back(i);
        }
    }
    else if (order == NUMAAware)
    {
        /// List of all available core ids.
        std::vector<std::uint16_t> numa_sorted_core_ids(system::cpu::count_cores());

        /// Fill from 0 to N-1.
        std::iota(numa_sorted_core_ids.begin(), numa_sorted_core_ids.end(), 0U);

        /// Sort by NUMA Node IDs (lower... upper).
        core_set::sort_by_numa(numa_sorted_core_ids);

        /// Emplace the first K core ids from the by NUMA sorted list..
        for (auto i = 0U; i < count_cores; ++i)
        {
            set.emplace_back(numa_sorted_core_ids[i]);
        }
    }
    else if (order == Physical)
    {
        const auto count_available_cores = system::cpu::count_cores();

        /// List of all available physical ids.
        auto physical_core_ids = std::vector<std::uint16_t>{};
        physical_core_ids.reserve(count_available_cores);

        /// List of all hyper thread ids.
        auto smt_core_ids = std::vector<std::uint16_t>{};
        smt_core_ids.reserve(count_available_cores);

        /// Fill both lists.
        for (auto i = 0U; i < count_available_cores; ++i)
        {
            if (system::cpu::is_smt_core(i) == false)
            {
                physical_core_ids.emplace_back(i);
            }
            else
            {
                smt_core_ids.emplace_back(i);
            }
        }

        /// Sort by NUMA Node IDs (lower... upper).
        core_set::sort_by_numa(physical_core_ids);
        core_set::sort_by_numa(smt_core_ids);

        /// Make a full list.
        std::move(smt_core_ids.begin(), smt_core_ids.end(), std::back_inserter(physical_core_ids));

        /// Emplace the first K core ids from the by NUMA sorted list..
        for (auto i = 0U; i < count_cores; ++i)
        {
            set.emplace_back(physical_core_ids[i]);
        }
    }

    return set;
}

void core_set::sort_by_numa(std::vector<std::uint16_t> &core_ids)
{
    std::sort(core_ids.begin(), core_ids.end(), [](const std::uint16_t &left, const std::uint16_t &right) {
        const auto left_node = system::cpu::node_id(left);
        const auto right_node = system::cpu::node_id(right);
        if (left_node == right_node)
        {
            return left < right;
        }

        return left_node < right_node;
    });
}

namespace mx::util {
std::ostream &operator<<(std::ostream &stream, const core_set &core_set)
{
    for (auto i = 0U; i < core_set.count_cores(); i++)
    {
        if (i > 0U)
        {
            stream << " ";
        }
        stream << core_set[i];
        if (core_set._is_worker_on_smt_thread[i])
        {
            stream << "*";
        }
    }
    return stream << std::flush;
}
} // namespace mx::util
