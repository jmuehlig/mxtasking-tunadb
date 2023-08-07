#pragma once

#include <algorithm>
#include <cstdint>
// #include <experimental/fixed_capacity_vector>
#include <ecpp/static_vector.hpp>
#include <mx/tasking/config.h>
#include <numa.h>
#include <sched.h>
#include <sys/sysinfo.h>
#include <thread>
#include <unordered_map>

namespace mx::system {
/**
 * Encapsulates methods for retrieving information
 * about the hardware landscape.
 */
class cpu
{
public:
    /**
     * @return Core where the caller is running.
     */
    [[nodiscard]] static std::uint16_t core_id() { return std::uint16_t(sched_getcpu()); }

    /**
     * Reads the NUMA region identifier of the given core.
     *
     * @param core_id Id of the core.
     * @return Id of the NUMA region the core stays in.
     */
    [[nodiscard]] static std::uint8_t node_id(const std::uint16_t core_id)
    {
        return std::max(numa_node_of_cpu(core_id), 0);
    }

    /**
     * Reads the NUMA region identifier of the current core.
     *
     * @return Id of the NUMA region the core stays in.
     */
    [[nodiscard]] static std::uint8_t node_id() { return std::max(numa_node_of_cpu(core_id()), 0); }

    /**
     * @return The greatest NUMA region identifier.
     */
    [[nodiscard]] static std::uint8_t max_node_id() { return std::uint8_t(numa_max_node()); }

    /**
     * @return Number of available cores.
     */
    [[nodiscard]] static std::uint16_t count_cores() { return std::uint16_t(get_nprocs_conf()); }

    /**
     * Checks if a given core is "the smt core" of a physical core.
     *
     *
     * @param core_id Core to check.
     * @return True, if the given core is "the smt core".
     */
    [[nodiscard]] static bool is_smt_core(std::uint16_t core_id);

    /**
     * Spots the sibling cores (other logical cores at the same
     * physical core) of a specific core.
     *
     * @param core_id Core to get the siblings of.
     * @return List of sibling core ids (if any).
     */
    [[nodiscard]] static ecpp::static_vector<std::uint16_t, tasking::config::max_smt_threads() - 1U> sibling_core_ids(
        std::uint16_t core_id);

private:
    static inline std::unordered_map<std::uint16_t,
                                     ecpp::static_vector<std::uint16_t, tasking::config::max_smt_threads()>>
        _logical_core_sibling_ids;

    [[nodiscard]] static ecpp::static_vector<std::uint16_t, tasking::config::max_smt_threads()> logical_core_ids(
        std::uint16_t core_id);
};
} // namespace mx::system