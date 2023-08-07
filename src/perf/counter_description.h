#pragma once
#include <cstdint>
#include <optional>
#include <string>

namespace perf {
/**
 * Description for a performance counter, describing the name
 * and the type.
 *
 * For more Performance Counter take a look into the Manual from Intel:
 *  https://software.intel.com/sites/default/files/managed/8b/6e/335279_performance_monitoring_events_guide.pdf
 *
 * To get event ids from manual specification see libpfm4:
 *  http://www.bnikolic.co.uk/blog/hpc-prof-events.html
 * Clone, Make, use examples/check_events to generate event id code from event:
 *  ./examples/check_events <category>:<umask>[:c=<cmask>]
 *  OR
 *  ./examples/check_events <name>
 *
 * Example:
 *  ./check_events cycle_activity.stalls_mem_any
 *  This will give you the "Codes": 0x145314a3 which needs to be put into a PERF_TYPE_RAW counter.
 */
class CounterDescription
{
public:
    /// CPU
    [[maybe_unused]] static CounterDescription INSTRUCTIONS;
    [[maybe_unused]] static CounterDescription BRANCHES;
    [[maybe_unused]] static CounterDescription BRANCH_MISSES;
    [[maybe_unused]] static CounterDescription BACLEARS_ANY;

    /// Cycles
    [[maybe_unused]] static CounterDescription CYCLES;
    [[maybe_unused]] static CounterDescription BUS_CYCLES;
    [[maybe_unused]] static CounterDescription CYCLE_ACTIVITY_STALLS_MEM_ANY;
    [[maybe_unused]] static CounterDescription CYCLE_ACTIVITY_STALLS_L1D_MISS;
    [[maybe_unused]] static CounterDescription CYCLE_ACTIVITY_STALLS_L2_MISS;
    [[maybe_unused]] static CounterDescription CYCLE_ACTIVITY_STALLS_L3_MISS;
    [[maybe_unused]] static CounterDescription CYCLE_ACTIVITY_CYCLES_L3_MISS;

    /// Cache
    [[maybe_unused]] static CounterDescription L1D_LOAD_MISSES;
    [[maybe_unused]] static CounterDescription L2_RQST_MISS;
    [[maybe_unused]] static CounterDescription LLC_LOAD_MISSES;
    [[maybe_unused]] static CounterDescription CACHE_MISSES;
    [[maybe_unused]] static CounterDescription CACHE_REFERENCES;

    /// Memory
    [[maybe_unused]] static CounterDescription MEM_INST_RETIRED_ALL_LOADS;
    [[maybe_unused]] static CounterDescription MEM_INST_RETIRED_ALL_STORES;
    [[maybe_unused]] static CounterDescription MEM_TRANS_RETIRED_LOAD_LATENCY_GT_32;
    [[maybe_unused]] static CounterDescription MEM_TRANS_RETIRED_LOAD_LATENCY_GT_128;
    [[maybe_unused]] static CounterDescription MEM_LOAD_RETIRED_L1_MISS;
    [[maybe_unused]] static CounterDescription MEM_LOAD_RETIRED_L2_MISS;
    [[maybe_unused]] static CounterDescription MEM_LOAD_RETIRED_L3_MISS;

    /// NUMA
    [[maybe_unused]] static CounterDescription NODE_LOADS;
    [[maybe_unused]] static CounterDescription NODE_LOAD_MISSES;
    [[maybe_unused]] static CounterDescription NODE_STORE_MISSES;
    [[maybe_unused]] static CounterDescription NODE_STORES;
    [[maybe_unused]] static CounterDescription MEM_LOAD_L3_MISS_RETIRED_REMOTE_DRAM;
    [[maybe_unused]] static CounterDescription MEM_LOAD_L3_MISS_RETIRED_LOCAL_DRAM;

    /// Prefetches
    [[maybe_unused]] static CounterDescription SW_PREFETCH_ACCESS_NTA;
    [[maybe_unused]] static CounterDescription SW_PREFETCH_ACCESS_T0;
    [[maybe_unused]] static CounterDescription SW_PREFETCH_ACCESS_T1_T2;
    [[maybe_unused]] static CounterDescription SW_PREFETCH_ACCESS_PREFETCHW;
    [[maybe_unused]] static CounterDescription L2_RQSTS_ALL_PF;
    [[maybe_unused]] static CounterDescription LOAD_HIT_PRE_SW_PF;
    [[maybe_unused]] static CounterDescription L1D_PEND_MISS_FB_FULL;
    [[maybe_unused]] static CounterDescription L2_LINES_OUT_USELESS_HWPF;

    /// DTLB
    [[maybe_unused]] static CounterDescription DTLB_LOAD_MISSES;
    [[maybe_unused]] static CounterDescription STLB_LOAD_MISSES;

    /// Offcore
    [[maybe_unused]] static CounterDescription OFFCORE_REQUESTS_DEMAND_DATA_RD;
    [[maybe_unused]] static CounterDescription OFFCORE_REQUESTS_ALL_DATA_RD;

    /// Transactional Memory
    [[maybe_unused]] static CounterDescription RTM_RETIRED_START;
    [[maybe_unused]] static CounterDescription RTM_RETIRED_COMMIT;
    [[maybe_unused]] static CounterDescription RTM_RETIRED_ABORTED;
    [[maybe_unused]] static CounterDescription RTM_RETIRED_ABORTED_MEM;
    [[maybe_unused]] static CounterDescription RTM_RETIRED_ABORTED_MEM_TYPE;
    [[maybe_unused]] static CounterDescription RTM_RETIRED_ABORTED_UNFRIENDLY;

    CounterDescription(std::string &&name, const std::uint64_t type, const std::uint64_t config) noexcept
        : _name(std::move(name)), _type(type), _event_id(config)
    {
    }

    CounterDescription(std::string &&name, const std::uint64_t type, const std::uint64_t config,
                       const std::uint64_t msr_value) noexcept
        : _name(std::move(name)), _type(type), _event_id(config), _msr_value(msr_value)
    {
    }

    CounterDescription(const CounterDescription &) = default;
    CounterDescription(CounterDescription &&) noexcept = default;

    ~CounterDescription() = default;

    CounterDescription &operator=(CounterDescription &&) noexcept = default;
    CounterDescription &operator=(const CounterDescription &) = default;

    [[nodiscard]] const std::string &name() const noexcept { return _name; }
    [[nodiscard]] std::uint64_t type() const noexcept { return _type; }
    [[nodiscard]] std::uint64_t event_id() const noexcept { return _event_id; }
    [[nodiscard]] std::optional<std::uint64_t> msr_value() const noexcept { return _msr_value; }

private:
    /// Name displayed when printing the counter.
    std::string _name;

    /// Perf type (corresponds to perf_type_id)
    std::uint64_t _type;

    /// Id of the event (e.g., the ID if _type == PERF_RAW_TYPE).
    std::uint64_t _event_id;

    /// Optional MSR value.
    std::optional<std::uint64_t> _msr_value;
};
} // namespace perf