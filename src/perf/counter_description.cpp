#include "counter_description.h"
#include <linux/perf_event.h>

using namespace perf;

[[maybe_unused]] CounterDescription CounterDescription::INSTRUCTIONS =
    CounterDescription{"instructions", PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS};

[[maybe_unused]] CounterDescription CounterDescription::CYCLES =
    CounterDescription{"cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES};

[[maybe_unused]] CounterDescription CounterDescription::L1D_LOAD_MISSES = CounterDescription{
    "L1-dcache-load-misses", PERF_TYPE_HW_CACHE,
    PERF_COUNT_HW_CACHE_L1D | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)};

[[maybe_unused]] CounterDescription CounterDescription::LLC_LOAD_MISSES = CounterDescription{
    "LLC-load-misses", PERF_TYPE_HW_CACHE,
    PERF_COUNT_HW_CACHE_LL | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)};

[[maybe_unused]] CounterDescription CounterDescription::CACHE_MISSES =
    CounterDescription{"cache-misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES};

[[maybe_unused]] CounterDescription CounterDescription::CACHE_REFERENCES =
    CounterDescription{"cache-references", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_REFERENCES};

[[maybe_unused]] CounterDescription CounterDescription::BRANCHES =
    CounterDescription{"branches", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_INSTRUCTIONS};

[[maybe_unused]] CounterDescription CounterDescription::BRANCH_MISSES =
    CounterDescription{"branch-misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES};

[[maybe_unused]] CounterDescription CounterDescription::BUS_CYCLES =
    CounterDescription{"bus-cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BUS_CYCLES};

[[maybe_unused]] CounterDescription CounterDescription::DTLB_LOAD_MISSES = CounterDescription{
    "dTLB-load-misses", PERF_TYPE_HW_CACHE,
    PERF_COUNT_HW_CACHE_DTLB | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_INST_RETIRED.STLB_MISS_LOADS"
 * Retired load instructions that miss the STLB Supports address when
precise (Precise event)
 */
[[maybe_unused]] CounterDescription CounterDescription::STLB_LOAD_MISSES =
    CounterDescription{"mem_inst_retired.stlb_miss_loads", PERF_TYPE_RAW, 0x5311d0};

/**
 * The number of memory reads served by the remote NUMA node.
 */
[[maybe_unused]] CounterDescription CounterDescription::NODE_LOAD_MISSES = CounterDescription{
    "node-load-misses", PERF_TYPE_HW_CACHE,
    PERF_COUNT_HW_CACHE_NODE | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)};

/**
 * The number of memory reads served by the local NUMA node.
 */
[[maybe_unused]] CounterDescription CounterDescription::NODE_LOADS = CounterDescription{
    "node-loads", PERF_TYPE_HW_CACHE,
    PERF_COUNT_HW_CACHE_NODE | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16)};

/**
 * The number of memory wrotes served by the remote NUMA node.
 */
[[maybe_unused]] CounterDescription CounterDescription::NODE_STORE_MISSES = CounterDescription{
    "node-store-misses", PERF_TYPE_HW_CACHE,
    PERF_COUNT_HW_CACHE_NODE | (PERF_COUNT_HW_CACHE_OP_WRITE << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)};

/**
 * The number of memory writes served by the local NUMA node.
 */
[[maybe_unused]] CounterDescription CounterDescription::NODE_STORES = CounterDescription{
    "node-stores", PERF_TYPE_HW_CACHE,
    PERF_COUNT_HW_CACHE_NODE | (PERF_COUNT_HW_CACHE_OP_WRITE << 8) | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16)};

/**
 * Micro architecture "Cascade Lake"
 * Counter "CYCLE_ACTIVITY.STALLS_MEM_ANY"
 * EventSel=A3H,UMask=14H, CMask=20
 * Execution stalls while memory subsystem has an outstanding load.
 */
[[maybe_unused]] CounterDescription CounterDescription::CYCLE_ACTIVITY_STALLS_MEM_ANY =
    CounterDescription{"cycle_activity.stalls_mem_any", PERF_TYPE_RAW, 0x145314a3};

/**
 * Micro architecture "Cascade Lake"
 * Counter "CYCLE_ACTIVITY.STALLS_L1_MISS"
 * Execution stalls while L1D cache miss demand load is outstanding.
 */
[[maybe_unused]] CounterDescription CounterDescription::CYCLE_ACTIVITY_STALLS_L1D_MISS =
    CounterDescription{"cycle_activity.stalls_l1d_miss", PERF_TYPE_RAW, 0xc530ca3};

/**
 * Micro architecture "Cascade Lake"
 * Counter "CYCLE_ACTIVITY.STALLS_L2_MISS"
 * Execution stalls while L2 cache miss demand load is outstanding.
 */
[[maybe_unused]] CounterDescription CounterDescription::CYCLE_ACTIVITY_STALLS_L2_MISS =
    CounterDescription{"cycle_activity.stalls_l2_miss", PERF_TYPE_RAW, 0x55305a3};

/**
 * Micro architecture "Cascade Lake"
 * Counter "CYCLE_ACTIVITY.STALLS_L3_MISS"
 * Execution stalls while L3 cache miss demand load is outstanding.
 */
[[maybe_unused]] CounterDescription CounterDescription::CYCLE_ACTIVITY_STALLS_L3_MISS =
    CounterDescription{"cycle_activity.stalls_l3_miss", PERF_TYPE_RAW, 0x65306a3};

/**
 * Micro architecture "Cascade Lake"
 * Counter "CYCLE_ACTIVITY.CYCLES_L3_MISS"
 * EventSel=A3H,UMask=14H, CMask=20
 * Cycles while L3 cache miss demand load is outstanding.
 */
[[maybe_unused]] CounterDescription CounterDescription::CYCLE_ACTIVITY_CYCLES_L3_MISS =
    CounterDescription{"cycle_activity.cycles_l3_miss", PERF_TYPE_RAW, 0x25302a3};

/**
 * Micro architecture "Cascade Lake"
 * Counter "SW_PREFETCH_ACCESS.NTA"
 * EventSel=32H,UMask=01H
 * Number of PREFETCHNTA instructions executed.
 */
[[maybe_unused]] CounterDescription CounterDescription::SW_PREFETCH_ACCESS_NTA =
    CounterDescription{"sw_prefetch_access.nta", PERF_TYPE_RAW, 0x530132};

/**
 * Micro architecture "Cascade Lake"
 * Counter "SW_PREFETCH_ACCESS.T0"
 * EventSel=32H,UMask=02H
 * Number of PREFETCHT0 instructions executed.
 */
[[maybe_unused]] CounterDescription CounterDescription::SW_PREFETCH_ACCESS_T0 =
    CounterDescription{"sw_prefetch_access.t0", PERF_TYPE_RAW, 0x530232};

/**
 * Micro architecture "Cascade Lake"
 * Counter "SW_PREFETCH_ACCESS.T1_T2"
 * EventSel=32H,UMask=04H
 * Number of PREFETCHT1 or PREFETCHT2 instructions executed.
 */
[[maybe_unused]] CounterDescription CounterDescription::SW_PREFETCH_ACCESS_T1_T2 =
    CounterDescription{"sw_prefetch_access.t1t2", PERF_TYPE_RAW, 0x530432};

/**
 * Micro architecture "Cascade Lake"
 * Counter "SW_PREFETCH_ACCESS.PREFETCHW"
 * EventSel=32H,UMask=08H
 * Number of PREFETCHW instructions executed.
 */
[[maybe_unused]] CounterDescription CounterDescription::SW_PREFETCH_ACCESS_PREFETCHW =
    CounterDescription{"sw_prefetch_access.prefetchw", PERF_TYPE_RAW, 0x530832};

/**
 * Micro architecture "Cascade Lake"
 * Counter "L2_RQSTS.ALL_PF"
 * Requests from the L1/L2/L3 hardware prefetchers or Load software prefetches.
 */
[[maybe_unused]] CounterDescription CounterDescription::L2_RQSTS_ALL_PF =
    CounterDescription{"l2_rqsts.all_pf", PERF_TYPE_RAW, 0x53f824};

/**
 * Micro architecture "Cascade Lake"
 * Counter "LOAD_HIT_PRE.SW_PF"
 * Demand load dispatches that hit L1D fill buffer (FB) allocated for software prefetch.
 * Note: If the number is high, the prefetch is too late (the requested cl is not loaded but in the FB).
 */
[[maybe_unused]] CounterDescription CounterDescription::LOAD_HIT_PRE_SW_PF =
    CounterDescription{"load_hit_pre.sw_pf", PERF_TYPE_RAW, 0x53014c};

/**
 * Micro architecture "Cascade Lake"
 * Counter "L1D_PEND_MISS.FB_FULL"
 * Number of times a request needed a FB entry but there was no entry
 * available for it. That is the FB unavailability was dominant reason
 * for blocking the request. A request includes cacheable/uncacheable
 * demands that is load, store or SW prefetch
 */
[[maybe_unused]] CounterDescription CounterDescription::L1D_PEND_MISS_FB_FULL =
    CounterDescription{"l1d_pend_miss.fb_full", PERF_TYPE_RAW, 0x530248};

/**
 * Micro architecture "Cascade Lake"
 * Counter "L2_LINES_OUT.USELESS_HWPF"
 * Counts the number of lines that have been hardware prefetched but not
 * used and now evicted by L2 cache.
 */
[[maybe_unused]] CounterDescription CounterDescription::L2_LINES_OUT_USELESS_HWPF =
    CounterDescription{"l2_lines_out.useless_hwpf", PERF_TYPE_RAW, 0x5304f2};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_LOAD_L3_MISS_RETIRED.REMOTE_DRAM"
 * Retired load instructions which data sources missed L3 but serviced
 * from remote dram.
 */
[[maybe_unused]] CounterDescription CounterDescription::MEM_LOAD_L3_MISS_RETIRED_REMOTE_DRAM =
    CounterDescription{"mem_load_l3_miss_retired.remote_dram", PERF_TYPE_RAW, 0x5302d3};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_LOAD_L3_MISS_RETIRED.LOCAL_DRAM"
 * Retired load instructions which data sources missed L3 but serviced
 * from local dram.
 */
[[maybe_unused]] CounterDescription CounterDescription::MEM_LOAD_L3_MISS_RETIRED_LOCAL_DRAM =
    CounterDescription{"mem_load_l3_miss_retired.local_dram", PERF_TYPE_RAW, 0x5301d3};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_LOAD_INST_RETIRED.ALL_LOADS"
 * All retired load instructions Supports address when precise (Precise event).
 */
[[maybe_unused]] CounterDescription CounterDescription::MEM_INST_RETIRED_ALL_LOADS =
    CounterDescription{"mem_inst_retired.all_loads", PERF_TYPE_RAW, 0x5381d0};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_LOAD_INST_RETIRED.ALL_STORES"
 * All retired store instructions Supports address when precise (Precise event).
 */
[[maybe_unused]] CounterDescription CounterDescription::MEM_INST_RETIRED_ALL_STORES =
    CounterDescription{"mem_inst_retired.all_stores", PERF_TYPE_RAW, 0x5382d0};

/**
 * Micro architecture "Cascade Lake"
 * Counter "BACLEARS.ANY"
 * Counts the total number when the front end is resteered, mainly when
 * the BPU cannot provide a correct prediction and this is corrected by
 * other branch handling mechanisms at the front end.
 */
[[maybe_unused]] CounterDescription CounterDescription::BACLEARS_ANY =
    CounterDescription{"baclears.any", PERF_TYPE_RAW, 0x5301e6};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_TRANS_RETIRED.LOAD_LATENCY_GT_32"
 * Counts randomly selected loads when the latency from first dispatch to
 * completion is greater than 32 cycles Supports address when precise
 * (Must be precise)
 */
[[maybe_unused]] CounterDescription CounterDescription::MEM_TRANS_RETIRED_LOAD_LATENCY_GT_32 =
    CounterDescription{"mem_trans_retired.load_latency_gt_32", PERF_TYPE_RAW, 0x5301cd, 0x20};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_TRANS_RETIRED.LOAD_LATENCY_GT_128"
 * Counts randomly selected loads when the latency from first dispatch to
 * completion is greater than 128 cycles Supports address when precise
 * (Must be precise)
 */
[[maybe_unused]] CounterDescription CounterDescription::MEM_TRANS_RETIRED_LOAD_LATENCY_GT_128 =
    CounterDescription{"mem_trans_retired.load_latency_gt_128", PERF_TYPE_RAW, 0x5301cd, 0x80};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_LOAD.RETIRED_L1_MISS"
 * Retired load instructions missed L1 cache as data sources.
 * Supports address when precise (Precise event)
 */
[[maybe_unused]] CounterDescription CounterDescription::MEM_LOAD_RETIRED_L1_MISS =
    CounterDescription{"mem_load_retired.l1_miss", PERF_TYPE_RAW, 0x5308d1};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_LOAD.RETIRED_L2_MISS"
 * Retired load instructions missed L2 cache as data sources.
 * Supports address when precise (Precise event)
 */
[[maybe_unused]] CounterDescription CounterDescription::MEM_LOAD_RETIRED_L2_MISS =
    CounterDescription{"mem_load_retired.l2_miss", PERF_TYPE_RAW, 0x5310d1};

/**
 * Micro architecture "Cascade Lake"
 * Counter "MEM_LOAD.RETIRED_L3_MISS"
 * Retired load instructions missed L3 cache as data sources.
 * Supports address when precise (Precise event)
 */
[[maybe_unused]] CounterDescription CounterDescription::MEM_LOAD_RETIRED_L3_MISS =
    CounterDescription{"mem_load_retired.l3_miss", PERF_TYPE_RAW, 0x5320d1};

/**
 * Micro architecture "Cascade Lake"
 * Counter "L2_RQSTS.MISS"
 * All requests that miss L2 cache
 */
[[maybe_unused]] CounterDescription CounterDescription::L2_RQST_MISS =
    CounterDescription{"l2_rqsts.miss", PERF_TYPE_RAW, 0x533f24};

/**
 * Micro architecture "Cascade Lake"
 * Counter "OFFCORE_REQUESTS.ALL_DATA_RD"
 * Counts the demand and prefetch data reads. All Core Data Reads include cacheable
 * 'Demands' and L2 prefetchers (not L3 prefetchers). Counting also covers
 * reads due to page walks resulted from any request type.
 */
[[maybe_unused]] CounterDescription CounterDescription::OFFCORE_REQUESTS_ALL_DATA_RD =
    CounterDescription{"offcore_requests.all_data_rd", PERF_TYPE_RAW, 0x5308b0};

/**
 * Micro architecture "Cascade Lake"
 * Counter "OFFCORE_REQUESTS.DEMAND_DATA_RD"
 * Counts the Demand Data Read requests sent to uncore. Use it in conjunction
 * with OFFCORE_REQUESTS_OUTSTANDING to determine average latency in the uncore.
 */
[[maybe_unused]] CounterDescription CounterDescription::OFFCORE_REQUESTS_DEMAND_DATA_RD =
    CounterDescription{"offcore_requests.demand_data_rd", PERF_TYPE_RAW, 0x5301b0};

/**
 * Micro architecture "Cascade Lake"
 * Counter "rtm_retired.start"
 * Number of times an RTM execution started
 */
[[maybe_unused]] CounterDescription CounterDescription::RTM_RETIRED_START =
    CounterDescription{"rtm_retired.start", PERF_TYPE_RAW, 0x5301c9};

/**
 * Micro architecture "Cascade Lake"
 * Counter "rtm_retired.commit"
 * Number of times an RTM execution successfully committed
 */
[[maybe_unused]] CounterDescription CounterDescription::RTM_RETIRED_COMMIT =
    CounterDescription{"rtm_retired.commit", PERF_TYPE_RAW, 0x5302c9};

/**
 * Micro architecture "Cascade Lake"
 * Counter "rtm_retired.aborted"
 * Number of times an RTM execution aborted due to any reasons
 * (multiple categories may count as one) (Precise event)
 */
[[maybe_unused]] CounterDescription CounterDescription::RTM_RETIRED_ABORTED =
    CounterDescription{"rtm_retired.aborted", PERF_TYPE_RAW, 0x5304c9};

/**
 * Micro architecture "Cascade Lake"
 * Counter "rtm_retired.aborted_mem"
 * Number of times an RTM execution aborted due to various
 * memory events (e.g. read/write capacity and conflicts)
 */
[[maybe_unused]] CounterDescription CounterDescription::RTM_RETIRED_ABORTED_MEM =
    CounterDescription{"rtm_retired.aborted_mem", PERF_TYPE_RAW, 0x5308c9};

/**
 * Micro architecture "Cascade Lake"
 * Counter "rtm_retired.aborted_memtype"
 * Number of times an RTM execution aborted due to incompatible memory type
 */
[[maybe_unused]] CounterDescription CounterDescription::RTM_RETIRED_ABORTED_MEM_TYPE =
    CounterDescription{"rtm_retired.aborted_memtype", PERF_TYPE_RAW, 0x5340c9};

/**
 * Micro architecture "Cascade Lake"
 * Counter "rtm_retired.aborted_unfriendly"
 * Number of times an RTM execution aborted due to
 * HLE-unfriendly instructions
 */
[[maybe_unused]] CounterDescription CounterDescription::RTM_RETIRED_ABORTED_UNFRIENDLY =
    CounterDescription{"rtm_retired.aborted_unfriendly", PERF_TYPE_RAW, 0x5320c9};