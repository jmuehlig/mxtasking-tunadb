#include "benchmark.h"
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mx/memory/global_heap.h>
#include <nlohmann/json.hpp>

using namespace application::blinktree_benchmark;

Benchmark::Benchmark(benchmark::Cores &&cores, const std::uint16_t iterations, std::string &&fill_workload_file,
                     std::string &&mixed_workload_file, const bool use_performance_counter,
                     const mx::synchronization::isolation_level node_isolation_level,
                     const mx::synchronization::protocol preferred_synchronization_method,
                     const bool print_tree_statistics, const bool check_tree, std::string &&result_file_name,
                     std::string &&statistic_file_name, std::string &&tree_file_name, std::string &&nodes_file_name,
                     const bool profile)
    : _cores(std::move(cores)), _iterations(iterations), _node_isolation_level(node_isolation_level),
      _preferred_synchronization_method(preferred_synchronization_method),
      _print_tree_statistics(print_tree_statistics), _check_tree(check_tree),
      _result_file_name(std::move(result_file_name)), _statistic_file_name(std::move(statistic_file_name)),
      _tree_file_name(std::move(tree_file_name)), _nodes_file_name(std::move(nodes_file_name)), _profile(profile)
{
    if (use_performance_counter)
    {
        /// Basic counter
        this->_chronometer.add({perf::CounterDescription::CYCLES, perf::CounterDescription::INSTRUCTIONS,
                                perf::CounterDescription::CACHE_MISSES, perf::CounterDescription::CACHE_REFERENCES});

        /// Cache / Memory Stalls
        this->_chronometer.add({perf::CounterDescription::CYCLE_ACTIVITY_STALLS_L1D_MISS,
                                perf::CounterDescription::CYCLE_ACTIVITY_STALLS_L2_MISS,
                                perf::CounterDescription::CYCLE_ACTIVITY_STALLS_L3_MISS,
                                perf::CounterDescription::CYCLE_ACTIVITY_STALLS_MEM_ANY});

        /// Prefetches
        this->_chronometer.add({perf::CounterDescription::SW_PREFETCH_ACCESS_T0,
                                perf::CounterDescription::SW_PREFETCH_ACCESS_T1_T2,
                                perf::CounterDescription::SW_PREFETCH_ACCESS_NTA});

        /// Prefetch Metrics
        this->_chronometer.add({perf::CounterDescription::LOAD_HIT_PRE_SW_PF,
                                perf::CounterDescription::L1D_PEND_MISS_FB_FULL,
                                perf::CounterDescription::OFFCORE_REQUESTS_ALL_DATA_RD,
                                perf::CounterDescription::OFFCORE_REQUESTS_DEMAND_DATA_RD});

        if (preferred_synchronization_method == mx::synchronization::protocol::RestrictedTransactionalMemory)
        {
            this->_chronometer.add(
                {perf::CounterDescription::RTM_RETIRED_START, perf::CounterDescription::RTM_RETIRED_COMMIT,
                 perf::CounterDescription::RTM_RETIRED_ABORTED, perf::CounterDescription::RTM_RETIRED_ABORTED_MEM});
        }
    }

    std::cout << "core configuration: \n" << this->_cores.dump(2) << std::endl;

    this->_workload.build(fill_workload_file, mixed_workload_file);
    if (this->_workload.empty(benchmark::phase::FILL) && this->_workload.empty(benchmark::phase::MIXED))
    {
        std::exit(1);
    }

    std::cout << "workload: " << this->_workload << "\n" << std::endl;
}

void Benchmark::start()
{
    /// Start the chronomete, this will also create the worker-local perf counter.
    this->_chronometer.setup(static_cast<std::uint16_t>(static_cast<benchmark::phase>(this->_workload)),
                             this->_current_iteration + 1, this->_cores.current());

    /// Spawn start-perf task on every worker (will run before the start task).
    for (auto worker_id = 0U; worker_id < this->_cores.current().count_cores(); ++worker_id)
    {
        auto *start_perf_task = mx::tasking::runtime::new_task<mx::tasking::LambdaTask>(
            0U, [&chronometer = this->_chronometer](const std::uint16_t worker_id) {
                chronometer.start(worker_id);

                return mx::tasking::TaskResult::make_remove();
            });
        start_perf_task->annotate(std::uint16_t(worker_id));

        mx::tasking::runtime::spawn(*start_perf_task, 0U);
    }

    // Reset request scheduler.
    if (this->_request_scheduler.empty() == false)
    {
        this->_request_scheduler.clear();
    }

    auto *start_task =
        mx::tasking::runtime::new_task<mx::tasking::LambdaTask>(0U, [this](const std::uint16_t worker_id) {
            // Reset tree.
            if (this->_tree == nullptr)
            {
                this->_tree = std::make_unique<db::index::blinktree::BLinkTree<std::uint64_t, std::int64_t>>(
                    this->_node_isolation_level, this->_preferred_synchronization_method);
            }

            // Create one request scheduler per core.
            for (auto target_worker_id = 0U; target_worker_id < this->_cores.current().count_cores();
                 ++target_worker_id)
            {
                auto *request_scheduler = mx::tasking::runtime::new_task<RequestSchedulerTask>(
                    worker_id, target_worker_id, this->_workload, this->_cores.current(), this->_tree.get(), this);
                mx::tasking::runtime::spawn(*request_scheduler, worker_id);
                this->_request_scheduler.push_back(request_scheduler);
            }
            this->_open_requests = this->_request_scheduler.size();

            // Start time measurement.
            this->_chronometer.start();

            return mx::tasking::TaskResult::make_remove();
        });
    start_task->annotate(std::uint16_t(0U));

    mx::tasking::runtime::spawn(*start_task, 0U);
}

const mx::util::core_set &Benchmark::core_set()
{
    if (this->_current_iteration == std::numeric_limits<std::uint16_t>::max())
    {
        // This is the very first time we start the benchmark.
        this->_current_iteration = 0U;
        return this->_cores.next();
    }

    // Switch from fill to mixed phase.
    if (this->_workload == benchmark::phase::FILL && this->_workload.empty(benchmark::phase::MIXED) == false)
    {
        this->_workload.reset(benchmark::phase::MIXED);
        return this->_cores.current();
    }
    this->_workload.reset(benchmark::phase::FILL);

    // Run the next iteration.
    if (++this->_current_iteration < this->_iterations)
    {
        return this->_cores.current();
    }
    this->_current_iteration = 0U;

    // At this point, all phases and all iterations for the current core configuration
    // are done. Increase the cores.
    return this->_cores.next();
}

void Benchmark::requests_finished()
{
    const auto open_requests = --this->_open_requests;

    if (open_requests == 0U) // All request schedulers are done.
    {
        // Stop and print time (and performance counter).
        const auto result = this->_chronometer.stop(this->_workload.size());
        mx::tasking::runtime::stop();
        std::cout << result << std::endl;

        // Dump results to file.
        if (this->_result_file_name.empty() == false)
        {
            std::ofstream result_file_stream(this->_result_file_name, std::ofstream::app);
            result_file_stream << result.to_json().dump() << std::endl;
        }

        // Dump statistics to file.
        if constexpr (mx::tasking::config::is_use_task_counter())
        {
            if (this->_statistic_file_name.empty() == false)
            {
                std::ofstream statistic_file_stream(this->_statistic_file_name, std::ofstream::app);
                auto result_json = nlohmann::json{};
                result_json["iteration"] = result.iteration();
                result_json["cores"] = result.core_count();
                result_json["phase"] = result.phase();
                for (auto worker_id = 0U; worker_id < this->_cores.current().count_cores(); ++worker_id)
                {
                    const auto worker_id_string = std::to_string(worker_id);
                    result_json["dispatched"][worker_id_string] =
                        result.task_counter().at(mx::tasking::profiling::TaskCounter::Counter::Dispatched)[worker_id] /
                        double(result.operation_count());
                    result_json["dispatched-locally"][worker_id_string] =
                        result.task_counter().at(
                            mx::tasking::profiling::TaskCounter::Counter::DispatchedLocally)[worker_id] /
                        double(result.operation_count());
                    result_json["dispatched-remotely"][worker_id_string] =
                        result.task_counter().at(
                            mx::tasking::profiling::TaskCounter::Counter::DispatchedRemotely)[worker_id] /
                        double(result.operation_count());
                    result_json["executed"][worker_id_string] =
                        result.task_counter().at(mx::tasking::profiling::TaskCounter::Counter::Executed)[worker_id] /
                        double(result.operation_count());
                    result_json["executed-reader"][worker_id_string] =
                        result.task_counter().at(
                            mx::tasking::profiling::TaskCounter::Counter::ExecutedReader)[worker_id] /
                        double(result.operation_count());
                    result_json["executed-writer"][worker_id_string] =
                        result.task_counter().at(
                            mx::tasking::profiling::TaskCounter::Counter::ExecutedWriter)[worker_id] /
                        double(result.operation_count());
                    result_json["filled-buffer"][worker_id_string] =
                        result.task_counter().at(
                            mx::tasking::profiling::TaskCounter::Counter::FilledBuffer)[worker_id] /
                        double(result.operation_count());
                }

                statistic_file_stream << result_json.dump(2) << std::endl;
            }
        }

        // Check and print the tree.
        if (this->_check_tree)
        {
            this->_tree->check();
        }

        if (this->_print_tree_statistics)
        {
            this->_tree->print_statistics();
        }

        const auto is_last_phase =
            this->_workload == benchmark::phase::MIXED || this->_workload.empty(benchmark::phase::MIXED);

        // Dump the tree.
        if (this->_tree_file_name.empty() == false && is_last_phase)
        {
            auto out_file = std::ofstream{this->_tree_file_name};
            out_file << static_cast<nlohmann::json>(*(this->_tree)).dump() << std::endl;
        }

        // Write node addresses.
        if (this->_nodes_file_name.empty() == false && is_last_phase)
        {
            auto out_file = std::ofstream{this->_nodes_file_name};
            out_file << this->_tree->node_addresses().dump() << std::endl;
        }

        // Delete the tree to free the hole memory.
        if (is_last_phase)
        {
            this->_tree.reset(nullptr);
        }
    }
}

std::string Benchmark::profile_file_name() const
{
    return "profiling-" + std::to_string(this->_cores.current().count_cores()) + "-cores" + "-phase-" +
           std::to_string(static_cast<std::uint16_t>(static_cast<benchmark::phase>(this->_workload))) + "-iteration-" +
           std::to_string(this->_current_iteration) + ".json";
}