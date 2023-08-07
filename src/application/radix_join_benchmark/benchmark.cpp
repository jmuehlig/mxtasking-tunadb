#include "benchmark.h"
#include "config.h"
#include "task.h"
#include <cstdlib>
#include <fmt/core.h>
#include <iostream>
#include <memory>
#include <mx/memory/global_heap.h>
#include <nlohmann/json.hpp>

using namespace application::rjbenchmark;

Benchmark::Benchmark(benchmark::Cores &&cores, const std::uint16_t iterations, std::string &&build_side_file,
                     std::string &&probe_side_file, bool use_performance_counter, std::string &&result_file_name)
    : _cores(std::move(cores)), _iterations(iterations), _result_file_name(std::move(result_file_name))
{
    if (use_performance_counter)
    {
        this->_chronometer.add({perf::CounterDescription::CYCLES, perf::CounterDescription::INSTRUCTIONS,
                                perf::CounterDescription::CYCLE_ACTIVITY_STALLS_MEM_ANY,
                                perf::CounterDescription::SW_PREFETCH_ACCESS_NTA,
                                perf::CounterDescription::L1D_PEND_MISS_FB_FULL});
    }

    /// Truncate the output file.
    if (this->_result_file_name.empty() == false)
    {
        auto result_file_stream = std::ofstream{this->_result_file_name, std::ofstream::trunc};
        if (result_file_stream.is_open())
        {
            result_file_stream.close();
        }
    }

    std::cout << "core configuration: \n" << this->_cores.dump(2) << std::endl;

    this->_build_relation = Benchmark::read_tuples(std::move(build_side_file));
    this->_probe_relation = Benchmark::read_tuples(std::move(probe_side_file));

    for (auto i = 0U; i < std::get<0>(this->_build_relation); ++i)
    {
        this->_build_relation_key_sum += std::get<1>(this->_build_relation)[i].key;
    }

    for (auto i = 0U; i < std::get<0>(this->_probe_relation); ++i)
    {
        this->_probe_relation_key_sum += std::get<1>(this->_probe_relation)[i].key;
    }

    std::cout << "workload: build " << std::get<0>(this->_build_relation) << " tuples / probe "
              << std::get<0>(this->_probe_relation) << " tuples\n"
              << std::endl;
}

void Benchmark::start()
{
    this->_build_local_partitions.clear();
    this->_probe_local_partitions.clear();
    this->_partition_squads.clear();

    /// Start the chronomete, this will also create the worker-local perf counter.
    this->_chronometer.setup(0, this->_current_iteration + 1, this->_cores.current());

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

    auto *start_task = mx::tasking::runtime::new_task<mx::tasking::LambdaTask>(0U, [this](
                                                                                       const std::uint16_t worker_id) {
        const auto count_worker = mx::tasking::runtime::workers();

        /// Initialize finish tasks.
        this->_pending_worker_counter.store(count_worker);
        auto finish_tasks = std::vector<SynchronizeWorkerTask *>{};
        finish_tasks.reserve(count_worker);
        for (auto target_worker_id = 0U; target_worker_id < count_worker; ++target_worker_id)
        {
            auto *task = mx::tasking::runtime::new_task<SynchronizeWorkerTask>(worker_id, this->_pending_worker_counter,
                                                                               std::bind(&Benchmark::finished, this));
            task->annotate(std::uint16_t(target_worker_id));
            finish_tasks.emplace_back(task);
        }

        const auto count_partitions = std::uint64_t(std::pow(2U, config::radix_bits()));

        /// Partition squads.
        const auto hash_table_size = std::get<0>(this->_build_relation) / count_partitions;
        this->_partition_squads.reserve(count_partitions);
        for (auto i = 0U; i < count_partitions; ++i)
        {
            this->_partition_squads.emplace_back(mx::tasking::runtime::new_squad(i % count_worker));
        }

        /// Build side.
        const auto build_worker_allocation =
            Benchmark::calculate_worker_relation_boundaries(std::get<0>(this->_build_relation), count_worker);
        this->_build_local_partitions.reserve(count_worker);
        for (auto target_worker_id = 0U; target_worker_id < count_worker; ++target_worker_id)
        {
            const auto numa_node_id = mx::tasking::runtime::numa_node_id(target_worker_id);
            this->_build_local_partitions.emplace_back(std::get<1>(build_worker_allocation[target_worker_id]),
                                                       this->_partition_squads, numa_node_id);
        }

        /// Probe side.
        const auto probe_worker_allocation =
            Benchmark::calculate_worker_relation_boundaries(std::get<0>(this->_probe_relation), count_worker);
        this->_probe_local_partitions.reserve(count_worker);
        for (auto target_worker_id = 0U; target_worker_id < count_worker; ++target_worker_id)
        {
            const auto numa_node_id = mx::tasking::runtime::numa_node_id(target_worker_id);
            this->_probe_local_partitions.emplace_back(std::get<1>(probe_worker_allocation[target_worker_id]),
                                                       this->_partition_squads, numa_node_id);
        }

        // Start time measurement.
        this->_chronometer.start();

        for (auto target_worker_id = 0U; target_worker_id < count_worker; ++target_worker_id)
        {
            const auto [probe_start_index, probe_count_tuples] = probe_worker_allocation[target_worker_id];
            const auto [build_start_index, build_count_tuples] = build_worker_allocation[target_worker_id];
            /// ###### Small Tasks ######
            /// Partition the probe side
            //                auto *probe_task =
            //                    mx::tasking::runtime::new_task<GenerateScanTask<ScanAndPartitionTask<SumKeysTask>,
            //                    SumKeysTask>>(
            //                        worker_id, probe_start_index, probe_count_tuples,
            //                        std::get<1>(this->_probe_relation),
            //                        this->_probe_local_partitions[target_worker_id], finish_tasks[target_worker_id]);
            //                probe_task->annotate(std::uint16_t(target_worker_id));

            /// Partition the build side
            //                auto *build_task =
            //                    mx::tasking::runtime::new_task<GenerateScanTask<ScanAndPartitionTask<SumKeysTask>,
            //                    SumKeysTask>>(
            //                        worker_id, build_start_index, build_count_tuples,
            //                        std::get<1>(this->_build_relation),
            //                        this->_build_local_partitions[target_worker_id], probe_task);
            //                build_task->annotate(std::uint16_t(target_worker_id));

            /// Start with build side
            //                mx::tasking::runtime::spawn(*build_task, worker_id);

            /// ###### Large Tasks ######
            auto *probe_task = mx::tasking::runtime::new_task<ScanAndPartitionAllTask<SumKeysTask>>(
                worker_id, std::get<1>(this->_probe_relation), probe_start_index,
                probe_start_index + probe_count_tuples, this->_probe_local_partitions[target_worker_id],
                finish_tasks[target_worker_id]);
            probe_task->annotate(std::uint16_t(target_worker_id));

            auto *build_task = mx::tasking::runtime::new_task<ScanAndPartitionAllTask<SumKeysTask>>(
                worker_id, std::get<1>(this->_build_relation), build_start_index,
                build_start_index + build_count_tuples, this->_build_local_partitions[target_worker_id], probe_task);
            build_task->annotate(std::uint16_t(target_worker_id));
            mx::tasking::runtime::spawn(*build_task, worker_id);
        }

        /**
         * TODO:
         *  - Calculate borders (1/x) per worker
         *  - Start a task for every worker that sends tiles to the worker to partition the Build side
         *      => Start Partition(Rel = Build)
         *          for tile in Rel:
         *              spawn partition(tile) -> Map to partition,
         *          spawn finalize(partitions) -> writes rest of partitions
         *  - Start a task for every worker that sends tiles to the worker to partition the Probe side
         *      => Start Partition(Rel = Probe)
         *          spawn finalizes also spawns a build HT task
         *              => spawns the partitions
         *  - After all tables are build, spawn probes
         *  - After all probes are done, collect result
         *  - Call benmchmark finished()
         */

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

void Benchmark::finished()
{
    const auto result = this->_chronometer.stop(std::get<0>(this->_probe_relation));
    mx::tasking::runtime::stop();
    std::cout << result << std::endl;

    if (this->_result_file_name.empty() == false)
    {
        auto result_file_stream = std::ofstream{this->_result_file_name, std::ofstream::app};
        result_file_stream << result.to_json().dump() << std::endl;
    }

    /// Verify partitions.
    std::int64_t sum = 0;
    std::uint64_t count_tuples = 0ULL;
    for (auto partition_squad : this->_partition_squads)
    {
        auto *squad = partition_squad.get<mx::tasking::TaskSquad>();
        squad->flush();
        mx::tasking::TaskInterface *task;
        while ((task = squad->pop_front()) != nullptr)
        {
            auto *sum_task = reinterpret_cast<SumKeysTask *>(task);
            sum_task->execute(0U);
            sum += sum_task->sum();
            count_tuples += sum_task->count_tuples();
        }
    }

    if (sum != std::int64_t(this->_build_relation_key_sum + this->_probe_relation_key_sum))
    {
        std::cout << "Verification failed. Expected sum = "
                  << (this->_build_relation_key_sum + this->_probe_relation_key_sum) << " | Is sum = " << sum
                  << std::endl;
    }

    if (count_tuples != (std::get<0>(this->_build_relation) + std::get<0>(this->_probe_relation)))
    {
        std::cout << "Verification failed. Expected count = "
                  << (std::get<0>(this->_build_relation) + std::get<0>(this->_probe_relation))
                  << " | Is count = " << count_tuples << std::endl;
    }

    //    const auto open_requests = --this->_open_requests;
    //
    //    if (open_requests == 0U) // All request schedulers are done.
    //    {
    //        // Stop and print time (and performance counter).
    //        const auto result = this->_chronometer.stop(this->_workload.size());
    //        mx::tasking::runtime::stop();
    //        std::cout << result << std::endl;
    //
    //        // Dump results to file.
    //        if (this->_result_file_name.empty() == false)
    //        {
    //            std::ofstream result_file_stream(this->_result_file_name, std::ofstream::app);
    //            result_file_stream << result.to_json().dump() << std::endl;
    //        }
    //
    //        // Dump statistics to file.
    //        if constexpr (mx::tasking::config::is_use_task_counter())
    //        {
    //            if (this->_statistic_file_name.empty() == false)
    //            {
    //                std::ofstream statistic_file_stream(this->_statistic_file_name, std::ofstream::app);
    //                auto result_json = nlohmann::json{};
    //                result_json["iteration"] = result.iteration();
    //                result_json["cores"] = result.core_count();
    //                result_json["phase"] = result.phase();
    //                for (auto worker_id = 0U; worker_id < this->_cores.current().count_cores(); ++worker_id)
    //                {
    //                    const auto worker_id_string = std::to_string(worker_id);
    //                    result_json["dispatched"][worker_id_string] =
    //                        result.task_counter().at(mx::tasking::profiling::TaskCounter::Counter::Dispatched)[worker_id]
    //                        / double(result.operation_count());
    //                    result_json["dispatched-locally"][worker_id_string] =
    //                        result.task_counter().at(
    //                            mx::tasking::profiling::TaskCounter::Counter::DispatchedLocally)[worker_id] /
    //                        double(result.operation_count());
    //                    result_json["dispatched-remotely"][worker_id_string] =
    //                        result.task_counter().at(
    //                            mx::tasking::profiling::TaskCounter::Counter::DispatchedRemotely)[worker_id] /
    //                        double(result.operation_count());
    //                    result_json["executed"][worker_id_string] =
    //                        result.task_counter().at(mx::tasking::profiling::TaskCounter::Counter::Executed)[worker_id]
    //                        / double(result.operation_count());
    //                    result_json["executed-reader"][worker_id_string] =
    //                        result.task_counter().at(
    //                            mx::tasking::profiling::TaskCounter::Counter::ExecutedReader)[worker_id] /
    //                        double(result.operation_count());
    //                    result_json["executed-writer"][worker_id_string] =
    //                        result.task_counter().at(
    //                            mx::tasking::profiling::TaskCounter::Counter::ExecutedWriter)[worker_id] /
    //                        double(result.operation_count());
    //                    result_json["filled-buffer"][worker_id_string] =
    //                        result.task_counter().at(
    //                            mx::tasking::profiling::TaskCounter::Counter::FilledBuffer)[worker_id] /
    //                        double(result.operation_count());
    //                }
    //
    //                statistic_file_stream << result_json.dump(2) << std::endl;
    //            }
    //        }
    //
    //        // Check and print the tree.
    //        if (this->_check_tree)
    //        {
    //            this->_tree->check();
    //        }
    //
    //        if (this->_print_tree_statistics)
    //        {
    //            this->_tree->print_statistics();
    //        }
    //
    //        const auto is_last_phase =
    //            this->_workload == benchmark::phase::MIXED || this->_workload.empty(benchmark::phase::MIXED);
    //
    //        // Dump the tree.
    //        if (this->_tree_file_name.empty() == false && is_last_phase)
    //        {
    //            std::ofstream tree_file_stream(this->_tree_file_name);
    //            tree_file_stream << static_cast<nlohmann::json>(*(this->_tree)).dump() << std::endl;
    //        }
    //
    //        // Delete the tree to free the hole memory.
    //        if (is_last_phase)
    //        {
    //            this->_tree.reset(nullptr);
    //        }
    //    }
}

std::pair<std::uint64_t, Tuple *> Benchmark::read_tuples(std::string &&file_name)
{
    auto count = 0ULL;
    auto file = std::ifstream{file_name};
    if (file.is_open())
    {
        count = std::count(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>(), '\n');

        auto *data = reinterpret_cast<Tuple *>(std::aligned_alloc(4096U, sizeof(Tuple) * count));

        file.seekg(0, std::ios::beg);
        std::string line;
        std::int64_t key;
        std::int64_t payload;
        char delimiter;

        auto cursor = 0ULL;
        auto key_sum = 0ULL;
        while (std::getline(file, line))
        {
            auto stream = std::stringstream{line};
            stream >> key >> delimiter >> payload;
            data[cursor++] = Tuple{key, payload};

            key_sum += key;
        }

        return std::make_pair(count, data);
    }

    return std::make_pair(0U, nullptr);
}

std::vector<std::pair<std::uint64_t, std::uint64_t>> Benchmark::calculate_worker_relation_boundaries(
    const std::uint64_t tuples, const std::uint64_t workers)
{
    const auto count_tiles = std::uint64_t(std::ceil(tuples / config::tuples_per_tile()));
    const auto tiles_per_worker = std::uint64_t(std::floor(count_tiles / workers));

    auto worker_indices = std::vector<std::pair<std::uint64_t, std::uint64_t>>{};
    worker_indices.reserve(workers);

    for (auto worker_id = 0U; worker_id < workers; ++worker_id)
    {
        const auto index = worker_id * (tiles_per_worker * config::tuples_per_tile());
        auto count_tuples = tiles_per_worker * config::tuples_per_tile();

        if (worker_id == (workers - 1U))
        {
            count_tuples = tuples - ((workers - 1U) * count_tuples);
        }

        worker_indices.emplace_back(index, count_tuples);
    }

    return worker_indices;
}