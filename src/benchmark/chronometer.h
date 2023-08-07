#pragma once

#include "perf/counter.h"
#include "phase.h"
#include <chrono>
#include <mx/tasking/config.h>
#include <mx/tasking/profiling/task_counter.h>
#include <mx/tasking/runtime.h>
#include <mx/util/core_set.h>
#include <nlohmann/json.hpp>
#include <numeric>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>

namespace benchmark {
/**
 * The InterimResult is part of the chronometer, which in turn holds
 * all results during a benchmark.
 */
template <typename P> class InterimResult
{
    friend std::ostream &operator<<(std::ostream &stream, const InterimResult &result)
    {
        stream << result.core_count() << "\t" << result.iteration() << "\t" << result.phase() << "\t"
               << result.time().count() << " ms"
               << "\t" << result.throughput() << " op/s";

        for (const auto &[name, value] : result.performance_counter())
        {
            const auto value_per_operation = value / double(result.operation_count());
            stream << "\t" << value_per_operation << " " << name << "/op";
        }

        if constexpr (mx::tasking::config::is_use_task_counter())
        {
            const auto &task_counter = result.task_counter();
            stream << "\t"
                   << task_counter.at(mx::tasking::profiling::TaskCounter::Counter::ExecutedWriter).sum() /
                          double(result.operation_count())
                   << " writer/op";
            stream << "\t"
                   << task_counter.at(mx::tasking::profiling::TaskCounter::Counter::ExecutedReader).sum() /
                          double(result.operation_count())
                   << " reader/op";
            stream << "\t"
                   << task_counter.at(mx::tasking::profiling::TaskCounter::Counter::DispatchedLocally).sum() /
                          double(result.operation_count())
                   << " locally/op";
            stream << "\t"
                   << task_counter.at(mx::tasking::profiling::TaskCounter::Counter::DispatchedRemotely).sum() /
                          double(result.operation_count())
                   << " remotely/op";
            stream << "\t"
                   << task_counter.at(mx::tasking::profiling::TaskCounter::Counter::FilledBuffer).sum() /
                          double(result.operation_count())
                   << " fills/op";
        }

        return stream << std::flush;
    }

public:
    InterimResult(const std::uint64_t operation_count, const P &phase, const std::uint16_t iteration,
                  const std::uint16_t core_count, const std::chrono::milliseconds time,
                  std::vector<std::pair<std::string, double>> &&performance_counter_results,
                  std::unordered_map<mx::tasking::profiling::TaskCounter::Counter,
                                     mx::tasking::profiling::WorkerTaskCounter> &&task_counter)
        : _operation_count(operation_count), _phase(phase), _iteration(iteration), _core_count(core_count), _time(time),
          _performance_counter(std::move(performance_counter_results)), _task_counter(std::move(task_counter))
    {
    }

    ~InterimResult() = default;

    std::uint64_t operation_count() const noexcept { return _operation_count; }
    const P &phase() const noexcept { return _phase; }
    std::uint16_t iteration() const noexcept { return _iteration; }
    std::uint16_t core_count() const noexcept { return _core_count; }
    std::chrono::milliseconds time() const noexcept { return _time; }
    double throughput() const { return _operation_count / (_time.count() / 1000.0); }
    const std::vector<std::pair<std::string, double>> &performance_counter() const noexcept
    {
        return _performance_counter;
    }

    const std::unordered_map<mx::tasking::profiling::TaskCounter::Counter, mx::tasking::profiling::WorkerTaskCounter>
        &task_counter() const noexcept
    {
        return _task_counter;
    }

    [[nodiscard]] nlohmann::json to_json() const noexcept
    {
        auto json = nlohmann::json{};
        json["iteration"] = iteration();
        json["cores"] = core_count();
        json["phase"] = phase();
        json["ms"] = _time.count();
        json["throughput"] = throughput();
        for (const auto &[name, value] : performance_counter())
        {
            json[name] = value / double(operation_count());
        }

        if constexpr (mx::tasking::config::is_use_task_counter())
        {
            json["executed-writer"] =
                _task_counter.at(mx::tasking::profiling::TaskCounter::Counter::ExecutedWriter).sum() /
                double(operation_count());
            json["executed-reader"] =
                _task_counter.at(mx::tasking::profiling::TaskCounter::Counter::ExecutedReader).sum() /
                double(operation_count());
            json["dispatched-locally"] =
                _task_counter.at(mx::tasking::profiling::TaskCounter::Counter::DispatchedLocally).sum() /
                double(operation_count());
            json["dispatched-remotely"] =
                _task_counter.at(mx::tasking::profiling::TaskCounter::Counter::DispatchedRemotely).sum() /
                double(operation_count());
            json["filled-buffer"] =
                _task_counter.at(mx::tasking::profiling::TaskCounter::Counter::ExecutedWriter).sum() /
                double(operation_count());
        }

        return json;
    }

private:
    const std::uint64_t _operation_count;
    const P &_phase;
    const std::uint16_t _iteration;
    const std::uint16_t _core_count;
    const std::chrono::milliseconds _time;
    std::vector<std::pair<std::string, double>> _performance_counter;
    const std::unordered_map<mx::tasking::profiling::TaskCounter::Counter, mx::tasking::profiling::WorkerTaskCounter>
        _task_counter;
};
/**
 * The Chronometer is the "benchmark clock", which will be started and stopped
 * before and after each benchmark run. On stopping, the chronometer will calculate
 * used time, persist performance counter values, and mx::tasking statistics.
 */
template <typename P> class Chronometer
{
public:
    Chronometer() = default;
    ~Chronometer() = default;

    void setup(const P phase, const std::uint16_t iteration, const mx::util::core_set &core_set)
    {
        _current_phase = phase;
        _current_iteration = iteration;
        _core_set = core_set;

        for (auto worker_id = 0U; worker_id < core_set.count_cores(); ++worker_id)
        {
            auto worker_local_groups = std::vector<perf::GroupCounter>{};
            worker_local_groups.reserve(_perf_groups.size());
            for (const auto &group_descriptions : _perf_groups)
            {
                worker_local_groups.emplace_back(group_descriptions);
            }
            _worker_local_group_counter.emplace_back(std::move(worker_local_groups));
        }
    }

    void start(const std::uint16_t worker_id)
    {
        for (auto &group : _worker_local_group_counter[worker_id])
        {
            if (group.open())
            {
                std::ignore = group.start();
            }
        }
    }

    void start() { _start = std::chrono::steady_clock::now(); }

    InterimResult<P> stop(const std::uint64_t count_operations)
    {
        const auto end = std::chrono::steady_clock::now();

        const auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(end - _start);

        for (auto worker_id = 0U; worker_id < _core_set.count_cores(); ++worker_id)
        {
            for (auto &worker_local_groups : _worker_local_group_counter)
            {
                for (auto &group : worker_local_groups)
                {
                    std::ignore = group.stop();
                    group.close();
                }
            }
        }

        /// Aggregate all performance counters of all workers.
        auto perf_results_map = std::unordered_map<std::string, double>{};
        for (const auto &worker_local_groups : _worker_local_group_counter)
        {
            for (const auto &group : worker_local_groups)
            {
                for (auto &[name, value] : group.get())
                {
                    if (auto iterator = perf_results_map.find(name); iterator != perf_results_map.end())
                    {
                        iterator->second += value;
                    }
                    else
                    {
                        perf_results_map.insert(std::make_pair(std::move(name), value));
                    }
                }
            }
        }
        _worker_local_group_counter.clear();

        auto perf_results = std::vector<std::pair<std::string, double>>{};
        for (const auto &counters : _perf_groups)
        {
            for (const auto &counter : counters)
            {
                if (auto iterator = perf_results_map.find(counter.name()); iterator != perf_results_map.end())
                {
                    perf_results.emplace_back(counter.name(), iterator->second);
                }
            }
        }

        return InterimResult{count_operations,
                             _current_phase,
                             _current_iteration,
                             _core_set.count_cores(),
                             milliseconds,
                             std::move(perf_results),
                             mx::tasking::runtime::task_counter()};
    }

    void add(std::vector<perf::CounterDescription> &&group) { _perf_groups.emplace_back(std::move(group)); }

private:
    std::uint16_t _current_iteration{0U};
    P _current_phase;
    mx::util::core_set _core_set;

    /// List with all counter descriptions, grouped.
    std::vector<std::vector<perf::CounterDescription>> _perf_groups;

    /// Group Counter per worker.
    std::vector<std::vector<perf::GroupCounter>> _worker_local_group_counter;

    /// Start of the benchmark.
    alignas(64) std::chrono::steady_clock::time_point _start;
};
} // namespace benchmark