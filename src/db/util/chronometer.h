#pragma once

#include "timed_events.h"
#include <chrono>
#include <db/exception/execution_exception.h>
#include <mx/system/cache.h>
#include <mx/tasking/profiling/task_counter.h>
#include <mx/tasking/runtime.h>
#include <nlohmann/json.hpp>
#include <numeric>
#include <perf/counter.h>
#include <perf/sample.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace db::util {
/**
 * The InterimResult is part of the chronometer, which in turn holds
 * all results during a benchmark.
 */
class ChronometerResult
{
public:
    explicit ChronometerResult(const std::chrono::microseconds microseconds) noexcept : _time(microseconds) {}

    ChronometerResult(
        const std::chrono::microseconds microseconds, std::vector<std::pair<std::string, double>> &&perf_counters,
        std::optional<perf::AggregatedSamples> &&perf_aggregated_samples,
        std::optional<perf::HistoricalSamples> &&perf_historical_samples,
        std::optional<std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>>
            &&memory_tags,
        mx::tasking::profiling::WorkerTaskCounter &&task_counter) noexcept
        : _time(microseconds), _perf_counter(std::move(perf_counters)),
          _perf_aggregated_samples(std::move(perf_aggregated_samples)),
          _perf_historical_samples(std::move(perf_historical_samples)), _memory_tags(std::move(memory_tags)),
          _task_counter(std::move(task_counter))
    {
    }

    ~ChronometerResult() noexcept = default;

    [[nodiscard]] std::chrono::microseconds microseconds() const noexcept { return _time; }
    [[nodiscard]] const std::vector<std::pair<std::string, double>> &performance_counter() const noexcept
    {
        return _perf_counter;
    }
    [[nodiscard]] const mx::tasking::profiling::WorkerTaskCounter &task_counter() const noexcept
    {
        return _task_counter;
    }
    [[nodiscard]] const std::optional<perf::AggregatedSamples> &performance_aggregated_samples() const noexcept
    {
        return _perf_aggregated_samples;
    }
    [[nodiscard]] const std::optional<perf::HistoricalSamples> &performance_historical_samples() const noexcept
    {
        return _perf_historical_samples;
    }

    [[nodiscard]] std::optional<std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>>
        &memory_tags() noexcept
    {
        return _memory_tags;
    }

private:
    const std::chrono::microseconds _time;
    std::vector<std::pair<std::string, double>> _perf_counter;
    std::optional<perf::AggregatedSamples> _perf_aggregated_samples{std::nullopt};
    std::optional<perf::HistoricalSamples> _perf_historical_samples{std::nullopt};
    std::optional<std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>> _memory_tags{
        std::nullopt};
    mx::tasking::profiling::WorkerTaskCounter _task_counter;
};

class StartSampleTask final : public mx::tasking::TaskInterface
{
public:
    constexpr explicit StartSampleTask(perf::Sample &sample) noexcept : _sample(sample) {}

    ~StartSampleTask() noexcept override = default;

    [[nodiscard]] mx::tasking::TaskResult execute(const std::uint16_t /*worker_id*/) override
    {
        if (_sample.open())
        {
            _sample.start();
        }

        return mx::tasking::TaskResult::make_remove();
    }

private:
    perf::Sample &_sample;
};

class StartCounterTask final : public mx::tasking::TaskInterface
{
public:
    constexpr explicit StartCounterTask(perf::CounterManager &counter) noexcept : _counter(counter) {}

    ~StartCounterTask() noexcept override = default;

    [[nodiscard]] mx::tasking::TaskResult execute(const std::uint16_t /*worker_id*/) override
    {
        if (_counter.open())
        {
            _counter.start();
        }

        return mx::tasking::TaskResult::make_remove();
    }

private:
    perf::CounterManager &_counter;
};

class StartGroupCounterTask final : public mx::tasking::TaskInterface
{
public:
    constexpr explicit StartGroupCounterTask(std::vector<perf::GroupCounter> &counters) noexcept : _counters(counters)
    {
    }

    ~StartGroupCounterTask() noexcept override = default;

    [[nodiscard]] mx::tasking::TaskResult execute(const std::uint16_t /*worker_id*/) override
    {
        for (auto &group : _counters)
        {
            if (group.open())
            {
                group.start();
            }
        }

        return mx::tasking::TaskResult::make_remove();
    }

private:
    std::vector<perf::GroupCounter> &_counters;
};

/**
 * The Chronometer is the "benchmark clock", which will be started and stopped
 * before and after each benchmark run. On stopping, the chronometer will calculate
 * used time, persist performance counter values, and mx::tasking statistics.
 */
class Chronometer
{
public:
    enum Id : std::uint16_t
    {
        Parsing,
        CreatingLogicalPlan,
        OptimizingLogicalPlan,
        GeneratingFlounder,
        CompilingFlounder,
        CreatingPhysicalPlan,
        Executing,
    };

    Chronometer() noexcept = default;
    ~Chronometer()
    {
        const auto count_workers = mx::tasking::runtime::workers();

        for (auto &worker_local_counter_groups : _perf_group_counters)
        {
            for (auto &group_counter : worker_local_counter_groups)
            {
                group_counter.close();
            }
        }

        if (_perf_samples != nullptr)
        {
            for (auto worker_id = 0U; worker_id < count_workers; ++worker_id)
            {
                _perf_samples[worker_id].close();
                _perf_samples[worker_id].~Sample();
            }

            std::free(_perf_samples);
        }
    }

    void start()
    {
        _start_time = std::chrono::steady_clock::now();
        _start_task_counter =
            mx::tasking::runtime::task_counter(mx::tasking::profiling::TaskCounter::Counter::Executed);
    }

    void reset() { _start_time = std::chrono::steady_clock::now(); }

    void stop(const Id id)
    {
        const auto end_time = std::chrono::steady_clock::now();

        if (id == Id::Executing)
        {
            for (auto &worker_local_counter_groups : _perf_group_counters)
            {
                for (auto &group_counter : worker_local_counter_groups)
                {
                    group_counter.stop();
                }
            }

            if (_perf_samples != nullptr)
            {
                for (auto worker_id = 0U; worker_id < mx::tasking::runtime::workers(); ++worker_id)
                {
                    _perf_samples[worker_id].stop();
                }
            }
        }

        const auto time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - _start_time);

        if (id != Id::Executing)
        {
            /// For most laps, we only record the time.
            _lap_results.insert(std::make_pair(id, ChronometerResult{time}));
        }
        else
        {
            /// For execution (only), we record time, tasks, performance counters, and samples.
            const auto end_task_counter =
                mx::tasking::runtime::task_counter(mx::tasking::profiling::TaskCounter::Counter::Executed);

            auto executed_tasks = end_task_counter - _start_task_counter;

            /// Get all performance counter values.
            auto perf_counters = std::unordered_map<std::string, double>{};
            for (const auto &worker_local_counter_groups : _perf_group_counters)
            {
                for (const auto &counter_group : worker_local_counter_groups)
                {
                    for (auto &[name, value] : counter_group.get())
                    {
                        if (auto counter_iterator = perf_counters.find(name); counter_iterator != perf_counters.end())
                        {
                            counter_iterator->second += value;
                        }
                        else
                        {
                            perf_counters.insert(std::make_pair(std::move(name), value));
                        }
                    }
                }
            }

            /// Transform into a list.
            auto perf_counter_list = std::vector<std::pair<std::string, double>>{};
            perf_counter_list.reserve(perf_counters.size());
            for (auto &[name, value] : perf_counters)
            {
                perf_counter_list.emplace_back(std::move(name), value);
            }

            /// Sort the list.
            auto counter_order = std::unordered_map<std::string, std::uint16_t>{};
            counter_order.reserve(perf_counter_list.size());
            auto order = 0U;
            for (const auto &group_description : _counter_descriptions)
            {
                for (const auto &description : group_description)
                {
                    counter_order.insert(std::make_pair(description.name(), order++));
                }
            }
            std::sort(perf_counter_list.begin(), perf_counter_list.end(),
                      [&counter_order](const auto &left, const auto &right) {
                          return counter_order.at(left.first) < counter_order.at(right.first);
                      });

            /// Collect samples.
            auto perf_aggregated_samples = std::optional<perf::AggregatedSamples>{std::nullopt};
            auto perf_historical_samples = std::optional<perf::HistoricalSamples>{std::nullopt};
            if (_perf_samples != nullptr)
            {
                if (_perf_samples[0].is_historical())
                {
                    auto buffer_tags = std::vector<std::pair<std::uintptr_t, std::uintptr_t>>{};
                    perf_historical_samples = std::make_optional(_perf_samples[0U].get());
                    buffer_tags.emplace_back(_perf_samples[0].buffer_range());
                    for (auto worker_id = 1U; worker_id < mx::tasking::runtime::workers(); ++worker_id)
                    {
                        perf_historical_samples->insert(_perf_samples[worker_id].get());
                        buffer_tags.emplace_back(_perf_samples[worker_id].buffer_range());
                    }

                    this->_memory_tags.insert(std::make_pair("Perf", std::move(buffer_tags)));
                    std::sort(perf_historical_samples->samples().begin(), perf_historical_samples->samples().end(),
                              [](const auto left, const auto right) { return std::get<0>(left) < std::get<0>(right); });
                }
                else
                {
                    perf_aggregated_samples = std::make_optional(_perf_samples[0U].aggregate());
                    for (auto worker_id = 1U; worker_id < mx::tasking::runtime::workers(); ++worker_id)
                    {
                        perf_aggregated_samples->insert(_perf_samples[worker_id].aggregate());
                    }
                }
            }

            _lap_results.insert(std::make_pair(
                id, ChronometerResult{time, std::move(perf_counter_list), std::move(perf_aggregated_samples),
                                      std::move(perf_historical_samples), std::move(_memory_tags),
                                      std::move(executed_tasks)}));
        }
    }

    void lap(const Id id)
    {
        stop(id);
        start();
    }

    void start_perf()
    {
        const auto local_worker_id = mx::tasking::runtime::worker_id();

        /// Start tasks for opening counters on every worker.
        if (_counter_descriptions.empty() == false)
        {
            const auto count_workers = mx::tasking::runtime::workers();
            _perf_group_counters.reserve(count_workers);

            for (auto worker_id = std::uint16_t(0U); worker_id < count_workers; ++worker_id)
            {
                auto worker_local_group_counters = std::vector<perf::GroupCounter>{};
                worker_local_group_counters.reserve(_counter_descriptions.size());

                for (const auto &counters : _counter_descriptions)
                {
                    worker_local_group_counters.emplace_back(counters);
                }

                _perf_group_counters.emplace_back(std::move(worker_local_group_counters));
            }

            for (auto worker_id = std::uint16_t(0U); worker_id < count_workers; ++worker_id)
            {
                auto *task = mx::tasking::runtime::new_task<StartGroupCounterTask>(local_worker_id,
                                                                                   _perf_group_counters[worker_id]);
                task->annotate(worker_id);
                mx::tasking::runtime::spawn(*task, local_worker_id);
            }
        }

        /// Start tasks for opening samples on every worker.
        if (_perf_samples != nullptr)
        {
            for (auto worker_id = std::uint16_t(0U); worker_id < mx::tasking::runtime::workers(); ++worker_id)
            {
                auto *task = mx::tasking::runtime::new_task<StartSampleTask>(local_worker_id, _perf_samples[worker_id]);
                task->annotate(worker_id);
                mx::tasking::runtime::spawn(*task, local_worker_id);
            }
        }
    }

    void add(std::vector<perf::CounterDescription> &&counter_descriptions)
    {
        _counter_descriptions.emplace_back(std::move(counter_descriptions));
    }

    void add(const perf::CounterDescription &counter_description, const std::uint64_t sample_type,
             const std::uint64_t frequency)
    {
        const auto count_workers = mx::tasking::runtime::workers();
        _perf_samples = reinterpret_cast<perf::Sample *>(std::aligned_alloc(64U, count_workers * sizeof(perf::Sample)));

        for (auto worker_id = 0U; worker_id < count_workers; ++worker_id)
        {
            _perf_samples[worker_id] = perf::Sample{counter_description, sample_type, frequency};
        }
    }

    void add(std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>> &&memory_tags)
    {
        for (auto &[name, ranges] : memory_tags)
        {
            if (auto iterator = _memory_tags.find(name); iterator != _memory_tags.end())
            {
                std::move(ranges.begin(), ranges.end(), std::back_inserter(iterator->second));
            }
            else
            {
                _memory_tags.insert(std::make_pair(std::move(name), std::move(ranges)));
            }
        }
    }

    [[nodiscard]] const ChronometerResult &result(const Id id) const noexcept { return _lap_results.at(id); }
    [[nodiscard]] ChronometerResult &result(const Id id) noexcept { return _lap_results.at(id); }

    [[nodiscard]] bool has_result(const Id id) const noexcept { return _lap_results.find(id) != _lap_results.end(); }

    [[nodiscard]] std::chrono::microseconds microseconds() const noexcept
    {
        return std::accumulate(_lap_results.begin(), _lap_results.end(), std::chrono::microseconds{0U},
                               [](const auto &sum, const auto &result) { return sum + result.second.microseconds(); });
    }

    [[nodiscard]] std::chrono::steady_clock::time_point start_time() const noexcept { return _start_time; }

    [[nodiscard]] const TimedEvents &timed_events() const noexcept { return _events; }
    [[nodiscard]] TimedEvents &timed_events() noexcept { return _events; }

private:
    /// List of groups of counters to record.
    std::vector<std::vector<perf::CounterDescription>> _counter_descriptions;

    // Perf Group.
    std::vector<std::vector<perf::GroupCounter>> _perf_group_counters;

    /// Worker-local perf samples.
    perf::Sample *_perf_samples{nullptr};

    std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>> _memory_tags;

    std::chrono::steady_clock::time_point _start_time;
    alignas(mx::system::cache::line_size()) mx::tasking::profiling::WorkerTaskCounter _start_task_counter;
    alignas(mx::system::cache::line_size()) std::unordered_map<Id, ChronometerResult> _lap_results;
    alignas(mx::system::cache::line_size()) TimedEvents _events;
};
} // namespace db::util