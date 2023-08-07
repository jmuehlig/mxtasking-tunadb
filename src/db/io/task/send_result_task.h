#pragma once

#include <db/io/query_result.h>
#include <db/topology/configuration.h>
#include <db/util/chronometer.h>
#include <memory>
#include <mx/tasking/profiling/time.h>
#include <mx/tasking/task.h>
#include <perf/imc/dram_bandwidth_monitor.h>
#include <string>

namespace db::io {
class SendQueryResultTask final : public mx::tasking::TaskInterface
{
public:
    constexpr SendQueryResultTask(const std::uint32_t client_id, const std::chrono::microseconds time) noexcept
        : _client_id(client_id), _time(time)
    {
    }

    SendQueryResultTask(const std::uint32_t client_id, const std::chrono::microseconds time,
                        std::unique_ptr<QueryResult> &&result) noexcept
        : _client_id(client_id), _time(time), _result(std::move(result))
    {
    }

    ~SendQueryResultTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::chrono::microseconds _time;
    std::unique_ptr<QueryResult> _result;
};

class SendErrorTask final : public mx::tasking::TaskInterface
{
public:
    SendErrorTask(const std::uint32_t client_id, std::string &&error) noexcept
        : _client_id(client_id), _error(std::move(error))
    {
    }
    ~SendErrorTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    std::string _error;
};

class SendPerformanceCounterTask final : public mx::tasking::TaskInterface
{
public:
    SendPerformanceCounterTask(const std::uint32_t client_id, const std::uint64_t count_records,
                               std::shared_ptr<util::Chronometer> &&performance_result) noexcept
        : _client_id(client_id), _count_records(count_records), _performance_result(std::move(performance_result))
    {
    }

    ~SendPerformanceCounterTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::uint64_t _count_records;
    std::shared_ptr<util::Chronometer> _performance_result;

    [[nodiscard]] float time(util::Chronometer::Id lap_id) const;
};

class SendSampleAssemblyTask final : public mx::tasking::TaskInterface
{
public:
    SendSampleAssemblyTask(const std::uint32_t client_id, const std::uint64_t count_records,
                           std::shared_ptr<util::Chronometer> &&chronometer, nlohmann::json &&programs) noexcept
        : _client_id(client_id), _count_records(count_records), _chronometer(std::move(chronometer)),
          _programs(std::move(programs))
    {
    }

    ~SendSampleAssemblyTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::uint64_t _count_records;
    std::shared_ptr<util::Chronometer> _chronometer;
    nlohmann::json _programs;
};

class SendSampleOperatorsTask final : public mx::tasking::TaskInterface
{
public:
    SendSampleOperatorsTask(const std::uint32_t client_id, const std::uint64_t count_records,
                            std::shared_ptr<util::Chronometer> &&chronometer, nlohmann::json &&programs) noexcept
        : _client_id(client_id), _count_records(count_records), _chronometer(std::move(chronometer)),
          _programs(std::move(programs))
    {
    }

    ~SendSampleOperatorsTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::uint64_t _count_records;
    std::shared_ptr<util::Chronometer> _chronometer;
    nlohmann::json _programs;
};

class SendSampleMemoryTask final : public mx::tasking::TaskInterface
{
public:
    SendSampleMemoryTask(const std::uint32_t client_id, const std::uint64_t count_records,
                         std::shared_ptr<util::Chronometer> &&chronometer, nlohmann::json &&samples) noexcept
        : _client_id(client_id), _count_records(count_records), _chronometer(std::move(chronometer)),
          _samples(std::move(samples))
    {
    }

    ~SendSampleMemoryTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::uint64_t _count_records;
    std::shared_ptr<util::Chronometer> _chronometer;
    nlohmann::json _samples;
};

class SendSampleMemoryHistoryTask final : public mx::tasking::TaskInterface
{
public:
    SendSampleMemoryHistoryTask(const std::uint32_t client_id, const std::uint64_t count_records,
                                std::shared_ptr<util::Chronometer> &&chronometer, nlohmann::json &&samples) noexcept
        : _client_id(client_id), _count_records(count_records), _chronometer(std::move(chronometer)),
          _samples(std::move(samples))
    {
    }

    ~SendSampleMemoryHistoryTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::uint64_t _count_records;
    std::shared_ptr<util::Chronometer> _chronometer;
    nlohmann::json _samples;
};

class SendTaskLoadTask final : public mx::tasking::TaskInterface
{
public:
    SendTaskLoadTask(const std::uint32_t client_id, const std::chrono::microseconds time,
                     const std::uint64_t count_records,
                     mx::tasking::profiling::WorkerIdleFrames &&worker_idle_frames) noexcept
        : _client_id(client_id), _time(time), _count_records(count_records),
          _worker_idle_frames(std::make_unique<mx::tasking::profiling::WorkerIdleFrames>(std::move(worker_idle_frames)))
    {
    }

    ~SendTaskLoadTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::chrono::microseconds _time;
    const std::uint64_t _count_records;
    std::unique_ptr<mx::tasking::profiling::WorkerIdleFrames> _worker_idle_frames;
};

class SendTaskTraceTask final : public mx::tasking::TaskInterface
{
public:
    SendTaskTraceTask(const std::uint32_t client_id, const std::chrono::microseconds time,
                      const std::uint64_t count_records,
                      std::unique_ptr<mx::tasking::profiling::TaskTraces> &&task_traces) noexcept
        : _client_id(client_id), _time(time), _count_records(count_records), _task_traces(std::move(task_traces))
    {
    }

    ~SendTaskTraceTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::chrono::microseconds _time;
    const std::uint64_t _count_records;
    std::unique_ptr<mx::tasking::profiling::TaskTraces> _task_traces;
};

class SendConfigurationTask final : public mx::tasking::TaskInterface
{
public:
    SendConfigurationTask(const std::uint32_t client_id, const topology::Configuration &configuration) noexcept
        : _client_id(client_id), _configuration(configuration)
    {
    }
    ~SendConfigurationTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const topology::Configuration _configuration;
};

class SendMemoryBandwithTask final : public mx::tasking::TaskInterface
{
public:
    SendMemoryBandwithTask(const std::uint32_t client_id, const std::chrono::microseconds time,
                           const std::uint64_t count_records,
                           std::vector<perf::DRAMBandwidthMonitor::BandwithSample> &&samples,
                           std::vector<std::pair<std::uint64_t, std::string>> &&events) noexcept
        : _client_id(client_id), _time(time), _count_records(count_records), _samples(std::move(samples)),
          _events(std::move(events))
    {
    }
    ~SendMemoryBandwithTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::chrono::microseconds _time;
    const std::uint64_t _count_records;
    std::vector<perf::DRAMBandwidthMonitor::BandwithSample> _samples;
    std::vector<std::pair<std::uint64_t, std::string>> _events;
};

class SendDataFlowGraphTask final : public mx::tasking::TaskInterface
{
public:
    SendDataFlowGraphTask(const std::uint32_t client_id, const std::chrono::microseconds time,
                          const std::uint64_t count_records, std::string &&dot) noexcept
        : _client_id(client_id), _time(time), _count_records(count_records), _dot(std::move(dot))
    {
    }
    ~SendDataFlowGraphTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::chrono::microseconds _time;
    const std::uint64_t _count_records;
    std::string _dot;
};

class SendTimesTask final : public mx::tasking::TaskInterface
{
public:
    SendTimesTask(const std::uint32_t client_id, const std::chrono::microseconds time,
                  const std::uint64_t count_records,
                  std::vector<std::pair<std::string, std::uint64_t>> &&node_times) noexcept
        : _client_id(client_id), _time(time), _count_records(count_records), _node_times(std::move(node_times))
    {
    }
    ~SendTimesTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    const std::chrono::microseconds _time;
    const std::uint64_t _count_records;
    std::vector<std::pair<std::string, std::uint64_t>> _node_times;
};
} // namespace db::io