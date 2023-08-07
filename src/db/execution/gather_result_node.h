#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <db/execution/compilation/compilation_node.h>
#include <db/execution/record_token.h>
#include <db/io/query_result.h>
#include <db/io/task/send_result_task.h>
#include <db/topology/database.h>
#include <db/topology/physical_schema.h>
#include <db/util/chronometer.h>
#include <db/util/timer.h>
#include <fmt/core.h>
#include <memory>
#include <mx/synchronization/spinlock.h>
#include <mx/system/cache.h>
#include <mx/tasking/dataflow/node.h>
#include <mx/tasking/runtime.h>
#include <mx/util/aligned_t.h>
#include <perf/imc/dram_bandwidth_monitor.h>
#include <vector>

namespace db::execution {
class GatherQueryResultNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherQueryResultNode(std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer,
                          const topology::PhysicalSchema &schema);

    ~GatherQueryResultNode() noexcept override { delete[] _worker_local_results; }

    void consume(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        _worker_local_results[worker_id].value().emplace_back(_result_id.fetch_add(1U), std::move(data));
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "Result"; }

private:
    const std::uint32_t _client_id;
    std::shared_ptr<util::Chronometer> _chronometer;
    topology::PhysicalSchema _schema;
    mx::util::aligned_t<std::vector<std::pair<std::uint64_t, RecordToken>>> *_worker_local_results{nullptr};
    alignas(mx::system::cache::line_size()) std::atomic_uint64_t _result_id{0U};
};

class GatherPerformanceCounterNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherPerformanceCounterNode(std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer);

    ~GatherPerformanceCounterNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "Measure"; }

private:
    const std::uint32_t _client_id;
    std::atomic<std::uint64_t> _count_records{0U};
    std::shared_ptr<util::Chronometer> _chronometer;
};

class GatherSampleAssemblyNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherSampleAssemblyNode(std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer,
                             const perf::CounterDescription &counter, std::optional<std::uint64_t> frequency);

    ~GatherSampleAssemblyNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "SampleAssebly"; }

private:
    const std::uint32_t _client_id;
    std::atomic<std::uint64_t> _count_records{0U};
    std::shared_ptr<util::Chronometer> _chronometer;
};

class GatherSampleOperatorsNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherSampleOperatorsNode(std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer,
                              const perf::CounterDescription &counter, std::optional<std::uint64_t> frequency);

    ~GatherSampleOperatorsNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "SampleOperators"; }

private:
    const std::uint32_t _client_id;
    std::atomic<std::uint64_t> _count_records{0U};
    std::shared_ptr<util::Chronometer> _chronometer;
};

class GatherSampleMemoryNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherSampleMemoryNode(const topology::Database &database, std::uint32_t client_id,
                           std::shared_ptr<util::Chronometer> &&chronometer, const perf::CounterDescription &counter,
                           std::optional<std::uint64_t> frequency);

    ~GatherSampleMemoryNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "SampleMemory"; }

private:
    const topology::Database &_database;
    const std::uint32_t _client_id;
    std::atomic<std::uint64_t> _count_records{0U};
    std::shared_ptr<util::Chronometer> _chronometer;
};

class GatherSampleMemoryHistoryNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherSampleMemoryHistoryNode(std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer,
                                  const perf::CounterDescription &counter, std::optional<std::uint64_t> frequency);

    ~GatherSampleMemoryHistoryNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "SampleMemoryHistory"; }

private:
    const std::uint32_t _client_id;
    std::atomic<std::uint64_t> _count_records{0U};
    std::shared_ptr<util::Chronometer> _chronometer;
};

class GatherTaskLoadNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherTaskLoadNode(const std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer) noexcept
        : _client_id(client_id), _chronometer(std::move(chronometer))
    {
    }

    ~GatherTaskLoadNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "Measure Load"; }

private:
    const std::uint32_t _client_id;
    std::atomic<std::uint64_t> _count_records{0U};
    std::shared_ptr<util::Chronometer> _chronometer;
};

class GatherTaskTraceNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherTaskTraceNode(const std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer) noexcept
        : _client_id(client_id), _chronometer(std::move(chronometer))
    {
    }

    ~GatherTaskTraceNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "Measure Load"; }

private:
    const std::uint32_t _client_id;
    std::atomic<std::uint64_t> _count_records{0U};
    std::shared_ptr<util::Chronometer> _chronometer;
};

class GatherMemoryBandwidthNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherMemoryBandwidthNode(const std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer) noexcept
        : _client_id(client_id), _chronometer(std::move(chronometer)), _bandwidth_monitor(1000U)
    {
        _bandwidth_monitor.start();
    }

    ~GatherMemoryBandwidthNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "Measure Memory Bandwith"; }

private:
    const std::uint32_t _client_id;
    std::shared_ptr<util::Chronometer> _chronometer;
    std::atomic<std::uint64_t> _count_records{0U};
    perf::DRAMBandwidthMonitor _bandwidth_monitor;
};

class GatherDataFlowGraphNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherDataFlowGraphNode(const std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer) noexcept
        : _client_id(client_id), _chronometer(std::move(chronometer))
    {
    }

    ~GatherDataFlowGraphNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "DataFlow Graph"; }

private:
    const std::uint32_t _client_id;
    std::shared_ptr<util::Chronometer> _chronometer;
    std::atomic<std::uint64_t> _count_records{0U};
};

class GatherTimesNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    GatherTimesNode(const std::uint32_t client_id, std::shared_ptr<util::Chronometer> &&chronometer) noexcept
        : _client_id(client_id), _chronometer(std::move(chronometer))
    {
    }

    ~GatherTimesNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken &&data) override
    {
        /// Results are hidden, only number of records is gathered.
        _count_records.fetch_add(data.data().tile().get<data::PaxTile>()->size());
    }

    void in_completed(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                      mx::tasking::dataflow::NodeInterface<RecordSet> &in_node) override;

    [[nodiscard]] std::string to_string() const noexcept override { return "Times"; }

private:
    const std::uint32_t _client_id;
    std::shared_ptr<util::Chronometer> _chronometer;
    std::atomic<std::uint64_t> _count_records{0U};
};
} // namespace db::execution