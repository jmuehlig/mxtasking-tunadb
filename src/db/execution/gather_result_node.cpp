#include "gather_result_node.h"
#include "memory_tracing_node.h"
#include <db/plan/physical/compilation_graph.h>
#include <fmt/core.h>

using namespace db::execution;

GatherQueryResultNode::GatherQueryResultNode(const std::uint32_t client_id,
                                             std::shared_ptr<db::util::Chronometer> &&chronometer,
                                             const db::topology::PhysicalSchema &schema)
    : _client_id(client_id), _chronometer(std::move(chronometer)), _schema(schema)
{
    const auto count_cores = mx::tasking::runtime::workers();
    this->_worker_local_results =
        new mx::util::aligned_t<std::vector<std::pair<std::uint64_t, RecordToken>>>[count_cores];
    for (auto worker_id = 0U; worker_id < count_cores; ++worker_id)
    {
        this->_worker_local_results[worker_id].value().reserve(1U << 8U);
    }
}

void GatherQueryResultNode::in_completed(const std::uint16_t worker_id,
                                         mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                         mx::tasking::dataflow::NodeInterface<RecordSet> & /*node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);

    /// Merge results.
    auto tokens = std::vector<std::pair<std::uint64_t, RecordToken>>{};
    for (auto local_worker_id = 0U; local_worker_id < mx::tasking::runtime::workers(); ++local_worker_id)
    {
        std::move(this->_worker_local_results[local_worker_id].value().begin(),
                  this->_worker_local_results[local_worker_id].value().end(), std::back_inserter(tokens));
    }

    /// Sort by their insertion order.
    std::sort(tokens.begin(), tokens.end(),
              [](const auto &first, const auto &second) { return std::get<0>(first) < std::get<0>(second); });

    auto query_result = std::make_unique<io::QueryResult>(std::move(this->_schema));
    for (auto &token : tokens)
    {
        query_result->add(std::move(std::get<1>(token).data()));
    }

    auto *result_task = mx::tasking::runtime::new_task<io::SendQueryResultTask>(
        worker_id, this->_client_id, this->_chronometer->microseconds(), std::move(query_result));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

GatherPerformanceCounterNode::GatherPerformanceCounterNode(const std::uint32_t client_id,
                                                           std::shared_ptr<db::util::Chronometer> &&chronometer)
    : _client_id(client_id), _chronometer(std::move(chronometer))
{
    /// Generic counters.
    this->_chronometer->add({perf::CounterDescription::CYCLES, perf::CounterDescription::INSTRUCTIONS,
                             perf::CounterDescription::CYCLE_ACTIVITY_STALLS_MEM_ANY,
                             perf::CounterDescription::L1D_PEND_MISS_FB_FULL});

    //    this->_chronometer->add(
    //        std::vector<perf::CounterDescription>{perf::CounterDescription::OFFCORE_REQUESTS_ALL_DATA_RD,
    //                                              perf::CounterDescription::OFFCORE_REQUESTS_DEMAND_DATA_RD});

    //    /// Stalled cycle counters.
    //    this->_chronometer->add({perf::CounterDescription::CYCLE_ACTIVITY_STALLS_L1D_MISS,
    //                             perf::CounterDescription::CYCLE_ACTIVITY_STALLS_L2_MISS,
    //                             perf::CounterDescription::CYCLE_ACTIVITY_STALLS_L3_MISS,
    //                             perf::CounterDescription::CYCLE_ACTIVITY_STALLS_MEM_ANY});
    //
    //    /// TLB counters.
    //    this->_chronometer->add(std::vector<perf::CounterDescription>{perf::CounterDescription::DTLB_LOAD_MISSES,
    //                                                                  perf::CounterDescription::STLB_LOAD_MISSES});
    //
    //    /// Cache Counters.
    //    this->_chronometer->add(std::vector<perf::CounterDescription>{perf::CounterDescription::L1D_LOAD_MISSES,
    //                                                                  perf::CounterDescription::L2_RQST_MISS,
    //                                                                  perf::CounterDescription::LLC_LOAD_MISSES});
    //
    //    /// Stalled NUMA counters.
    //    this->_chronometer->add(std::vector<perf::CounterDescription>{
    //        perf::CounterDescription::NODE_LOADS, perf::CounterDescription::NODE_LOAD_MISSES,
    //        perf::CounterDescription::MEM_LOAD_L3_MISS_RETIRED_LOCAL_DRAM,
    //        perf::CounterDescription::MEM_LOAD_L3_MISS_RETIRED_REMOTE_DRAM});
    //
    //    /// Prefetch counters.
    //    this->_chronometer->add(std::vector<perf::CounterDescription>{
    //        perf::CounterDescription::SW_PREFETCH_ACCESS_T0, perf::CounterDescription::SW_PREFETCH_ACCESS_T1_T2,
    //        perf::CounterDescription::L1D_PEND_MISS_FB_FULL, perf::CounterDescription::LOAD_HIT_PRE_SW_PF});
}

void GatherPerformanceCounterNode::in_completed(const std::uint16_t worker_id,
                                                mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                                mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);
    auto *result_task = mx::tasking::runtime::new_task<io::SendPerformanceCounterTask>(
        worker_id, this->_client_id, this->_count_records.load(), std::move(this->_chronometer));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

GatherSampleAssemblyNode::GatherSampleAssemblyNode(const std::uint32_t client_id,
                                                   std::shared_ptr<db::util::Chronometer> &&chronometer,
                                                   const perf::CounterDescription &counter,
                                                   std::optional<std::uint64_t> frequency)
    : _client_id(client_id), _chronometer(std::move(chronometer))
{
    this->_chronometer->add(counter, perf::Sample::Type::Instruction,
                            frequency.value_or(config::default_sample_frequency()));
}

void GatherSampleAssemblyNode::in_completed(const std::uint16_t worker_id,
                                            mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                            mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);
    const auto &samples = this->_chronometer->result(util::Chronometer::Id::Executing).performance_aggregated_samples();

    /// List of programs where program is a tuple of (name, [ (instruction address, instruction) ])
    auto programs = reinterpret_cast<plan::physical::CompilationGraph *>(&graph)->to_assembly(samples.value());

    auto *result_task = mx::tasking::runtime::new_task<io::SendSampleAssemblyTask>(
        worker_id, this->_client_id, this->_count_records.load(), std::move(this->_chronometer), std::move(programs));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

GatherSampleOperatorsNode::GatherSampleOperatorsNode(const std::uint32_t client_id,
                                                     std::shared_ptr<db::util::Chronometer> &&chronometer,
                                                     const perf::CounterDescription &counter,
                                                     std::optional<std::uint64_t> frequency)
    : _client_id(client_id), _chronometer(std::move(chronometer))
{
    this->_chronometer->add(counter, perf::Sample::Type::Instruction,
                            frequency.value_or(config::default_sample_frequency()));
}

void GatherSampleOperatorsNode::in_completed(std::uint16_t worker_id,
                                             mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                             mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);
    const auto &samples = this->_chronometer->result(util::Chronometer::Id::Executing).performance_aggregated_samples();

    /// List of programs where program is a tuple of (name, [ (instruction address, instruction) ])
    auto programs = reinterpret_cast<plan::physical::CompilationGraph *>(&graph)->to_contexts(samples.value());

    auto *result_task = mx::tasking::runtime::new_task<io::SendSampleOperatorsTask>(
        worker_id, this->_client_id, this->_count_records.load(), std::move(this->_chronometer), std::move(programs));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

GatherSampleMemoryNode::GatherSampleMemoryNode(const topology::Database &database, const std::uint32_t client_id,
                                               std::shared_ptr<db::util::Chronometer> &&chronometer,
                                               const perf::CounterDescription &counter,
                                               std::optional<std::uint64_t> frequency)
    : _database(database), _client_id(client_id), _chronometer(std::move(chronometer))
{
    this->_chronometer->add(counter, perf::Sample::Type::Address,
                            frequency.value_or(config::default_sample_frequency()));
}

void GatherSampleMemoryNode::in_completed(const std::uint16_t worker_id,
                                          mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                          mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);

    const auto &samples = this->_chronometer->result(util::Chronometer::Id::Executing).performance_aggregated_samples();
    auto tile_samples = this->_database.map_to_tiles(samples.value());

    /// Transform to json.
    auto samples_json = nlohmann::json{{"count", samples.value().count()}};
    for (auto &[name, tile_sample] : tile_samples)
    {
        auto tile_json = nlohmann::json{};
        tile_json["name"] = name;
        tile_json["samples"] = tile_sample.samples();

        for (auto &column : tile_sample.columns())
        {
            if (column.has_sample())
            {
                auto column_json = nlohmann::json{};
                column_json["name"] = column.name();
                column_json["id"] = column.id();
                column_json["offset"] = column.offset();
                for (const auto column_sample : column.samples())
                {
                    column_json["samples"].emplace_back(column_sample);
                }
                tile_json["columns"].emplace_back(std::move(column_json));
            }
        }

        samples_json["tiles"].emplace_back(std::move(tile_json));
    }

    auto *result_task = mx::tasking::runtime::new_task<io::SendSampleMemoryTask>(
        worker_id, this->_client_id, this->_count_records.load(), std::move(this->_chronometer),
        std::move(samples_json));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

GatherSampleMemoryHistoryNode::GatherSampleMemoryHistoryNode(const std::uint32_t client_id,
                                                             std::shared_ptr<db::util::Chronometer> &&chronometer,
                                                             const perf::CounterDescription &counter,
                                                             std::optional<std::uint64_t> frequency)
    : _client_id(client_id), _chronometer(std::move(chronometer))
{
    this->_chronometer->add(counter, perf::Sample::Type::Address | perf::Sample::Type::Time,
                            frequency.value_or(config::default_sample_frequency()));
}

void GatherSampleMemoryHistoryNode::in_completed(const std::uint16_t worker_id,
                                                 mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                                 mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);

    /// Transform memory address samples to json.
    auto &result = this->_chronometer->result(util::Chronometer::Id::Executing);
    auto &samples = result.performance_historical_samples();
    auto samples_json = nlohmann::json{};
    if (samples.has_value())
    {
        auto addresses_json = nlohmann::json{};
        for (const auto &[time, value] : samples.value().samples())
        {
            auto item = nlohmann::json{};
            item["t"] = time;
            item["a"] = value;

            addresses_json.emplace_back(std::move(item));
        }

        samples_json["samples"] = std::move(addresses_json);
    }

    /// Transform memory tags to json.
    auto tags_json = nlohmann::json{};

    //// Tags from operators (operator help structures).
    if (result.memory_tags().has_value())
    {
        for (auto &&[tag, ranges] : result.memory_tags().value())
        {
            auto item = nlohmann::json{};
            item["name"] = std::move(tag);

            for (const auto &[begin, end] : ranges)
            {
                item["ranges"].emplace_back(nlohmann::json{{"b", begin}, {"e", end}});
            }

            tags_json.emplace_back(std::move(item));
        }
    }

    /// Tags from emitted data (materialized tokens).
    graph.for_each_node([&tags_json](auto *node) {
        if (typeid(*node) == typeid(MemoryTracingNode))
        {
            auto *memory_tracing_node = dynamic_cast<MemoryTracingNode *>(node);
            auto ranges = nlohmann::json{};
            for (const auto &[begin, end] : memory_tracing_node->ranges())
            {
                ranges.emplace_back(nlohmann::json{{"b", begin}, {"e", end}});
            }
            tags_json.emplace_back(
                nlohmann::json{{"name", memory_tracing_node->data_name()}, {"ranges", std::move(ranges)}});
        }
    });

    /// Tags from tasking (workers, tasks).
    for (auto &&[tag, ranges] : mx::tasking::runtime::memory_tags())
    {
        auto item = nlohmann::json{};
        item["name"] = std::move(tag);

        for (const auto &[begin, end] : ranges)
        {
            item["ranges"].emplace_back(nlohmann::json{{"b", begin}, {"e", end}});
        }

        tags_json.emplace_back(std::move(item));
    }

    samples_json["tags"] = std::move(tags_json);

    auto *result_task = mx::tasking::runtime::new_task<io::SendSampleMemoryHistoryTask>(
        worker_id, this->_client_id, this->_count_records.load(), std::move(this->_chronometer),
        std::move(samples_json));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

void GatherTaskLoadNode::in_completed(const std::uint16_t worker_id,
                                      mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                      mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);
    auto idle_times = mx::tasking::runtime::stop_idle_profiler();
    auto *result_task = mx::tasking::runtime::new_task<io::SendTaskLoadTask>(
        worker_id, this->_client_id, this->_chronometer->result(util::Chronometer::Id::Executing).microseconds(),
        this->_count_records.load(), idle_times.group(std::chrono::nanoseconds(2000000U)));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

void GatherTaskTraceNode::in_completed(const std::uint16_t worker_id,
                                       mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                       mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);

    auto task_traces = mx::tasking::runtime::stop_tracing();
    auto *result_task = mx::tasking::runtime::new_task<io::SendTaskTraceTask>(
        worker_id, this->_client_id, this->_chronometer->result(util::Chronometer::Id::Executing).microseconds(),
        this->_count_records.load(), std::make_unique<mx::tasking::profiling::TaskTraces>(std::move(task_traces)));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

void GatherMemoryBandwidthNode::in_completed(const std::uint16_t worker_id,
                                             mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                             mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);
    auto bandwith = this->_bandwidth_monitor.stop(this->_chronometer->start_time());
    auto events = this->_chronometer->timed_events().normalized(this->_chronometer->start_time());

    auto *result_task = mx::tasking::runtime::new_task<io::SendMemoryBandwithTask>(
        worker_id, this->_client_id, this->_chronometer->microseconds(), this->_count_records.load(),
        std::move(bandwith), std::move(events));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

void GatherDataFlowGraphNode::in_completed(std::uint16_t worker_id,
                                           mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                           mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);

    auto *data_flow_graph = reinterpret_cast<plan::physical::DataFlowGraph *>(&graph);
    auto dot = plan::physical::DataFlowGraph::to_dot(data_flow_graph, true);

    auto *result_task = mx::tasking::runtime::new_task<io::SendDataFlowGraphTask>(
        worker_id, this->_client_id, this->_chronometer->microseconds(), this->_count_records.load(), std::move(dot));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}

void GatherTimesNode::in_completed(const std::uint16_t worker_id,
                                   mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                                   mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/)
{
    this->_chronometer->stop(util::Chronometer::Id::Executing);

    auto *data_flow_graph = reinterpret_cast<plan::physical::DataFlowGraph *>(&graph);

    auto times = std::vector<std::pair<std::string, std::uint64_t>>{};
    auto node_times = data_flow_graph->node_times();
    std::transform(node_times.begin(), node_times.end(), std::back_inserter(times), [](auto node_and_time) {
        return std::make_pair(std::get<0>(node_and_time)->to_string(), std::get<1>(node_and_time).count());
    });

    auto *result_task = mx::tasking::runtime::new_task<io::SendTimesTask>(
        worker_id, this->_client_id, this->_chronometer->microseconds(), this->_count_records.load(), std::move(times));
    mx::tasking::runtime::spawn(*result_task, worker_id);

    graph.finalize(worker_id, this);
    mx::tasking::runtime::defragment();
}