#pragma once
#include "compilation_plan.h"
#include "dataflow_graph.h"
#include <db/data/row_record_view.h>
#include <db/execution/compilation/compilation_node.h>
#include <db/execution/compilation/operator/operator_interface.h>
#include <db/topology/database.h>
#include <db/util/chronometer.h>
#include <flounder/compilation/compiler.h>
#include <memory>
#include <mx/tasking/dataflow/graph.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace db::plan::physical {
class CompilationGraph final : public DataFlowGraph
{
public:
    CompilationGraph(const bool is_profile, const bool is_keep_compiled_code, const bool is_explain_times)
        : DataFlowGraph(is_explain_times), _compiler(is_profile, is_keep_compiled_code)
    {
    }

    ~CompilationGraph() override = default;

    [[nodiscard]] static CompilationGraph *build(
        const topology::Database &_database, std::shared_ptr<util::Chronometer> chronometer,
        CompilationPlan &&compilation_plan, std::uint32_t client_id, bool is_record_performance,
        bool is_record_task_load, bool is_record_task_traces, bool is_explain_flounder, bool is_explain_assembly,
        bool is_explain_dram_bandwidth, bool is_explain_task_graph, bool is_explain_data_flow_graph,
        bool is_explain_times,
        std::optional<
            std::tuple<logical::SampleNode::Level, logical::SampleNode::CounterType, std::optional<std::uint64_t>>>
            sample_type,
        const perf::Counter &profiling_counter);

    void compile(bool make_visible_to_perf, bool make_visible_to_vtune);

    [[nodiscard]] nlohmann::json to_flounder() const { return CompilationGraph::to_code(false, std::nullopt); }
    [[nodiscard]] nlohmann::json to_assembly() const { return CompilationGraph::to_code(true, std::nullopt); }
    [[nodiscard]] nlohmann::json to_assembly(const perf::AggregatedSamples &samples) const
    {
        return CompilationGraph::to_code(true, std::make_optional(std::ref(samples)));
    }
    [[nodiscard]] nlohmann::json to_contexts(const perf::AggregatedSamples &samples) const;

    [[nodiscard]] const flounder::Compiler &compiler() const noexcept { return _compiler; }

private:
    flounder::Compiler _compiler;

    [[nodiscard]] execution::compilation::CompilationNode *build(
        execution::compilation::OperatorInterface *compilation_operator, const perf::Counter &profiling_counter,
        const std::shared_ptr<util::Chronometer> &chronometer, bool is_collect_operator_information,
        bool is_collect_memory_traces);
    [[nodiscard]] nlohmann::json to_code(
        bool compiled_code, std::optional<std::reference_wrapper<const perf::AggregatedSamples>> samples) const;
    [[nodiscard]] static perf::CounterDescription to_perf_counter(
        logical::SampleNode::CounterType logical_counter) noexcept;
};
} // namespace db::plan::physical