#pragma once
#include "dataflow_graph.h"
#include <db/execution/operator_interface.h>
#include <db/plan/logical/plan.h>
#include <db/topology/database.h>
#include <db/util/chronometer.h>
#include <memory>
#include <mx/tasking/dataflow/graph.h>
#include <mx/tasking/runtime.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace db::plan::physical {
class InterpretationGraph
{
public:
    [[nodiscard]] static DataFlowGraph *build(topology::Database &database,
                                              std::shared_ptr<util::Chronometer> chronometer,
                                              logical::Plan &&logical_plan, std::uint32_t client_id,
                                              bool is_record_performance, bool is_record_task_load,
                                              bool is_record_task_traces);

private:
    [[nodiscard]] static execution::OperatorInterface *build(topology::Database &database, DataFlowGraph *graph,
                                                             std::unique_ptr<logical::NodeInterface> &logical_node);

    template <class T> static void register_for_tracing(T *node)
    {
        if constexpr (mx::tasking::config::is_collect_task_traces())
        {
            const auto trace_id = node->trace_id();
            if (trace_id > 0U)
            {
                mx::tasking::runtime::register_task_for_trace(trace_id, node->to_string());
            }
        }
    }
};
} // namespace db::plan::physical