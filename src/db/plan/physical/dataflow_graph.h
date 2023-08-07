#pragma once

#include <db/execution/record_token.h>
#include <mx/tasking/dataflow/graph.h>

namespace db::plan::physical {
class DataFlowGraph : public mx::tasking::dataflow::Graph<execution::RecordSet>
{
public:
    explicit DataFlowGraph(const bool is_record_times = false)
        : mx::tasking::dataflow::Graph<execution::RecordSet>(is_record_times)
    {
    }
    ~DataFlowGraph() override = default;

    [[nodiscard]] static std::string to_dot(DataFlowGraph *graph, bool is_include_emit_count = false);
};
} // namespace db::plan::physical