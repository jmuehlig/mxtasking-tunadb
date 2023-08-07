#include "interpretation_graph.h"
#include <db/execution/gather_result_node.h>
#include <db/execution/interpretation/copy_node.h>
#include <db/execution/interpretation/create_table_node.h>
#include <db/execution/interpretation/deliver_node.h>
#include <db/execution/interpretation/describe_table_node.h>
#include <db/execution/interpretation/insert_node.h>
#include <db/execution/interpretation/show_tables_node.h>
#include <db/execution/interpretation/update_statistics_node.h>
#include <db/plan/logical/node/command_nodes.h>
#include <db/plan/logical/node/copy_node.h>
#include <db/plan/logical/node/create_table_node.h>
#include <db/plan/logical/node/insert_node.h>
#include <db/plan/logical/node/materialize_node.h>

using namespace db::plan::physical;

DataFlowGraph *InterpretationGraph::build(topology::Database &database,
                                          std::shared_ptr<db::util::Chronometer> chronometer,
                                          logical::Plan &&logical_plan, const std::uint32_t client_id,
                                          const bool is_record_performance, const bool is_record_task_load,
                                          const bool is_record_task_traces)
{
    auto *graph = new DataFlowGraph();

    /// Build operators/nodes according to the logical plan.
    auto *last_operator_node = InterpretationGraph::build(database, graph, logical_plan.root_node());

    /// Internal requests (e.g., when starting the system) do not require the result.
    if (client_id == std::numeric_limits<std::uint32_t>::max())
    {
        auto *drying_up_node = new mx::tasking::dataflow::EmptyNode<execution::RecordSet>();
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         drying_up_node);
        return graph;
    }

    /// The user requested performance counter only. Collect performance
    /// results and send them to the user.
    if (is_record_performance)
    {
        auto *gather_performance_node = new execution::GatherPerformanceCounterNode{client_id, std::move(chronometer)};
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         gather_performance_node);
        return graph;
    }

    /// The user requested task load only. Collect task load results
    /// from the runtime and send them to the user.
    if (is_record_task_load)
    {
        auto *gather_task_load_node = new execution::GatherTaskLoadNode{client_id, std::move(chronometer)};
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         gather_task_load_node);
        return graph;
    }

    /// The user requested to trace tasks. Collect tasks times from
    /// the runtime and send them to the user.
    if (is_record_task_traces)
    {
        auto *gather_task_traces_node = new execution::GatherTaskTraceNode{client_id, std::move(chronometer)};
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         gather_task_traces_node);
        return graph;
    }

    /// (Normal) user requests will be answered by the gather result node,
    /// which collects the results and send them to the user.
    auto *gather_results_node =
        new execution::GatherQueryResultNode{client_id, std::move(chronometer), last_operator_node->schema()};
    graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                     gather_results_node);
    return graph;
}

db::execution::OperatorInterface *InterpretationGraph::build(topology::Database &database, DataFlowGraph *graph,
                                                             std::unique_ptr<logical::NodeInterface> &logical_node)
{
    auto *node = logical_node.get();

    if (typeid(*node) == typeid(logical::ExplainNode))
    {
        /// Ignore explain nodes during execution.
        return InterpretationGraph::build(database, graph,
                                          reinterpret_cast<logical::ExplainNode *>(logical_node.get())->child());
    }

    if (typeid(*node) == typeid(logical::MaterializeNode))
    {
        /// Ignore materialization during interpretation.
        return InterpretationGraph::build(database, graph,
                                          reinterpret_cast<logical::MaterializeNode *>(logical_node.get())->child());
    }

    if (typeid(*node) == typeid(logical::CreateTableNode))
    {
        auto *create_table_node = reinterpret_cast<logical::CreateTableNode *>(node);
        return new execution::interpretation::CreateTableNode(database, std::move(create_table_node->table_name()),
                                                              std::move(create_table_node->physical_schema()));
    }

    if (typeid(*node) == typeid(logical::InsertNode))
    {
        auto *insert_node = reinterpret_cast<logical::InsertNode *>(node);
        auto &table = database[insert_node->table_name()];

        auto *deliver = new execution::interpretation::DeliverNode(
            topology::PhysicalSchema{table.schema()},
            execution::interpretation::DeliverNode::build_column_indices(table, insert_node->column_names()),
            std::move(insert_node->value_lists()));

        auto *insert = new execution::interpretation::InsertNode(table);
        graph->make_edge(deliver, insert);

        return insert;
    }

    if (typeid(*node) == typeid(logical::ShowTablesNode))
    {
        return new execution::interpretation::ShowTablesNode{database};
    }

    if (typeid(*node) == typeid(logical::DescribeTableNode))
    {
        const auto &table_name = reinterpret_cast<logical::DescribeTableNode *>(node)->table_name();
        return new execution::interpretation::DescribeTableNode{database[table_name]};
    }

    if (typeid(*node) == typeid(logical::UpdateStatisticsNode))
    {
        const auto &table_name = reinterpret_cast<logical::UpdateStatisticsNode *>(node)->table_name();
        return new execution::interpretation::UpdateStatisticsNode{database[table_name]};
    }

    if (typeid(*node) == typeid(logical::CopyNode))
    {
        auto *copy_node = reinterpret_cast<logical::CopyNode *>(node);
        auto &table = database[copy_node->table_name()];

        auto *csv_node =
            new execution::interpretation::CopyNode(topology::PhysicalSchema{table.schema()},
                                                    std::move(copy_node->file_name()), copy_node->separator().front());

        auto *insert_node = new execution::interpretation::InsertNode(table);
        graph->make_edge(csv_node, insert_node);

        return insert_node;
    }

    throw exception::ExecutionException{
        "Could not create physical plan from logical plan. Missing logical node to physical operator transformation."};
}