#include "planning_task.h"
#include "load_file_task.h"
#include "send_result_task.h"
#include <db/exception/parser_exception.h>
#include <db/network/protocol/server_response.h>
#include <db/parser/sql_parser.h>
#include <db/plan/optimizer/optimizer.h>
#include <db/plan/physical/compilation_graph.h>
#include <db/plan/physical/compilation_plan.h>
#include <db/plan/physical/interpretation_graph.h>
#include <db/storage/serializer.h>
#include <mx/tasking/runtime.h>

using namespace db::io;

mx::tasking::TaskResult PlanningTask::execute(const std::uint16_t worker_id)
{
    try
    {
        auto chronometer = std::make_shared<util::Chronometer>();
        chronometer->start();

        /// From string to abstract syntax tree.
        auto parser = parser::SQLParser{};
        auto ast = parser.parse(std::move(this->_query));
        if (ast == nullptr)
        {
            throw exception::ParserException{"Could not parse query; AST is empty."};
        }
        chronometer->lap(util::Chronometer::Id::Parsing);

        /// From abstract syntax tree to logical plan.
        auto logical_plan = plan::logical::Plan::build(this->_database, std::move(ast));
        chronometer->lap(util::Chronometer::Id::CreatingLogicalPlan);

        /// Stop the server, if wanted.
        if (logical_plan.is_stop()) [[unlikely]]
        {
            mx::tasking::runtime::send_message(this->_client_id, network::ConnectionClosedResponse::to_string());
            return mx::tasking::TaskResult::make_stop(worker_id);
        }

        /// The first node in the plan may the 'LOAD FILE <filename>' statement.
        /// In this case, load the file instead of executing a single plan.
        if (logical_plan.is_load_file()) [[unlikely]]
        {
            auto &file_name =
                reinterpret_cast<plan::logical::LoadFileNode *>(logical_plan.root_node().get())->file_name();
            auto *load_file_task = mx::tasking::runtime::new_task<LoadFileTask>(
                worker_id, this->_client_id, this->_database, this->_configuration, std::move(file_name));
            load_file_task->annotate(worker_id);
            return mx::tasking::TaskResult::make_succeed_and_remove(load_file_task);
        }

        /// The first node in the plan is the '.STORE <filename>' or '.RESTORE <filename>' command.
        /// In this case, (re-)store the whole database in/from a file.
        if (logical_plan.is_store() || logical_plan.is_restore()) [[unlikely]]
        {
            if (logical_plan.is_store())
            {
                auto &file_name =
                    reinterpret_cast<plan::logical::StoreNode *>(logical_plan.root_node().get())->file_name();
                storage::Serializer::serialize(this->_database, file_name);
            }
            else if (logical_plan.is_restore())
            {
                auto &file_name =
                    reinterpret_cast<plan::logical::RestoreNode *>(logical_plan.root_node().get())->file_name();
                storage::Serializer::deserialize(this->_database, file_name);
            }

            if (this->_client_id < std::numeric_limits<std::uint32_t>::max())
            {
                mx::tasking::runtime::send_message(this->_client_id, network::SuccessResponse::to_string());
            }
            return mx::tasking::TaskResult::make_remove();
        }

        /// Handle requests to read or change the configuration.
        if (logical_plan.is_configuration()) [[unlikely]]
        {
            return this->handle_configuration_request(worker_id, std::move(logical_plan));
        }

        /// Perform optimizations on SELECT queries.
        if (logical_plan.is_select_query()) [[likely]]
        {
            auto optimizer = plan::optimizer::ConfigurableOptimizer{this->_database};
            logical_plan = optimizer.optimize(std::move(logical_plan));
            chronometer->lap(util::Chronometer::Id::OptimizingLogicalPlan);

            /// Explains are evaluated directly.
            if (logical_plan.is_explain_plan()) [[unlikely]]
            {
                const auto time = chronometer->microseconds();
                mx::tasking::runtime::send_message(
                    this->_client_id,
                    network::LogicalPlanResponse::to_string(time, logical_plan.to_json(this->_database).dump()));
                return mx::tasking::TaskResult::make_remove();
            }
        }

        plan::physical::DataFlowGraph *dataflow_graph;
        const auto is_explain_performance = logical_plan.is_explain_performance();
        const auto is_explain_task_graph = logical_plan.is_explain_task_graph();
        const auto is_explain_data_flow_graph = logical_plan.is_explain_data_flow_graph();
        const auto is_explain_task_load = logical_plan.is_explain_task_load();
        const auto is_explain_task_traces = logical_plan.is_explain_task_traces();
        const auto is_explain_dram_bandwidth = logical_plan.is_explain_dram_bandwidth();
        const auto is_explain_times = logical_plan.is_explain_times();

        if (is_explain_task_traces) [[unlikely]]
        {
            if constexpr (mx::tasking::config::is_collect_task_traces() == false)
            {
                throw db::exception::ExecutionException{
                    "Collecting Task Traces is disabled. Plase enable first (in src/mx/tasking/config.h)."};
            }
        }

        /// At this point, we have a logical plan, that could be executed in two ways:
        /// (1) Select queries are compiled using flounder xor (2) interpretation for inserts/updates/etc.
        if (logical_plan.is_select_query())
        {
            /// Build an compilation plan, compile that plan and execute the code using task graphs.
            const auto is_explain_flounder = logical_plan.is_explain_flounder();
            const auto is_explain_assembly = logical_plan.is_explain_assembly();

            /// Map logical nodes to physical operators.
            auto compilation_plan = plan::physical::CompilationPlan::build(this->_database, std::move(logical_plan));

            /// Create tags for memory, if recording memory traces.
            if (logical_plan.sample_type().has_value() &&
                std::get<0>(logical_plan.sample_type().value()) == plan::logical::SampleNode::Level::HistoricalMemory)
                [[unlikely]]
            {
                auto plan_memory_tags = compilation_plan.memory_tags();

                //                for (const auto& [name, table] : this->_database.tables())
                //                {
                //                    const auto size = data::PaxTile::size(table.schema()) * config::tuples_per_tile();
                //                    auto tiles = std::vector<std::pair<std::uintptr_t, std::uintptr_t>>{};
                //                    tiles.reserve(table.tiles().size());
                //                    for (const auto tile : table.tiles())
                //                    {
                //                        const auto begin = std::uintptr_t(tile.get());
                //                        tiles.emplace_back(std::make_pair(begin, begin + size));
                //                    }
                //
                //                    plan_memory_tags.insert(std::make_pair(name, std::move(tiles)));
                //                }

                chronometer->add(std::move(plan_memory_tags));
            }

            chronometer->lap(util::Chronometer::Id::CreatingPhysicalPlan);

            /// Build programs from operators. A program could map a full pipeline or
            /// one part of a sequential pipeline, that takes outcome of the preceding
            /// program and emits output to the succeeding program.
            dataflow_graph = plan::physical::CompilationGraph::build(
                this->_database, chronometer, std::move(compilation_plan), this->_client_id, is_explain_performance,
                is_explain_task_load, is_explain_task_traces, is_explain_flounder, is_explain_assembly,
                is_explain_dram_bandwidth, is_explain_task_graph, is_explain_data_flow_graph, is_explain_times,
                logical_plan.sample_type(), this->_database.profiling_counter());
            chronometer->lap(util::Chronometer::Id::GeneratingFlounder);

            auto *compilation_graph = reinterpret_cast<plan::physical::CompilationGraph *>(dataflow_graph);
            if (is_explain_task_graph == false) [[likely]]
            {
                if (is_explain_flounder) [[unlikely]]
                {
                    const auto time = chronometer->microseconds();
                    mx::tasking::runtime::send_message(
                        this->_client_id,
                        network::FlounderCodeResponse::to_string(time, compilation_graph->to_flounder().dump()));
                    delete dataflow_graph;
                    return mx::tasking::TaskResult::make_remove();
                }

                /// Compile flounder graph.
                compilation_graph->compile(config::emit_flounder_code_to_perf(), config::emit_flounder_code_to_vtune());
                chronometer->lap(util::Chronometer::Id::CompilingFlounder);

                /// If the user want the assembly, here you are.
                if (is_explain_assembly) [[unlikely]]
                {
                    const auto time = chronometer->microseconds();
                    mx::tasking::runtime::send_message(
                        this->_client_id,
                        network::AssemblyCodeResponse::to_string(time, compilation_graph->to_assembly().dump()));
                    delete dataflow_graph;
                    return mx::tasking::TaskResult::make_remove();
                }
            }
        }
        else
        {
            if (logical_plan.is_sample()) [[unlikely]]
            {
                throw exception::NotImplementedException{"perf record for interpreted engine"};
            }

            /// Build a task graph that interprets the data and the query.
            dataflow_graph = plan::physical::InterpretationGraph::build(
                this->_database, chronometer, std::move(logical_plan), this->_client_id, is_explain_performance,
                is_explain_task_load, is_explain_task_traces);
            chronometer->lap(util::Chronometer::Id::CreatingPhysicalPlan);
        }

        /// Explain the task graph.
        if (is_explain_task_graph) [[unlikely]]
        {
            const auto time = chronometer->microseconds();
            mx::tasking::runtime::send_message(
                this->_client_id,
                network::TaskGraphResponse::to_string(time, plan::physical::DataFlowGraph::to_dot(dataflow_graph)));
            delete dataflow_graph;
            return mx::tasking::TaskResult::make_remove();
        }

        /// If we want to record the load, start the profiler.
        if (is_explain_task_load) [[unlikely]]
        {
            mx::tasking::runtime::start_idle_profiler();
        }

        /// If we want to record the traces, start the tracer.
        if (is_explain_task_traces) [[unlikely]]
        {
            mx::tasking::runtime::start_tracing();
        }

        /// Start the perf counter and/or perf sample, if any.
        chronometer->start_perf();

        auto *run_query_task =
            mx::tasking::runtime::new_task<RunQueryTask>(worker_id, std::move(chronometer), dataflow_graph);
        return mx::tasking::TaskResult::make_succeed_and_remove(run_query_task);
    }
    catch (std::exception &e)
    {
        if (this->_client_id != std::numeric_limits<std::uint32_t>::max())
        {
            auto *error_task =
                mx::tasking::runtime::new_task<SendErrorTask>(worker_id, this->_client_id, std::string{e.what()});
            error_task->annotate(worker_id);
            return mx::tasking::TaskResult::make_succeed_and_remove(error_task);
        }

        mx::util::Logger::error(e.what());
        return mx::tasking::TaskResult::make_remove();
    }
}

mx::tasking::TaskResult PlanningTask::handle_configuration_request(const std::uint16_t worker_id,
                                                                   plan::logical::Plan &&logical_plan)
{
    const auto &root = logical_plan.root_node();
    if (typeid(*root) == typeid(plan::logical::GetConfigurationNode))
    {
        auto *send_configuration_task =
            mx::tasking::runtime::new_task<SendConfigurationTask>(worker_id, this->_client_id, this->_configuration);
        send_configuration_task->annotate(worker_id);
        return mx::tasking::TaskResult::make_succeed_and_remove(send_configuration_task);
    }

    if (typeid(*root) == typeid(plan::logical::SetCoresNode))
    {
        auto *set_cores_node = reinterpret_cast<plan::logical::SetCoresNode *>(root.get());
        this->_configuration.count_cores(set_cores_node->count_cores());
        mx::tasking::runtime::send_message(this->_client_id, network::SuccessResponse::to_string());
        return mx::tasking::TaskResult::make_stop(worker_id, false);
    }

    throw exception::ExecutionException{"Configuration not implemented."};
}