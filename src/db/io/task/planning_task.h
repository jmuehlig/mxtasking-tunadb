#pragma once
#include <db/plan/logical/plan.h>
#include <db/plan/physical/dataflow_graph.h>
#include <db/topology/configuration.h>
#include <db/topology/database.h>
#include <db/util/chronometer.h>
#include <memory>
#include <mx/tasking/task.h>
#include <string>

namespace db::io {
class PlanningTask final : public mx::tasking::TaskInterface
{
public:
    PlanningTask(const std::uint32_t client_id, topology::Database &database, topology::Configuration &configuration,
                 std::string &&query) noexcept
        : _client_id(client_id), _database(database), _configuration(configuration), _query(std::move(query))
    {
    }

    ~PlanningTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

    [[nodiscard]] std::uint64_t trace_id() const noexcept override { return config::task_id_planning(); }

private:
    const std::uint32_t _client_id;
    topology::Database &_database;
    topology::Configuration &_configuration;
    std::string _query;

    [[nodiscard]] mx::tasking::TaskResult handle_configuration_request(std::uint16_t worker_id,
                                                                       plan::logical::Plan &&logical_plan);
};

class RunQueryTask final : public mx::tasking::TaskInterface
{
public:
    RunQueryTask(std::shared_ptr<util::Chronometer> &&chronometer, plan::physical::DataFlowGraph *task_graph) noexcept
        : _chronometer(std::move(chronometer)), _task_graph(task_graph)
    {
    }

    ~RunQueryTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t worker_id) override
    {
        /// Reset the chronometer to exclude opening perf from time.
        _chronometer->reset();

        /// Execute the physical plan.
        _task_graph->start(worker_id);

        return mx::tasking::TaskResult::make_remove();
    }

    [[nodiscard]] std::uint64_t trace_id() const noexcept override { return config::task_id_planning(); }

private:
    std::shared_ptr<util::Chronometer> _chronometer;
    plan::physical::DataFlowGraph *_task_graph;
};
} // namespace db::io