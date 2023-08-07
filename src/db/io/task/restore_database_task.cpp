#include "restore_database_task.h"
#include "planning_task.h"
#include <db/network/protocol/server_response.h>
#include <fmt/core.h>
#include <mx/tasking/runtime.h>

using namespace db::io;

mx::tasking::TaskResult RestoreDatabaseTask::execute(const std::uint16_t worker_id)
{
    auto *planning_task =
        mx::tasking::runtime::new_task<PlanningTask>(worker_id, this->_client_id, this->_database, this->_configuration,
                                                     fmt::format(".restore '{}';", std::move(this->_file_name)));
    planning_task->annotate(worker_id);

    return mx::tasking::TaskResult::make_succeed_and_remove(planning_task);
}