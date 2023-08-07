#include "load_file_task.h"
#include "planning_task.h"
#include <db/network/protocol/server_response.h>
#include <fmt/core.h>
#include <mx/tasking/runtime.h>

using namespace db::io;

mx::tasking::TaskResult LoadFileTask::execute(const std::uint16_t worker_id)
{
    auto file_stream = std::ifstream{this->_file_name};
    if (file_stream.is_open() == false)
    {
        auto error_message = fmt::format("Can not open file '{}'.", this->_file_name);
        if (this->_client_id < std::numeric_limits<std::uint32_t>::max())
        {
            mx::tasking::runtime::send_message(this->_client_id,
                                               network::ErrorResponse::to_string(std::move(error_message)));
        }
        else
        {
            mx::util::Logger::error(std::move(error_message));
        }
        return mx::tasking::TaskResult::make_remove();
    }

    auto *task_line = mx::tasking::runtime::new_task<mx::tasking::TaskLine>(worker_id);
    std::string line;
    while (std::getline(file_stream, line, ';'))
    {
        line.erase(std::remove(line.begin(), line.end(), '\n'), line.end());
        line.erase(std::remove(line.begin(), line.end(), '\t'), line.end());

        if (line.empty() == false)
        {
            auto *planning_task = mx::tasking::runtime::new_task<PlanningTask>(
                worker_id, this->_client_id, this->_database, this->_configuration, std::move(line));
            planning_task->annotate(worker_id);
            task_line->add(planning_task);
        }
    }

    if (task_line->empty())
    {
        mx::tasking::runtime::delete_task(worker_id, task_line);
        return mx::tasking::TaskResult::make_remove();
    }

    return mx::tasking::TaskResult::make_succeed_and_remove(task_line);
}