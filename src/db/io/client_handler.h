#pragma once

#include "task/planning_task.h"
#include <cstdint>
#include <db/network/protocol/server_response.h>
#include <db/topology/configuration.h>
#include <db/topology/database.h>
#include <mx/io/network/server.h>
#include <mx/tasking/runtime.h>
#include <string>

namespace db::io {
class ClientHandler final : public mx::io::network::MessageHandler
{
public:
    constexpr ClientHandler(topology::Database &database, topology::Configuration &configuration) noexcept
        : _database(database), _configuration(configuration)
    {
    }
    ~ClientHandler() noexcept override = default;

    mx::tasking::TaskResult handle(const std::uint16_t worker_id, const std::uint32_t client_id,
                                   std::string &&message) override
    {
        auto *planning_task = mx::tasking::runtime::new_task<PlanningTask>(worker_id, client_id, this->_database,
                                                                           this->_configuration, std::move(message));
        planning_task->annotate(mx::tasking::annotation::execution_destination::local);
        return mx::tasking::TaskResult::make_succeed(planning_task);
    }

private:
    topology::Database &_database;
    topology::Configuration &_configuration;
};
} // namespace db::io