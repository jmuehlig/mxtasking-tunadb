#pragma once
#include <db/topology/configuration.h>
#include <db/topology/database.h>
#include <mx/tasking/task.h>
#include <string>

namespace db::io {
class LoadFileTask final : public mx::tasking::TaskInterface
{
public:
    LoadFileTask(const std::uint32_t client_id, topology::Database &database, topology::Configuration &configuration,
                 std::string &&file_name) noexcept
        : _client_id(client_id), _database(database), _configuration(configuration), _file_name(std::move(file_name))
    {
    }

    ~LoadFileTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint32_t _client_id;
    topology::Database &_database;
    topology::Configuration &_configuration;
    std::string _file_name;
};
} // namespace db::io