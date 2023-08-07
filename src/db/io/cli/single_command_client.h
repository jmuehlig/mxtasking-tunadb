#pragma once

#include <cstdint>
#include <mx/tasking/task.h>
#include <optional>
#include <string>

namespace db::io {
class StartSingleCommandClientTask final : public mx::tasking::TaskInterface
{
public:
    StartSingleCommandClientTask(const std::uint16_t port, std::string &&command,
                                 std::optional<std::string> output_file) noexcept
        : _port(port), _command(std::move(command)), _output_file(std::move(output_file))
    {
    }

    ~StartSingleCommandClientTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint16_t _port;
    std::string _command;
    std::optional<std::string> _output_file;
};
} // namespace db::io