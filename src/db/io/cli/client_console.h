#pragma once
#include <cstdint>
#include <db/io/abstract_client.h>
#include <mx/tasking/task.h>
#include <string>
#include <string_view>

namespace db::io {
class StartClientConsoleTask final : public mx::tasking::TaskInterface
{
public:
    StartClientConsoleTask(std::string &&server_address, const std::uint16_t port) noexcept
        : _server_address(std::move(server_address)), _port(port)
    {
    }

    ~StartClientConsoleTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    std::string _server_address;
    const std::uint16_t _port;
};

class ClientConsole final : public AbstractClient
{
public:
    ClientConsole(std::string &&server_address, const std::uint16_t port,
                  std::optional<std::string> &&output_file = std::nullopt) noexcept
        : AbstractClient(std::move(server_address), port), _output_file(std::move(output_file))
    {
    }
    ~ClientConsole() noexcept override = default;

    void listen();

protected:
    void handle(const network::SuccessResponse *response) override;
    void handle(const network::ErrorResponse *response) override;
    void handle(const network::ConnectionClosedResponse *response) override;
    void handle(const network::GetConfigurationResponse *response) override;
    void handle(const network::LogicalPlanResponse *response) override;
    void handle(const network::TaskGraphResponse *response) override;
    void handle(const network::QueryResultResponse *response) override;
    void handle(const network::PerformanceCounterResponse *response) override;
    void handle(const network::SampleAssemblyResponse *response) override;
    void handle(const network::SampleOperatorsResponse *response) override;
    void handle(const network::SampleMemoryResponse *response) override;
    void handle(const network::SampleMemoryHistoryResponse *response) override;
    void handle(const network::TaskLoadResponse *response) override;
    void handle(const network::TaskTraceResponse *response) override;
    void handle(const network::FlounderCodeResponse *response) override;
    void handle(const network::AssemblyCodeResponse *response) override;
    void handle(const network::DRAMBandwidthResponse *response) override;
    void handle(const network::DataflowGraphResponse *response) override;
    void handle(const network::TimesResponse *response) override;

private:
    bool _is_running{true};
    std::optional<std::string> _output_file;

    static void print_programs(std::ostream &out_stream, std::string_view programs_data);
    static void print_perf_sample(std::ostream &out_stream, std::string_view programs_data);
};
} // namespace db::io