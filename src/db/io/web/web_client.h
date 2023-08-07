#pragma once
#include <cstdint>
#include <db/io/abstract_client.h>
#include <db/network/client.h>
#include <httplib.h>
#include <mx/tasking/task.h>
#include <string>

namespace db::io {
class StartWebServerTask final : public mx::tasking::TaskInterface
{
public:
    StartWebServerTask(std::string &&server_address, const std::uint16_t server_port,
                       const std::uint16_t web_port) noexcept
        : _server_address(std::move(server_address)), _server_port(server_port), _web_port(web_port)
    {
    }

    ~StartWebServerTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    std::string _server_address;
    const std::uint16_t _server_port;
    const std::uint16_t _web_port;
};

class WebServer
{
public:
    WebServer(std::string &&server_address, const std::uint16_t server_port, const std::uint16_t web_port) noexcept
        : _server_address(std::move(server_address)), _server_port(server_port), _web_port(web_port)
    {
        /// Just one thread for the webserver.
        _web_server.new_task_queue = [] { return new httplib::ThreadPool(1U); };
    }
    ~WebServer() noexcept = default;

    void listen();

private:
    std::string _server_address;
    const std::uint16_t _server_port;
    const std::uint16_t _web_port;
    httplib::Server _web_server;

    [[nodiscard]] static nlohmann::json queries();
};

class WebRequestClient final : public AbstractClient
{
public:
    WebRequestClient(std::string &&address, const std::uint16_t port, httplib::Response &response)
        : AbstractClient(std::move(address), port), _http_response(response)
    {
    }

    ~WebRequestClient() override = default;

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
    void handle(const network::TaskLoadResponse *response) override;
    void handle(const network::TaskTraceResponse *response) override;
    void handle(const network::FlounderCodeResponse *response) override;
    void handle(const network::AssemblyCodeResponse *response) override;
    void handle(const network::SampleOperatorsResponse *response) override;
    void handle(const network::SampleMemoryResponse *response) override;
    void handle(const network::SampleMemoryHistoryResponse *response) override;
    void handle(const network::DRAMBandwidthResponse *response) override;
    void handle(const network::DataflowGraphResponse *response) override;
    void handle(const network::TimesResponse *response) override;

private:
    httplib::Response &_http_response;
};

} // namespace db::io