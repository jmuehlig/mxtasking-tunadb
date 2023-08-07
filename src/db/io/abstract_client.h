#pragma once
#include <cstdint>
#include <db/network/client.h>
#include <db/network/protocol/server_response.h>
#include <string>

namespace db::io {
class AbstractClient
{
public:
    AbstractClient(std::string &&server_address, const std::uint16_t port)
        : _network_client(std::move(server_address), port)
    {
    }
    virtual ~AbstractClient() = default;

    [[nodiscard]] bool connect();
    [[nodiscard]] const std::string &server_address() const { return _network_client.server_address(); }
    [[nodiscard]] std::uint16_t port() const { return _network_client.port(); }

    void disconnect() { _network_client.disconnect(); }
    void execute(std::string &&query);

protected:
    virtual void handle(const network::SuccessResponse *response) = 0;
    virtual void handle(const network::ErrorResponse *response) = 0;
    virtual void handle(const network::ConnectionClosedResponse *response) = 0;
    virtual void handle(const network::GetConfigurationResponse *response) = 0;
    virtual void handle(const network::LogicalPlanResponse *response) = 0;
    virtual void handle(const network::TaskGraphResponse *response) = 0;
    virtual void handle(const network::QueryResultResponse *response) = 0;
    virtual void handle(const network::PerformanceCounterResponse *response) = 0;
    virtual void handle(const network::SampleAssemblyResponse *response) = 0;
    virtual void handle(const network::SampleOperatorsResponse *response) = 0;
    virtual void handle(const network::SampleMemoryResponse *response) = 0;
    virtual void handle(const network::SampleMemoryHistoryResponse *response) = 0;
    virtual void handle(const network::TaskLoadResponse *response) = 0;
    virtual void handle(const network::TaskTraceResponse *response) = 0;
    virtual void handle(const network::FlounderCodeResponse *response) = 0;
    virtual void handle(const network::AssemblyCodeResponse *response) = 0;
    virtual void handle(const network::DRAMBandwidthResponse *response) = 0;
    virtual void handle(const network::DataflowGraphResponse *response) = 0;
    virtual void handle(const network::TimesResponse *response) = 0;

private:
    network::Client _network_client;
};
} // namespace db::io