#include "abstract_client.h"
#include <fmt/core.h>
#include <mx/util/logger.h>
#include <regex>

using namespace db::io;

bool AbstractClient::connect()
{
    if (this->_network_client.connect())
    {
        return true;
    }

    mx::util::Logger::error(fmt::format("Connection to server {}:{} failed.", this->_network_client.server_address(),
                                        this->_network_client.port()));
    return false;
}

void AbstractClient::execute(std::string &&query)
{
    if (query.empty())
    {
        return;
    }

    /// Check if the query is a file; if so, the query will
    /// be replaced by the contents of the file.
    static auto exec_regex = std::regex{R"(\.exec ([a-zA-Z0-9_\-\.\/]+))", std::regex::icase};
    auto match = std::smatch{};
    if (std::regex_match(query, match, exec_regex))
    {
        const auto file_name = match[1].str();
        auto filestream = std::ifstream{file_name};
        if (filestream.is_open() == false)
        {
            std::cerr << fmt::format("Can not open file '{}' for execution.", file_name) << std::endl;
            return;
        }

        query.clear();
        query.assign((std::istreambuf_iterator<char>(filestream)), (std::istreambuf_iterator<char>()));
        std::replace(query.begin(), query.end(), '\n', ' ');
        std::replace(query.begin(), query.end(), '\t', ' ');
    }

    /// Send the response to the server.
    const auto response = this->_network_client.send(query);
    const auto *server_response = reinterpret_cast<const network::ServerResponse *>(response.data());

    /// Process the response to the client.
    switch (server_response->type())
    {
    case network::ServerResponse::Type::Success:
        this->handle(reinterpret_cast<const network::SuccessResponse *>(server_response));
        break;
    case network::ServerResponse::Type::Error:
        this->handle(reinterpret_cast<const network::ErrorResponse *>(server_response));
        break;
    case network::ServerResponse::Type::GetConfiguration:
        this->handle(reinterpret_cast<const network::GetConfigurationResponse *>(server_response));
        break;
    case network::ServerResponse::Type::QueryResult:
        this->handle(reinterpret_cast<const network::QueryResultResponse *>(server_response));
        break;
    case network::ServerResponse::Type::LogicalPlan:
        this->handle(reinterpret_cast<const network::LogicalPlanResponse *>(server_response));
        break;
    case network::ServerResponse::Type::TaskGraph:
        this->handle(reinterpret_cast<const network::TaskGraphResponse *>(server_response));
        break;
    case network::ServerResponse::Type::DataflowGraph:
        this->handle(reinterpret_cast<const network::DataflowGraphResponse *>(server_response));
        break;
    case network::ServerResponse::Type::PerformanceCounter:
        this->handle(reinterpret_cast<const network::PerformanceCounterResponse *>(server_response));
        break;
    case network::ServerResponse::Type::TaskLoad:
        this->handle(reinterpret_cast<const network::TaskLoadResponse *>(server_response));
        break;
    case network::ServerResponse::Type::TaskTrace:
        this->handle(reinterpret_cast<const network::TaskTraceResponse *>(server_response));
        break;
    case network::ServerResponse::Type::FlounderCode:
        this->handle(reinterpret_cast<const network::FlounderCodeResponse *>(server_response));
        break;
    case network::ServerResponse::Type::AssemblyCode:
        this->handle(reinterpret_cast<const network::AssemblyCodeResponse *>(server_response));
        break;
    case network::ServerResponse::Type::SampleAssembly:
        this->handle(reinterpret_cast<const network::SampleAssemblyResponse *>(server_response));
        break;
    case network::ServerResponse::Type::SampleOperators:
        this->handle(reinterpret_cast<const network::SampleOperatorsResponse *>(server_response));
        break;
    case network::ServerResponse::Type::SampleMemory:
        this->handle(reinterpret_cast<const network::SampleMemoryResponse *>(server_response));
        break;
    case network::ServerResponse::Type::SampleMemoryHistory:
        this->handle(reinterpret_cast<const network::SampleMemoryHistoryResponse *>(server_response));
        break;
    case network::ServerResponse::Type::ConnectionClosed:
        this->handle(reinterpret_cast<const network::ConnectionClosedResponse *>(server_response));
        break;
    case network::ServerResponse::Type::DRAMBandwidth:
        this->handle(reinterpret_cast<const network::DRAMBandwidthResponse *>(server_response));
        break;
    case network::ServerResponse::Type::Times:
        this->handle(reinterpret_cast<const network::TimesResponse *>(server_response));
        break;
    }
}