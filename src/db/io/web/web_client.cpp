#include "web_client.h"
#include <db/io/cli/serialized_plan.h>
#include <db/network/protocol/server_response.h>
#include <filesystem>
#include <fmt/core.h>
#include <fstream>
#include <mx/system/thread.h>
#include <mx/util/logger.h>
#include <nlohmann/json.hpp>
#include <thread>

using namespace db::io;

mx::tasking::TaskResult StartWebServerTask::execute(const std::uint16_t /*worker_id*/)
{
    mx::util::Logger::info("Starting web client at http://0.0.0.0:" + std::to_string(this->_web_port) + " .");

    auto *web_client_thread = new std::thread{[this] {
        auto web_client = db::io::WebServer{std::move(this->_server_address), this->_server_port, this->_web_port};
        web_client.listen();
    }};
    mx::system::thread::name(*web_client_thread, "db::webclient");
    web_client_thread->detach();

    return mx::tasking::TaskResult::make_remove();
}

void WebServer::listen()
{
    // this->_web_server.set_base_dir("./src/db/io/web");
    this->_web_server.set_mount_point("/", "./src/db/io/web");
    this->_web_server.Post("/query", [&](const httplib::Request &request, httplib::Response &response) {
        auto client = WebRequestClient{std::string{this->_server_address}, this->_server_port, response};

        /// Try to connect to database.
        if (client.connect() == false)
        {
            auto web_response = nlohmann::json{{"type", "error"}};
            web_response["error"] = "Could not connect to database server.";
            response.set_content(web_response.dump(), "text/json");
            return;
        }

        /// Replace newlines and add END identifier, if it does not exist.
        auto input = request.body;
        std::replace(input.begin(), input.end(), '\n', ' ');
        if (input.back() != ';')
        {
            input += ';';
        }

        /// Process the request. During handling the result,
        /// the client will change the http response.
        client.execute(std::move(input));
        client.disconnect();
    });

    this->_web_server.Get("/queries.json", [&](const httplib::Request & /*request*/, httplib::Response &response) {
        response.set_content(WebServer::queries().dump(), "text/json");
    });

    this->_web_server.listen("0.0.0.0", this->_web_port);
}

nlohmann::json WebServer::queries()
{
    auto queries = nlohmann::json{};
    const auto path = std::string{"sql/queries"};

    for (const auto &query_file : std::filesystem::recursive_directory_iterator(path))
    {
        if (query_file.is_regular_file() && query_file.path().string().ends_with(".sql"))
        {
            auto query_file_stream = std::ifstream{query_file.path().string()};
            auto query_name = query_file.path().string().substr(path.size() + 1U);

            auto query = nlohmann::json{};
            query["name"] = std::move(query_name);
            query["query"] =
                std::string((std::istreambuf_iterator<char>(query_file_stream)), (std::istreambuf_iterator<char>()));
            queries.emplace_back(std::move(query));
        }
    }

    std::sort(queries.begin(), queries.end(),
              [](const auto &first, const auto &second) { return first["name"] < second["name"]; });

    return queries;
}

void WebRequestClient::handle(const network::SuccessResponse * /*response*/)
{
    auto web_response = nlohmann::json{{"type", "success"}};
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::ErrorResponse *response)
{
    auto web_response = nlohmann::json{{"type", "error"}, {"error", response->data()}};
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::ConnectionClosedResponse * /*response*/)
{
    auto web_response = nlohmann::json{{"type", "connection-closed"}};
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::GetConfigurationResponse *response)
{
    auto web_response = nlohmann::json::parse(response->data());
    web_response["type"] = "config";
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::LogicalPlanResponse *response)
{
    const auto serialized_plan = SerializedPlan{nlohmann::json::parse(response->data())};

    auto web_response = nlohmann::json{{"type", "plan"}};
    web_response["dot"] = serialized_plan.to_dot();
    web_response["ms"] = fmt::format("{:.3f}", response->time().count() / 1000.0);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::TaskGraphResponse *response)
{
    auto web_response = nlohmann::json{{"type", "task-graph"}};
    web_response["dot"] = response->data();
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::QueryResultResponse *response)
{
    const auto query_result = QueryResult::deserialize(response->data());

    auto web_response = nlohmann::json{{"type", "data"}};
    web_response["result"] = query_result.to_json();
    web_response["count-rows"] = response->count_rows();
    web_response["ms"] = fmt::format("{:.3f}", response->time().count() / 1000.0);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::PerformanceCounterResponse *response)
{
    auto web_response = nlohmann::json{{"type", "performance"}};
    web_response["data"] = nlohmann::json::parse(response->data());
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::SampleAssemblyResponse *response)
{
    auto web_response = nlohmann::json{{"type", "sampled-assembly"}};
    web_response["sampled_programs"] = nlohmann::json::parse(response->data());
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    web_response["count-rows"] = response->count_rows();
    web_response["count-samples"] = response->count_samples();
    web_response["percentage"] = response->percentage();
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::SampleOperatorsResponse *response)
{
    auto web_response = nlohmann::json{{"type", "sampled-operators"}};
    web_response["sampled_operators"] = nlohmann::json::parse(response->data());
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    web_response["count-rows"] = response->count_rows();
    web_response["count-samples"] = response->count_samples();
    web_response["percentage"] = response->percentage();
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::SampleMemoryResponse *response)
{
    auto web_response = nlohmann::json{{"type", "sampled-memory"}};
    web_response["sampled_tiles"] = nlohmann::json::parse(response->data());
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    web_response["count-rows"] = response->count_records().value_or(0U);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::SampleMemoryHistoryResponse *response)
{
    auto web_response = nlohmann::json{{"type", "sampled-memory-history"}};
    web_response["samples"] = nlohmann::json::parse(response->data());
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    web_response["count-rows"] = response->count_records().value_or(0U);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::TaskLoadResponse *response)
{
    auto web_response = nlohmann::json{{"type", "task-load"}};
    web_response["count-rows"] = response->count_rows();
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    auto data = std::string{reinterpret_cast<const char *>(response->data())};
    web_response["channel-frames"] = nlohmann::json::parse(data);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::TaskTraceResponse *response)
{
    auto web_response = nlohmann::json{{"type", "task-trace"}};
    web_response["count-rows"] = response->count_rows();
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    auto data = std::string{reinterpret_cast<const char *>(response->data())};
    web_response["traces"] = nlohmann::json::parse(data);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::FlounderCodeResponse *response)
{
    auto web_response = nlohmann::json{{"type", "flounder-code"}};
    web_response["programs"] = nlohmann::json::parse(response->data());
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::AssemblyCodeResponse *response)
{
    auto web_response = nlohmann::json{{"type", "assembly-code"}};
    web_response["programs"] = nlohmann::json::parse(response->data());
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::DRAMBandwidthResponse *response)
{
    auto web_response = nlohmann::json{{"type", "dram-bandwidth"}};
    web_response["data"] = nlohmann::json::parse(response->data());
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    web_response["count-rows"] = response->count_records().value_or(0ULL);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::DataflowGraphResponse *response)
{
    auto web_response = nlohmann::json{{"type", "data-flow-graph"}};
    web_response["dot"] = response->data();
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    web_response["count-rows"] = response->count_records().value_or(0ULL);
    this->_http_response.set_content(web_response.dump(), "text/json");
}

void WebRequestClient::handle(const network::TimesResponse *response)
{
    auto web_response = nlohmann::json{{"type", "times"}};
    web_response["data"] = nlohmann::json::parse(response->data());
    web_response["count-rows"] = response->count_records().value_or(0ULL);
    web_response["ms"] = fmt::format("{0:.3f}", response->time().count() / 1000.0);
    this->_http_response.set_content(web_response.dump(), "text/json");
}