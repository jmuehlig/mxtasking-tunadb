#pragma once
#include <cstdint>
#include <db/io/abstract_client.h>
#include <mx/tasking/task.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace db::io::cli {
class StartBenchmarkTask final : public mx::tasking::TaskInterface
{
public:
    StartBenchmarkTask(const std::uint16_t port, std::string &&query, const std::uint16_t iterations,
                       std::optional<std::string> &&output_file) noexcept
        : _iterations(iterations), _port(port), _query(std::move(query)), _output_file(std::move(output_file))
    {
    }

    ~StartBenchmarkTask() noexcept override = default;

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const std::uint16_t _iterations;
    const std::uint16_t _port;
    const std::string _query;
    const std::optional<std::string> _output_file;
};

class BenchmarkClient final : public AbstractClient
{
public:
    BenchmarkClient(std::string &&server_address, const std::uint16_t port) noexcept
        : AbstractClient(std::move(server_address), port)
    {
    }
    ~BenchmarkClient() noexcept override = default;

    [[nodiscard]] const std::vector<nlohmann::json> &explain_performance_results() const noexcept
    {
        return _explain_performance_results;
    }
    [[nodiscard]] const std::vector<nlohmann::json> &query_results() const noexcept { return _query_results; }

    [[nodiscard]] static std::optional<std::pair<std::vector<std::uint16_t>, nlohmann::json>>
    summarize_explain_performance(const std::vector<nlohmann::json> &results);
    [[nodiscard]] static std::optional<nlohmann::json> summarize_query_results(
        const std::vector<nlohmann::json> &results);
    static void write(const std::vector<nlohmann::json> &results, const std::string &file);

    [[nodiscard]] const std::vector<std::tuple<std::uint16_t, std::chrono::microseconds, nlohmann::json>> &results()
        const noexcept
    {
        return _results;
    }

    [[nodiscard]] std::vector<std::tuple<std::uint16_t, std::chrono::microseconds, nlohmann::json>> &results() noexcept
    {
        return _results;
    }

protected:
    void handle(const network::SuccessResponse * /*response*/) override {}
    void handle(const network::ErrorResponse * /*response*/) override {}
    void handle(const network::ConnectionClosedResponse * /*response*/) override {}
    void handle(const network::GetConfigurationResponse * /*response*/) override {}
    void handle(const network::LogicalPlanResponse * /*response*/) override {}
    void handle(const network::TaskGraphResponse * /*response*/) override {}
    void handle(const network::QueryResultResponse *response) override;
    void handle(const network::PerformanceCounterResponse *response) override;
    void handle(const network::SampleAssemblyResponse * /*response*/) override {}
    void handle(const network::SampleOperatorsResponse * /*response*/) override {}
    void handle(const network::SampleMemoryResponse *response) override;
    void handle(const network::SampleMemoryHistoryResponse * /*response*/) override {}
    void handle(const network::TaskLoadResponse * /*response*/) override {}
    void handle(const network::TaskTraceResponse * /*response*/) override {}
    void handle(const network::FlounderCodeResponse * /*response*/) override {}
    void handle(const network::AssemblyCodeResponse * /*response*/) override {}
    void handle(const network::DRAMBandwidthResponse * /*response*/) override {}
    void handle(const network::DataflowGraphResponse * /*response*/) override {}
    void handle(const network::TimesResponse *response) override;

private:
    std::vector<nlohmann::json> _explain_performance_results;
    std::vector<nlohmann::json> _query_results;

    std::vector<std::tuple<std::uint16_t, std::chrono::microseconds, nlohmann::json>> _results;
};
} // namespace db::io::cli