#include "benchmark_client.h"
#include <db/util/text_table.h>
#include <fstream>
#include <mx/system/thread.h>
#include <spdlog/spdlog.h>

using namespace db::io::cli;

void BenchmarkClient::handle(const network::QueryResultResponse *response)
{
    const auto run_id = this->_results.size();
    this->_results.emplace_back(
        std::make_tuple(run_id, response->time(),
                        nlohmann::json{nlohmann::json{{"name", "time"}, {"result", response->time().count()}},
                                       nlohmann::json{{"name", "rows"}, {"result", response->count_rows()}}}));
}

void BenchmarkClient::handle(const network::PerformanceCounterResponse *response)
{
    const auto run_id = this->_results.size();
    this->_results.emplace_back(run_id, response->time(), nlohmann::json::parse(response->data()));
}

void BenchmarkClient::handle(const network::TimesResponse *response)
{
    const auto run_id = this->_results.size();
    this->_results.emplace_back(run_id, response->time(), nlohmann::json::parse(response->data()));
}

void BenchmarkClient::handle(const network::SampleMemoryResponse *response)
{
    const auto run_id = this->_results.size();
    this->_results.emplace_back(std::make_tuple(
        run_id, response->time(),
        nlohmann::json{nlohmann::json{{"name", "time"}, {"result", response->time().count()}},
                       nlohmann::json{{"name", "samples"}, {"result", nlohmann::json::parse(response->data())}}}));
}

mx::tasking::TaskResult StartBenchmarkTask::execute(const std::uint16_t /*worker_id*/)
{
    auto *benchmark_thread = new std::thread{[port = this->_port, iterations = this->_iterations,
                                              command = this->_query, output_file = this->_output_file] {
        auto client = BenchmarkClient{"localhost", port};
        if (client.connect())
        {
            spdlog::info("Starting Benchmark");
            for (auto i = 0U; i < iterations; ++i)
            {
                spdlog::info("Starting Run {}", i);
                client.execute(std::string{command});
            }
            spdlog::info("Finished Benchmark");
            client.execute(".stop");

            auto results = std::move(client.results());
            if (results.empty() == false)
            {
                /// Write results as json array into file.
                if (output_file.has_value())
                {
                    auto json_results = std::vector<nlohmann::json>{};
                    std::transform(results.begin(), results.end(), std::back_inserter(json_results),
                                   [](const auto &result) { return std::get<2>(result); });
                    BenchmarkClient::write(json_results, output_file.value());
                }

                /// Sort results by total time ascending.
                std::sort(results.begin(), results.end(),
                          [](const auto &left, const auto &right) { return std::get<1>(left) < std::get<1>(right); });

                auto data = nlohmann::json{};

                auto header = std::vector<std::string>{"Item"};
                auto keys = std::vector<std::string>{"item"};
                if (results.size() == 1U)
                {
                    header.emplace_back("Result");
                    keys.emplace_back("run-0");
                }
                else
                {
                    for (const auto &result : results)
                    {
                        header.emplace_back(fmt::format("Run #{}", std::get<0>(result)));
                        keys.emplace_back(fmt::format("run-{}", std::get<0>(result)));
                    }
                }

                auto items = std::vector<std::string>{};
                for (const auto &row : std::get<2>(results.front()))
                {
                    items.emplace_back(row.items().begin().value().get<std::string>());
                }

                for (auto &name : items)
                {
                    auto row = nlohmann::json{{"item", name}};
                    for (const auto &run : results)
                    {
                        const auto run_id = std::get<0>(run);
                        const auto &run_result = std::get<2>(run);

                        for (const auto &item : run_result)
                        {
                            const auto &run_items = item.items();
                            if (/*run_items.begin().value().is_string() &&*/
                                run_items.begin().value().get<std::string>() == name)
                            {
                                auto &val = run_items.begin().operator++().value();

                                if (val.is_array())
                                {
                                    row[fmt::format("run-{}", run_id)] = val.dump();
                                }
                                else
                                {
                                    row[fmt::format("run-{}", run_id)] = val;
                                }

                                break;
                            }
                        }
                    }
                    data.emplace_back(std::move(row));
                }

                auto text_table = util::TextTable::from_json(std::move(header), std::move(keys), std::move(data));
                std::cout << text_table << std::flush;
            }

            //            if (client.query_results().empty() == false)
            //            {
            //                const auto &results = client.query_results();
            //
            //                auto summary = BenchmarkClient::summarize_query_results(results);
            //                if (summary.has_value())
            //                {
            //                    auto text_table =
            //                        util::TextTable::from_json({"Item", "Result"}, {"item", "result"},
            //                        std::move(summary.value()));
            //                    std::cout << text_table << std::flush;
            //                }
            //
            //                /// Write results into file.
            //                if (output_file.has_value())
            //                {
            //                    BenchmarkClient::write(results, output_file.value());
            //                }
            //            }
            //            else if (client.explain_performance_results().empty() == false)
            //            {
            //                const auto &results = client.explain_performance_results();
            //
            //                auto performance = BenchmarkClient::summarize_explain_performance(results);
            //                if (performance.has_value())
            //                {
            //                    auto [run_ids, summary] = std::move(performance.value());
            //
            //                    auto header = std::vector<std::string>{"Item"};
            //                    auto keys = std::vector<std::string>{"name"};
            //
            //                    if (run_ids.size() == 1U)
            //                    {
            //                        header.emplace_back("Result");
            //                        keys.emplace_back("run-0");
            //                    }
            //                    else
            //                    {
            //                        std::transform(run_ids.begin(), run_ids.end(), std::back_inserter(header),
            //                                       [](const auto run_id) { return fmt::format("Run #{}", run_id); });
            //                        std::transform(run_ids.begin(), run_ids.end(), std::back_inserter(keys),
            //                                       [](const auto run_id) { return fmt::format("run-{}", run_id); });
            //                    }
            //
            //                    auto text_table =
            //                        util::TextTable::from_json(std::move(header), std::move(keys),
            //                        std::move(summary));
            //                    std::cout << text_table << std::flush;
            //                }
            //
            //                /// Write results into file.
            //                if (output_file.has_value())
            //                {
            //                    BenchmarkClient::write(results, output_file.value());
            //                }
            //            }
        }
    }};
    mx::system::thread::name(*benchmark_thread, "db::bench_exec");
    benchmark_thread->detach();

    return mx::tasking::TaskResult::make_remove();
}

std::optional<std::pair<std::vector<std::uint16_t>, nlohmann::json>> BenchmarkClient::summarize_explain_performance(
    const std::vector<nlohmann::json> &results)
{
    if (results.empty())
    {
        return std::nullopt;
    }

    /// List of pairs (run id, total time) ordered by total time asc.
    auto ordered_runs = std::vector<std::pair<std::uint16_t, float>>{};
    for (auto run_id = 0U; run_id < results.size(); ++run_id)
    {
        const auto &result = results[run_id];
        for (const auto &item : result)
        {
            if (item.contains("name") && item["name"].get<std::string>() == "Total Time (ms)")
            {
                if (item.contains("result") && item["result"].is_number_float())
                {
                    ordered_runs.emplace_back(run_id, item["result"].get<float>());
                }
            }
        }
        std::sort(ordered_runs.begin(), ordered_runs.end(),
                  [](const auto left, const auto right) { return std::get<1>(left) < std::get<1>(right); });
    }

    auto run_ids = std::vector<std::uint16_t>{};
    std::transform(ordered_runs.begin(), ordered_runs.end(), std::back_inserter(run_ids),
                   [](const auto item) { return std::get<0>(item); });

    auto summary = results.front();

    for (auto &item : summary)
    {
        if (item.contains("result"))
        {
            item.erase("result");
        }

        for (const auto run_id : run_ids)
        {
            for (const auto &run_item : results[run_id])
            {
                if (run_item.contains("name") && run_item["name"] == item["name"])
                {
                    item[fmt::format("run-{}", run_id)] = run_item["result"];
                    break;
                }
            }
        }
    }

    return std::make_optional(std::make_pair(std::move(run_ids), std::move(summary)));
}

std::optional<nlohmann::json> BenchmarkClient::summarize_query_results(const std::vector<nlohmann::json> &results)
{
    if (results.empty())
    {
        return std::nullopt;
    }

    auto count_rows = 0UL;
    auto time_sum = 0UL;
    auto time_min = std::numeric_limits<std::uint64_t>::max();
    auto time_max = 0UL;

    for (const auto &result : results)
    {
        if (result.contains("count_rows") && result.contains("time"))
        {
            const auto time = result["time"].get<std::uint64_t>();
            count_rows += result["count_rows"].get<std::uint64_t>();
            time_sum += time;
            time_min = std::min(time_min, time);
            time_max = std::max(time_max, time);
        }
    }

    auto summary = nlohmann::json{};
    summary.emplace_back(
        nlohmann::json{{"item", "Fetched Rows"}, {"result", std::uint64_t(count_rows / results.size())}});
    summary.emplace_back(nlohmann::json{{"item", "Avg. Time (ms)"}, {"result", time_sum / 1000. / results.size()}});
    summary.emplace_back(nlohmann::json{{"item", "Min. Time (ms)"}, {"result", time_min / 1000.}});
    summary.emplace_back(nlohmann::json{{"item", "Max. Time (ms)"}, {"result", time_max / 1000.}});

    return summary;
}

void BenchmarkClient::write(const std::vector<nlohmann::json> &results, const std::string &file)
{
    auto results_as_json = nlohmann::json{};
    for (const auto &result : results)
    {
        auto new_result = nlohmann::json{};
        for (const auto &item : result)
        {
            auto items = item.items();
            new_result[items.begin().value().get<std::string>()] = items.begin().operator++().value();
        }
        results_as_json.emplace_back(std::move(new_result));
    }
    auto output_stream = std::ofstream{file, std::ios::out | std::ios::trunc};
    output_stream << results_as_json.dump() << std::endl;

    std::cout << "Wrote results to " << file << std::endl;
}