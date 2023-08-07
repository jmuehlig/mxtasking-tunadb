#include "send_result_task.h"
#include <db/config.h>
#include <db/network/protocol/server_response.h>
#include <fmt/core.h>
#include <mx/system/environment.h>
#include <mx/tasking/config.h>
#include <mx/tasking/runtime.h>

using namespace db::io;

mx::tasking::TaskResult SendQueryResultTask::execute(const std::uint16_t /*worker_id*/)
{
    if (this->_result == nullptr || this->_result->schema().empty())
    {
        mx::tasking::runtime::send_message(this->_client_id, network::SuccessResponse::to_string());
    }
    else
    {
        mx::tasking::runtime::send_message(
            this->_client_id,
            network::QueryResultResponse::to_string(_time, this->_result->count_records(), std::move(*this->_result)));
    }

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendErrorTask::execute(const std::uint16_t /*worker_id*/)
{
    mx::tasking::runtime::send_message(this->_client_id, network::ErrorResponse::to_string(std::move(this->_error)));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendPerformanceCounterTask::execute(const std::uint16_t /*worker_id*/)
{
    nlohmann::json performance_result;

    /// Result of the query.
    performance_result.emplace_back(nlohmann::json{{"name", "Fetched Rows"}, {"result", this->_count_records}});

    /// Release / Debug Mode.
    if constexpr (mx::system::Environment::is_debug())
    {
        performance_result.emplace_back(nlohmann::json{{"name", "Build Type"}, {"result", "Debug"}});
    }
    else
    {
        performance_result.emplace_back(nlohmann::json{{"name", "Build Type"}, {"result", "Release"}});
    }

    /// Number of cores and prefetch distance.
    performance_result.emplace_back(
        nlohmann::json{{"name", "Utilized Cores"}, {"result", mx::tasking::runtime::workers()}});

    const auto prefetch_distance = mx::tasking::runtime::prefetch_distance();
    auto prefetch_distance_string = std::string{"Disabled"};
    if (prefetch_distance.is_automatic())
    {
        prefetch_distance_string = "Automatic";
    }
    else if (prefetch_distance.fixed_distance() > 0U)
    {
        prefetch_distance_string = fmt::format("{}", prefetch_distance.fixed_distance());
    }
    performance_result.emplace_back(
        nlohmann::json{{"name", "Prefetch Distance"}, {"result", std::move(prefetch_distance_string)}});
    performance_result.emplace_back(
        nlohmann::json{{"name", "Prefetch Iterations"}, {"result", config::prefetch_iterations()}});

    performance_result.emplace_back(
        nlohmann::json{{"name", "Resource Aware HT-Dispatching"},
                       {"result", mx::tasking::config::is_consider_resource_bound_workers() ? "Enabled" : "Disabled"}});

    /// Tile configuration.
    performance_result.emplace_back(nlohmann::json{{"name", "Tuples / Tile"}, {"result", config::tuples_per_tile()}});

    /// Times.
    performance_result.emplace_back(
        nlohmann::json{{"name", "Parsing (ms)"}, {"result", this->time(util::Chronometer::Id::Parsing)}});

    performance_result.emplace_back(nlohmann::json{{"name", "Building logical Plan (ms)"},
                                                   {"result", this->time(util::Chronometer::Id::CreatingLogicalPlan)}});

    performance_result.emplace_back(nlohmann::json{{"name", "Optimizing logical Plan (ms)"},
                                                   {"result", this->time(util::Chronometer::OptimizingLogicalPlan)}});

    if (this->_performance_result->has_result(util::Chronometer::Id::CreatingPhysicalPlan))
    {
        performance_result.emplace_back(nlohmann::json{
            {"name", "Building physical Plan (ms)"}, {"result", this->time(util::Chronometer::CreatingPhysicalPlan)}});
    }

    if (this->_performance_result->has_result(util::Chronometer::Id::GeneratingFlounder))
    {
        performance_result.emplace_back(nlohmann::json{{"name", "Generating Flounder Code (ms)"},
                                                       {"result", this->time(util::Chronometer::GeneratingFlounder)}});
    }

    if (this->_performance_result->has_result(util::Chronometer::Id::CompilingFlounder))
    {
        performance_result.emplace_back(nlohmann::json{{"name", "Compiling Flounder (ms)"},
                                                       {"result", this->time(util::Chronometer::CompilingFlounder)}});
    }

    performance_result.emplace_back(
        nlohmann::json{{"name", "Executing (ms)"}, {"result", this->time(util::Chronometer::Executing)}});

    performance_result.emplace_back(nlohmann::json{
        {"name", "Total Time (ms)"}, {"result", this->_performance_result->microseconds().count() / 1000.0}});

    /// Performance Counter.
    for (const auto &[name, value] :
         this->_performance_result->result(util::Chronometer::Id::Executing).performance_counter())
    {
        performance_result.emplace_back(
            nlohmann::json{{"name", fmt::format("Perf. Counter '{}'", name)}, {"result", value}});
    }

    /// Executed Tasks.
    if constexpr (mx::tasking::config::is_use_task_counter())
    {
        for (auto worker_id = 0U;
             worker_id < this->_performance_result->result(util::Chronometer::Id::Executing).task_counter().size();
             ++worker_id)
        {
            performance_result.emplace_back(nlohmann::json{
                {"name", fmt::format("Executed Tasks (Worker {})", worker_id)},
                {"result",
                 this->_performance_result->result(util::Chronometer::Id::Executing).task_counter()[worker_id]}});
        }
        performance_result.emplace_back(nlohmann::json{
            {"name", "Executed Tasks (total)"},
            {"result", this->_performance_result->result(util::Chronometer::Id::Executing).task_counter().sum()}});
    }
    else
    {
        performance_result.emplace_back(nlohmann::json{{"name", "Executed Tasks (total)"}, {"result", "Disabled"}});
    }

    mx::tasking::runtime::send_message(this->_client_id, network::PerformanceCounterResponse::to_string(
                                                             this->_performance_result->microseconds(),
                                                             this->_count_records, performance_result.dump()));

    return mx::tasking::TaskResult::make_remove();
}

float SendPerformanceCounterTask::time(const util::Chronometer::Id lap_id) const
{
    return this->_performance_result->result(lap_id).microseconds().count() / 1000.0;
}

mx::tasking::TaskResult SendSampleAssemblyTask::execute(const std::uint16_t /*worker_id*/)
{
    const auto microseconds = this->_chronometer->microseconds();
    const auto &samples = this->_chronometer->result(util::Chronometer::Id::Executing).performance_aggregated_samples();
    if (samples.has_value() == false || this->_programs.empty())
    {
        mx::tasking::runtime::send_message(this->_client_id,
                                           network::ErrorResponse::to_string(std::string{"Sampling failed."}));
        return mx::tasking::TaskResult::make_remove();
    }

    auto count = std::uint64_t(0U);
    auto percentage = float{.0};
    for (const auto &program : this->_programs)
    {
        if (program.contains("code"))
        {
            for (const auto &item : program["code"])
            {
                if (item.contains("percentage"))
                {
                    percentage += item["percentage"].get<float>();
                }
            }
        }
    }

    mx::tasking::runtime::send_message(this->_client_id, network::SampleAssemblyResponse::to_string(
                                                             microseconds, this->_count_records, samples->count(),
                                                             percentage, this->_programs.dump()));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendSampleOperatorsTask::execute(const std::uint16_t /*worker_id*/)
{
    const auto microseconds = this->_chronometer->microseconds();
    const auto &samples = this->_chronometer->result(util::Chronometer::Id::Executing).performance_aggregated_samples();
    if (samples.has_value() == false || this->_programs.empty())
    {
        mx::tasking::runtime::send_message(this->_client_id,
                                           network::ErrorResponse::to_string(std::string{"Sampling failed."}));
        return mx::tasking::TaskResult::make_remove();
    }

    auto percentage = float{.0};
    for (const auto &program : this->_programs)
    {
        if (program.contains("contexts"))
        {
            for (const auto &item : program["contexts"])
            {
                if (item.contains("percentage"))
                {
                    percentage += item["percentage"].get<float>();
                }
            }
        }
    }

    mx::tasking::runtime::send_message(this->_client_id, network::SampleOperatorsResponse ::to_string(
                                                             microseconds, this->_count_records, samples->count(),
                                                             percentage, this->_programs.dump()));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendSampleMemoryTask::execute(const std::uint16_t /*worker_id*/)
{
    const auto microseconds = this->_chronometer->microseconds();

    mx::tasking::runtime::send_message(
        this->_client_id,
        network::SampleMemoryResponse::to_string(microseconds, this->_count_records, this->_samples.dump()));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendSampleMemoryHistoryTask::execute(const std::uint16_t /*worker_id*/)
{
    const auto microseconds = this->_chronometer->microseconds();

    mx::tasking::runtime::send_message(
        this->_client_id,
        network::SampleMemoryHistoryResponse::to_string(microseconds, this->_count_records, this->_samples.dump()));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendTaskLoadTask::execute(const std::uint16_t /*worker_id*/)
{
    mx::tasking::runtime::send_message(
        this->_client_id,
        network::TaskLoadResponse::to_string(this->_time, this->_count_records, std::move(*this->_worker_idle_frames)));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendTaskTraceTask::execute(const std::uint16_t /*worker_id*/)
{
    mx::tasking::runtime::send_message(
        this->_client_id,
        network::TaskTraceResponse::to_string(this->_time, this->_count_records, std::move(*this->_task_traces)));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendConfigurationTask::execute(const std::uint16_t /*worker_id*/)
{
    auto configuration = nlohmann::json{};
    configuration["cores"] = this->_configuration.count_cores();
    configuration["cores-available"] = mx::system::cpu::count_cores();

    mx::tasking::runtime::send_message(this->_client_id,
                                       network::GetConfigurationResponse::to_string(configuration.dump()));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendMemoryBandwithTask::execute(const std::uint16_t /*worker_id*/)
{
    auto bandwidth = nlohmann::json{};
    for (const auto &sample : this->_samples)
    {
        auto sample_json = nlohmann::json{};
        sample_json["timestamp"] = sample.timestamp();
        sample_json["read_gb_s"] = sample.read_gb_per_second();
        sample_json["write_gb_s"] = sample.write_gb_per_second();
        sample_json["gb_s"] = sample.gb_per_second();
        bandwidth["bandwidth"].emplace_back(std::move(sample_json));
    }

    for (auto &event : this->_events)
    {
        auto event_json = nlohmann::json{};
        event_json["timestamp"] = std::get<0>(event);
        event_json["name"] = std::move(std::get<1>(event));
        bandwidth["events"].emplace_back(std::move(event_json));
    }

    mx::tasking::runtime::send_message(this->_client_id, network::DRAMBandwidthResponse::to_string(
                                                             this->_time, this->_count_records, bandwidth.dump()));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendDataFlowGraphTask::execute(const std::uint16_t /*worker_id*/)
{
    mx::tasking::runtime::send_message(this->_client_id, network::DataflowGraphResponse::to_string(
                                                             this->_time, this->_count_records, std::move(this->_dot)));

    return mx::tasking::TaskResult::make_remove();
}

mx::tasking::TaskResult SendTimesTask::execute(const std::uint16_t /*worker_id*/)
{
    auto times = nlohmann::json{};

    for (auto &[node, time] : this->_node_times)
    {
        times.emplace_back(nlohmann::json{{"node", std::move(node)}, {"time", time / 1000000.0}});
    }

    mx::tasking::runtime::send_message(
        this->_client_id, network::TimesResponse ::to_string(this->_time, this->_count_records, times.dump()));

    return mx::tasking::TaskResult::make_remove();
}