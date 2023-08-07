#include "client_console.h"
#include "command_line_interface.h"
#include "serialized_plan.h"
#include <db/config.h>
#include <fmt/core.h>
#include <iostream>
#include <mx/system/thread.h>
#include <optional>
#include <regex>
#include <string>
#include <thread>

using namespace db::io;

mx::tasking::TaskResult StartClientConsoleTask::execute(const std::uint16_t /*worker_id*/)
{
    auto *client_thread = new std::thread{[this] {
        auto console = db::io::ClientConsole{std::move(this->_server_address), this->_port};
        if (console.connect() == false)
        {
            return;
        }
        mx::util::Logger::info(
            fmt::format("Connected to tunadb::server {}:{} .", console.server_address(), console.port()));

        console.listen();
    }};
    mx::system::thread::name(*client_thread, "db::cli");
    client_thread->detach();

    return mx::tasking::TaskResult::make_remove();
}

void ClientConsole::handle(const network::SuccessResponse * /*response*/)
{
    std::cout << std::flush;
}

void ClientConsole::handle(const network::ErrorResponse *response)
{
    std::cerr << reinterpret_cast<const network::ErrorResponse *>(response)->data() << std::endl;
}

void ClientConsole::handle(const network::ConnectionClosedResponse * /*response*/)
{
    std::cout << "Connection closed by server." << std::endl;
    this->disconnect();
    this->_is_running = false;
}

void ClientConsole::handle(const network::GetConfigurationResponse *response)
{
    auto configuration_json = nlohmann::json::parse(response->data());
    auto table = util::TextTable{{"Configuration", "Value"}};

    table.emplace_back({"Number of cores", fmt::format(" {}", configuration_json["cores"].get<std::uint16_t>())});

    std::cout << table << std::flush;
}

void ClientConsole::handle(const network::LogicalPlanResponse *response)
{
    const auto serialized_plan = SerializedPlan{nlohmann::json::parse(response->data())};
    std::cout << serialized_plan.to_string() << "Created query plan in \033[1;33m"
              << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
}

void ClientConsole::handle(const network::TaskGraphResponse *response)
{
    std::cout << response->data() << "Created task graph in \033[1;33m"
              << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;

    auto output_file = this->_output_file.value_or("task-graph.dot");
    std::ofstream dot_file{output_file};
    dot_file << response->data() << std::flush;
    std::cout << fmt::format("Wrote task graph to '{}'.", output_file) << std::endl;
}

void ClientConsole::handle(const network::QueryResultResponse *response)
{
    const auto query_result = QueryResult::deserialize(response->data());
    std::cout << query_result.to_string() << "Fetched \033[1;32m" << response->count_rows() << "\033[0m row"
              << (response->count_rows() == 1U ? "" : "s") << " in \033[1;33m"
              << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
}

void ClientConsole::handle(const network::PerformanceCounterResponse *response)
{
    auto text_table =
        util::TextTable::from_json({"Item", "Result"}, {"name", "result"}, nlohmann::json::parse(response->data()));

    std::cout << text_table << std::flush;
}

void ClientConsole::handle(const network::SampleAssemblyResponse *response)
{
    ClientConsole::print_perf_sample(std::cout, response->data());
    std::cout << "Recorded \033[1;32m" << response->count_samples() << "\033[0m samples ("
              << fmt::format("{:.2f}", response->percentage()) << "% in compiled code).\n";
    std::cout << "Fetched \033[1;32m" << response->count_rows() << "\033[0m row"
              << (response->count_rows() == 1U ? "" : "s") << " in \033[1;33m"
              << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
}

void ClientConsole::handle(const network::SampleOperatorsResponse *response)
{
    std::cerr << "TODO: Print operators" << std::endl;

    std::cout << "Recorded \033[1;32m" << response->count_samples() << "\033[0m samples ("
              << fmt::format("{:.2f}", response->percentage()) << "% in compiled code).\n";
    std::cout << "Fetched \033[1;32m" << response->count_rows() << "\033[0m row"
              << (response->count_rows() == 1U ? "" : "s") << " in \033[1;33m"
              << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
}

void ClientConsole::handle(const network::SampleMemoryResponse *response)
{
    std::cerr << "TODO: Print addresses" << std::endl;

    auto samples = nlohmann::json::parse(response->data());
    std::cout << samples.dump(2) << std::endl;
    std::cout << "Fetched \033[1;32m" << response->count_records().value_or(0U) << "\033[0m row"
              << (response->count_records().value_or(0U) == 1U ? "" : "s") << " in \033[1;33m"
              << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
}

void ClientConsole::handle(const network::SampleMemoryHistoryResponse *response)
{
    std::cerr << "TODO: Print addresses" << std::endl;

    auto samples = nlohmann::json::parse(response->data());
    std::cout << "Fetched \033[1;32m" << response->count_records().value_or(0U) << "\033[0m row"
              << (response->count_records().value_or(0U) == 1U ? "" : "s") << " in \033[1;33m"
              << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
    auto output_file = this->_output_file.value_or("memory-traces.json");
    std::ofstream dot_file{output_file};
    dot_file << response->data() << std::flush;
    std::cout << fmt::format("Wrote {} traces to '{}'.", samples.size(), output_file) << std::endl;
}

void ClientConsole::handle(const network::TaskLoadResponse * /*response*/)
{
    std::cout << "Can not handle task load." << std::endl;
}

void ClientConsole::handle(const network::TaskTraceResponse *response)
{
    auto data = std::string{reinterpret_cast<const char *>(response->data())};
    auto task_traces = nlohmann::json::parse(data);

    if (this->_output_file.has_value())
    {
        auto out_stream = std::ofstream{this->_output_file.value()};
        out_stream << task_traces.dump() << std::flush;
        std::cout << fmt::format("Wrote task trace to '{}'.", this->_output_file.value()) << std::endl;
    }
    else
    {
        std::cout << task_traces.dump() << std::endl;
    }

    std::cout << "Fetched \033[1;32m" << response->count_rows() << "\033[0m row"
              << (response->count_rows() == 1U ? "" : "s") << " in \033[1;33m"
              << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
}

void ClientConsole::handle(const network::FlounderCodeResponse *response)
{
    ClientConsole::print_programs(std::cout, response->data());
    std::cout << "Generated flounder in "
              << " in \033[1;33m" << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
}

void ClientConsole::handle(const network::AssemblyCodeResponse *response)
{
    ClientConsole::print_programs(std::cout, response->data());
    std::cout << "Generated assembly in "
              << "\033[1;33m" << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
}

void ClientConsole::handle(const network::DRAMBandwidthResponse *response)
{
    std::cerr << reinterpret_cast<const network::DRAMBandwidthResponse *>(response)->data() << std::endl;
}

void ClientConsole::handle(const network::DataflowGraphResponse * /*response*/)
{
    std::cout << "Can not handle data flow graph response." << std::endl;
}

void ClientConsole::handle(const network::TimesResponse *response)
{
    auto text_table =
        util::TextTable::from_json({"Node", "Time (ms)"}, {"node", "time"}, nlohmann::json::parse(response->data()));

    std::cout << text_table << std::flush;

    const auto count_records = response->count_records().value_or(0UL);
    std::cout << "Fetched \033[1;32m" << count_records << "\033[0m row" << (count_records == 1U ? "" : "s")
              << " in \033[1;33m" << fmt::format("{:.3f}", response->time().count() / 1000.0) << "\033[0m ms.\n"
              << std::flush;
}

void ClientConsole::listen()
{
    const auto quit_regex = std::regex{"q|quit", std::regex_constants::icase};
    static auto help_regex = std::regex{"h|help", std::regex::icase};

    std::cout << "Type 'q' or 'quit' to exit.\n"
              << "Type 'h' or 'help' to show available commands.\n"
              << std::flush;

    auto command_line = CommandLineInterface{".client_history.txt", std::string{db::config::name()} + ">"};
    std::optional<std::string> user_input;
    while ((user_input = command_line.next()) != std::nullopt && this->_is_running)
    {
        std::smatch match;

        if (std::regex_match(user_input.value(), match, quit_regex))
        {
            this->disconnect();
            break;
        }

        if (std::regex_match(user_input.value(), match, help_regex))
        {
            std::cout
                << "Type a query or one of the following commands.\n"
                << "    .help                 Shows this information.\n"
                << "    .exec <file>          Execute all commands and queries from the given file.\n"
                << "    .stop                 Shutdown the server.\n"
                << "    .tables               List all tables.\n"
                << "    .table <name>         List all columns of a specific table.\n"
                << "    .config               Show the configuration of the system.\n"
                << "    .set cores <count>    Use <count> cores for query execution.\n"
                << "    <query>               Executes a query.\n"
                << "    compile <query>       Compiles the given query using flounder and executes it.\n"
                << "    explain <query>       Shows the logical plan of a query.\n"
                << "    explain task graph <query>\n"
                << "                          Shows the logical plan of a query.\n"
                << "    explain flounder <query>\n"
                << "                          Shows the generated flounder code for the specified query.\n"
                << "    explain asm <query>   Shows the generated assembly for the specified query.\n"
                << "    explain performance [compile] <query>\n"
                << "                          Executes the query and shows the performance.\n"
                << "    sample <counter> compile <query>\n"
                << "                          Records hardware events by the given counter and samples\n"
                << "                          instructions. The jitted assembly code will be shown with\n"
                << "                          percentage of samples recorded. <counter> can be one of\n"
                << "                          'branches', 'cycles', 'instructions', 'cache misses', 'cache "
                   "references',\n"
                << "                          or 'stalls mem any'. Example: sample cycles compile select count(*) from "
                   "lineitem\n"
                << std::flush;
            break;
        }

        this->execute(std::move(user_input.value()));
    }

    std::cout << "Client closed. Server may still run." << std::endl;
}

void ClientConsole::print_programs(std::ostream &out_stream, std::string_view programs_data)
{
    auto programs = nlohmann::json::parse(programs_data);
    for (auto &program : programs)
    {
        if (program.contains("code"))
        {
            for (auto &[name, code] : program["code"].items())
            {
                auto table = util::TextTable{};
                table.header({fmt::format("{}::{}()", program["name"].get<std::string>(), std::move(name))});

                for (auto &line : code)
                {
                    table.emplace_back({std::move(line)});
                }
                out_stream << table;
            }
        }
    }

    out_stream << std::flush;
}

void ClientConsole::print_perf_sample(std::ostream &out_stream, std::string_view programs_data)
{
    auto programs = nlohmann::json::parse(programs_data);
    for (auto &program : programs)
    {
        if (program.contains("code"))
        {
            auto table = util::TextTable{};
            table.header({fmt::format("{:.2f}%", program["percentage"].get<double>()),
                          fmt::format("{}()", program["name"].get<std::string>())});

            for (auto &line : program["code"])
            {
                auto percentage = std::string{" "};
                if (line["percentage"].get<double>() > .0)
                {
                    percentage = fmt::format("{:.2f}%", line["percentage"].get<double>());
                }

                table.emplace_back({std::move(percentage), line["instruction"].get<std::string>()});
            }

            out_stream << table;
        }
    }

    out_stream << std::flush;
}
