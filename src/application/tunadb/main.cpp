#include "udf.h"
#include <argparse/argparse.hpp>
#include <cstdint>
#include <db/config.h>
#include <db/io/cli/benchmark_client.h>
#include <db/io/cli/client_console.h>
#include <db/io/cli/single_command_client.h>
#include <db/io/client_handler.h>
#include <db/io/task/load_file_task.h>
#include <db/io/task/restore_database_task.h>
#include <db/io/web/web_client.h>
#include <db/topology/configuration.h>
#include <db/topology/database.h>
#include <db/util/string.h>
#include <filesystem>
#include <fmt/core.h>
#include <iostream>
#include <mx/tasking/runtime.h>
#include <mx/util/logger.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

void boot(db::topology::Database &database, db::topology::Configuration &configuration,
          const std::optional<std::string> &initial_load_file, std::optional<std::string> &&execute_statement,
          std::optional<std::string> &&output_file, std::uint16_t iterations, std::string &&host, std::uint16_t port,
          std::uint16_t web_port, bool is_server_only, bool is_web_client);

std::optional<std::string> parse_execution_statement(const std::optional<std::string> &statement);

int main(const int count_arguments, char **arguments)
{
    /// Setup logger.
    spdlog::set_pattern("[%d.%m.%Y %H:%M:%S.%f] %v");
    auto logger = spdlog::basic_logger_mt("main", "tunadb.log", true);
    spdlog::set_default_logger(std::move(logger));

    spdlog::info("Starting tunadb");

    argparse::ArgumentParser argument_parser{db::config::name(), "0.1.0"};
    argument_parser.add_argument("cores")
        .help("Number of cores used for executing tasks.")
        .default_value(std::uint16_t(1U))
        .action([](const std::string &value) { return std::uint16_t(std::stoi(value)); });
    argument_parser.add_argument("-co", "--core-order")
        .help("How to order cores (numa, smt, system).")
        .default_value(std::string{"numa"})
        .action([](const std::string &value) { return value; });
    argument_parser.add_argument("-pd", "--prefetch-distance")
        .help("How many tasks before should the data be prefetched?")
        .default_value(std::uint8_t(0U))
        .action([](const std::string &value) { return std::uint8_t(std::stoi(value)); });
    argument_parser.add_argument("--prefetch4me")
        .help("Enables automatic prefetching. When set, the fixed prefetch distance will be discarded.")
        .implicit_value(true)
        .default_value(false);
    argument_parser.add_argument("--execute")
        .help("Execute the given query/statement directly and shutdown afterwards. Useful for benchmarking.")
        .default_value(std::optional<std::string>{std::nullopt})
        .action([](const std::string &value) { return std::make_optional(value); });
    argument_parser.add_argument("--output")
        .help("Write the results of 'explain performance ..' queries in JSON format to the given file.")
        .default_value(std::optional<std::string>{std::nullopt})
        .action([](const std::string &value) { return std::make_optional(value); });
    argument_parser.add_argument("-i", "--iterations")
        .help("Execute the given query N times (only available with using an output file where to write the results).")
        .default_value(std::uint16_t(1U))
        .action([](const std::string &value) { return std::uint16_t(std::stoi(value)); });
    argument_parser.add_argument("--load")
        .help("Execute a specific file that loads data into the database on startup. Used only in server mode.")
        .default_value(std::optional<std::string>{std::nullopt})
        .action([](const std::string &value) { return std::make_optional(value); });
    argument_parser.add_argument("-p", "--port")
        .help("Port the server is listen to.")
        .default_value(std::uint16_t(9090))
        .action([](const std::string &value) { return std::uint16_t(std::stoi(value)); });
    argument_parser.add_argument("--server-only")
        .help("Only start the server.")
        .implicit_value(true)
        .default_value(false);
    argument_parser.add_argument("--client-only")
        .help("Only start a client.")
        .implicit_value(true)
        .default_value(false);
    argument_parser.add_argument("--host")
        .help("Host the client should connect to.")
        .default_value(std::string("localhost"));
    argument_parser.add_argument("--web-client").help("Start web client.").implicit_value(true).default_value(false);
    argument_parser.add_argument("--web-port")
        .help("Port of the web client.")
        .default_value(std::uint16_t(9100))
        .action([](const std::string &value) { return std::uint16_t(std::stoi(value)); });

    try
    {
        argument_parser.parse_args(count_arguments, arguments);
    }
    catch (std::runtime_error &e)
    {
        std::cout << argument_parser << std::endl;
        return 1;
    }

    const auto count_cores = argument_parser.get<std::uint16_t>("cores");
    const auto cores_order = argument_parser.get<std::string>("-co");
    auto prefetch_distance = mx::tasking::PrefetchDistance{argument_parser.get<std::uint8_t>("-pd")};
    if (argument_parser.get<bool>("--prefetch4me"))
    {
        prefetch_distance = mx::tasking::PrefetchDistance::make_automatic();
    }
    auto execute_statement = parse_execution_statement(argument_parser.get<std::optional<std::string>>("--execute"));
    auto output_file = argument_parser.get<std::optional<std::string>>("--output");
    const auto iterations = argument_parser.get<std::uint16_t>("-i");
    const auto initial_load_file = argument_parser.get<std::optional<std::string>>("--load");
    const auto port = argument_parser.get<std::uint16_t>("-p");
    const auto is_server_only = argument_parser.get<bool>("--server-only");
    const auto is_client_only = execute_statement.has_value() == false && argument_parser.get<bool>("--client-only");
    const auto is_web_client = execute_statement.has_value() == false && argument_parser.get<bool>("--web-client");

    const auto web_port = argument_parser.get<std::uint16_t>("--web-port");
    auto host = argument_parser.get<std::string>("--host");

    if (is_client_only)
    {
        /// Tasking runtime is needed to allocate tiles for received data.
        auto core_set = mx::util::core_set::build(1U);
        mx::tasking::runtime::init(core_set, mx::tasking::PrefetchDistance{0U}, false);

        auto console = db::io::ClientConsole{std::move(host), port};
        if (console.connect() == false)
        {
            return 1;
        }

        if (execute_statement.has_value())
        {
            console.execute(std::move(execute_statement.value()));
        }

        console.listen();
    }
    else
    {
        auto tuna_db = db::topology::Database{};

        /// Add UDF for tests.
        tuna_db.insert(db::udf::Descriptor{"test",
                                           true,
                                           {std::make_pair("o_totalprice", db::type::Type::make_decimal(16, 2)),
                                            std::make_pair("l_extendedprice", db::type::Type::make_decimal(16, 2))},
                                           db::type::Type::make_decimal(16, 2),
                                           std::uintptr_t(&application::tunadb::UDF::test)});

        auto configuration = db::topology::Configuration{};
        configuration.count_cores(count_cores);

        if (cores_order == "smt")
        {
            configuration.cores_order(mx::util::core_set::Order::Physical);
        }
        else if (cores_order == "system")
        {
            configuration.cores_order(mx::util::core_set::Order::Ascending);
        }

        mx::util::Logger::info(fmt::format("Starting server at port {}.", port));

        auto is_db_booted = false;
        do
        {
            auto cores = mx::util::core_set::build(configuration.count_cores(), configuration.cores_order());
            mx::util::Logger::info(fmt::format("Utilizing {} cores: {}.", cores.count_cores(), cores.to_string()));

            auto runtime = mx::tasking::runtime_guard{true, cores, prefetch_distance};
            if (mx::tasking::config::is_collect_task_traces() ||
                mx::tasking::config::is_monitor_task_cycles_for_prefetching())
            {
                mx::tasking::runtime::register_task_for_trace(db::config::task_id_planning(), "Planning");
                mx::tasking::runtime::register_task_for_trace(db::config::task_id_hash_table_memset(), "Memset HT");
            }

            /// Start the database, if it is not started.
            if (std::exchange(is_db_booted, true) == false)
            {
                boot(tuna_db, configuration, initial_load_file, std::move(execute_statement), std::move(output_file),
                     iterations, std::move(host), port, web_port, is_server_only, is_web_client);
                mx::tasking::runtime::listen_on_port(std::make_unique<db::io::ClientHandler>(tuna_db, configuration),
                                                     port);
            }
            else
            {
                tuna_db.update_core_mapping(cores);
            }

        } while (mx::tasking::runtime::is_listening());
    }
}

void boot(db::topology::Database &database, db::topology::Configuration &configuration,
          const std::optional<std::string> &initial_load_file, std::optional<std::string> &&execute_statement,
          std::optional<std::string> &&output_file, const std::uint16_t iterations, std::string &&host,
          const std::uint16_t port, const std::uint16_t web_port, const bool is_server_only, const bool is_web_client)
{
    /// The task line will execute the initial load file and start client task.
    auto *task_line = mx::tasking::runtime::new_task<mx::tasking::TaskLine>(0U);

    const auto start = std::chrono::steady_clock::now();
    if (initial_load_file.has_value())
    {
        if (std::filesystem::exists(std::filesystem::path{initial_load_file.value()}))
        {
            const auto is_restore = initial_load_file.value().ends_with(".tdb");

            if (is_restore)
            {
                mx::util::Logger::info(fmt::format("Restoring database from '{}'.", initial_load_file.value()));

                auto *restore_task = mx::tasking::runtime::new_task<db::io::RestoreDatabaseTask>(
                    0U, std::numeric_limits<std::uint32_t>::max(), database, configuration,
                    std::string{initial_load_file.value()});
                restore_task->annotate(std::uint16_t(0U));
                task_line->add(restore_task);
            }
            else
            {
                mx::util::Logger::info(fmt::format("Executing commands from '{}'.", initial_load_file.value()));

                auto *load_task = mx::tasking::runtime::new_task<db::io::LoadFileTask>(
                    0U, std::numeric_limits<std::uint32_t>::max(), database, configuration,
                    std::string{initial_load_file.value()});
                load_task->annotate(std::uint16_t(0U));
                task_line->add(load_task);
            }

            auto *print_file_loaded_task =
                mx::tasking::runtime::new_task<mx::tasking::LambdaTask>(0U, [start, &initial_load_file, is_restore] {
                    const auto now = std::chrono::steady_clock::now();
                    const auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(now - start);

                    if (is_restore)
                    {
                        mx::util::Logger::info(fmt::format("Restoring database from '{0}' took {1:.2f} seconds.",
                                                           initial_load_file.value(), diff.count() / 1000.0));
                    }
                    else
                    {
                        mx::util::Logger::info(fmt::format("Executing commands from '{0}' took {1:.2f} seconds.",
                                                           initial_load_file.value(), diff.count() / 1000.0));
                    }
                });
            print_file_loaded_task->annotate(std::uint16_t(0U));
            task_line->add(print_file_loaded_task);
        }
    }

    if (execute_statement.has_value() == false)
    {
        auto *print_welcome_message_task = mx::tasking::runtime::new_task<mx::tasking::LambdaTask>(
            0U, [] { mx::util::Logger::info("Server is ready for requests."); });
        print_welcome_message_task->annotate(std::uint16_t(0U));
        task_line->add(print_welcome_message_task);
    }

    if (is_web_client)
    {
        auto *start_web_client_task =
            mx::tasking::runtime::new_task<db::io::StartWebServerTask>(0U, std::string{host}, port, web_port);
        start_web_client_task->annotate(std::uint16_t(0U));
        task_line->add(start_web_client_task);
    }

    if (is_server_only == false && execute_statement.has_value() == false)
    {
        auto *start_client_task =
            mx::tasking::runtime::new_task<db::io::StartClientConsoleTask>(0U, std::move(host), port);
        start_client_task->annotate(std::uint16_t(0U));
        task_line->add(start_client_task);
    }

    if (execute_statement.has_value())
    {
        if (iterations > 1U)
        {
            auto *benchmark_task = mx::tasking::runtime::new_task<db::io::cli::StartBenchmarkTask>(
                0U, port, std::move(execute_statement.value()), iterations, std::move(output_file));
            benchmark_task->annotate(std::uint16_t(0U));
            task_line->add(benchmark_task);
        }
        else
        {
            auto *execute_statement_task = mx::tasking::runtime::new_task<db::io::StartSingleCommandClientTask>(
                0U, port, std::move(execute_statement.value()), std::move(output_file));
            execute_statement_task->annotate(std::uint16_t(0U));
            task_line->add(execute_statement_task);
        }
    }

    /// If at least load file was found or client has to be started: spawn the task line.
    if (task_line->empty() == false)
    {
        mx::tasking::runtime::spawn(*task_line);
    }
}

std::optional<std::string> parse_execution_statement(const std::optional<std::string> &statement)
{
    if (statement.has_value() == false)
    {
        return std::nullopt;
    }

    /// Check, if the last word in the statement is a file. If so, we replace the filename
    /// by the content of the file.
    /// Examples:
    ///     --execute sql/TPCH-01.sql
    ///     OR --execute "compile sql/TPCH-01.sql"
    ///     OR --execute "sample cycles compile sql/TPCH-01.sql"
    const auto last_space = statement.value().find_last_of(' ');
    auto file_name = last_space == std::string::npos ? statement.value() : statement.value().substr(last_space + 1U);

    if (std::filesystem::exists(std::filesystem::path{file_name}))
    {
        /// Load the contents of the file.
        auto file_stream = std::ifstream{file_name};
        auto file_content =
            std::string((std::istreambuf_iterator<char>(file_stream)), (std::istreambuf_iterator<char>()));

        /// Replace all tabs and newlines (which indicate the end of the query) by spaces.
        file_content =
            db::util::string::replace(std::move(file_content), {std::make_pair("\t", ""), std::make_pair("\n", " ")});

        /// Replace the filename within the original statement by the content of the file.
        auto new_statement = db::util::string::replace(std::string{statement.value()},
                                                       {std::make_pair(std::move(file_name), file_content)});

        return std::make_optional(std::move(new_statement));
    }

    return statement;
}
