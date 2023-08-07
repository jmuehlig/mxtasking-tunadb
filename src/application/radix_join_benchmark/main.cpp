#include "benchmark.h"
#include <argparse/argparse.hpp>
#include <benchmark/cores.h>
#include <mx/system/environment.h>
#include <mx/system/thread.h>
#include <mx/tasking/runtime.h>
#include <mx/util/core_set.h>
#include <tuple>

using namespace application::rjbenchmark;

/**
 * Instantiates the BLink-Tree benchmark with CLI arguments.
 * @param count_arguments Number of CLI arguments.
 * @param arguments Arguments itself.
 *
 * @return Instance of the benchmark and parameters for tasking runtime.
 */
std::tuple<Benchmark *, mx::tasking::PrefetchDistance> create_benchmark(int count_arguments, char **arguments);

/**
 * Starts the benchmark.
 *
 * @param count_arguments Number of CLI arguments.
 * @param arguments Arguments itself.
 *
 * @return Return code of the application.
 */
int main(int count_arguments, char **arguments)
{
    if (mx::system::Environment::is_numa_balancing_enabled())
    {
        std::cout << "[Warn] NUMA balancing may be enabled, set '/proc/sys/kernel/numa_balancing' to '0'" << std::endl;
    }

    auto [benchmark, prefetch_distance] = create_benchmark(count_arguments, arguments);
    if (benchmark == nullptr)
    {
        return 1;
    }

    auto cores = mx::util::core_set{};

    while ((cores = benchmark->core_set()))
    {
        auto _ = mx::tasking::runtime_guard{false, cores, prefetch_distance};
        benchmark->start();
    }

    delete benchmark;

    return 0;
}

std::tuple<Benchmark *, mx::tasking::PrefetchDistance> create_benchmark(int count_arguments, char **arguments)
{
    // Set up arguments.
    argparse::ArgumentParser argument_parser("blinktree_benchmark");
    argument_parser.add_argument("cores")
        .help("Range of the number of cores (1 for using 1 core, 1: for using 1 up to available cores, 1:4 for using "
              "cores from 1 to 4).")
        .default_value(std::string("1"));
    argument_parser.add_argument("-s", "--steps")
        .help("Steps, how number of cores is increased (1,2,4,6,.. for -s 2).")
        .default_value(std::uint16_t(2))
        .action([](const std::string &value) { return std::uint16_t(std::stoi(value)); });
    argument_parser.add_argument("-i", "--iterations")
        .help("Number of iterations for each workload")
        .default_value(std::uint16_t(1))
        .action([](const std::string &value) { return std::uint16_t(std::stoi(value)); });
    argument_parser.add_argument("-sco", "--system-core-order")
        .help("Use systems core order. If not, cores are ordered by node id (should be preferred).")
        .implicit_value(true)
        .default_value(false);
    argument_parser.add_argument("-p", "--perf")
        .help("Use performance counter.")
        .implicit_value(true)
        .default_value(false);
    argument_parser.add_argument("--build")
        .help("File containing the data for the build side")
        .default_value(std::string{"R.tbl"});
    argument_parser.add_argument("--probe")
        .help("File containing the data for the probe side")
        .default_value(std::string{"S.tbl"});
    argument_parser.add_argument("-pd", "--prefetch-distance")
        .help("Distance of prefetched data objects (0 = disable prefetching).")
        .default_value(std::uint8_t(0))
        .action([](const std::string &value) { return std::uint8_t(std::stoi(value)); });
    argument_parser.add_argument("--prefetch4me")
        .help("Enables automatic prefetching. When set, the fixed prefetch distance will be discarded.")
        .implicit_value(true)
        .default_value(false);
    argument_parser.add_argument("-o", "--out")
        .help("Name of the file, the results will be written to.")
        .default_value(std::string(""));

    // Parse arguments.
    try
    {
        argument_parser.parse_args(count_arguments, arguments);
    }
    catch (std::runtime_error &e)
    {
        std::cout << argument_parser << std::endl;
        return std::make_tuple(nullptr, mx::tasking::PrefetchDistance{0U});
    }

    auto order =
        argument_parser.get<bool>("-sco") ? mx::util::core_set::Order::Ascending : mx::util::core_set::Order::NUMAAware;
    auto cores =
        benchmark::Cores({argument_parser.get<std::string>("cores"), argument_parser.get<std::uint16_t>("-s"), order});
    auto build_file_name = argument_parser.get<std::string>("--build");
    auto probe_file_name = argument_parser.get<std::string>("--probe");

    // Create the benchmark.
    auto *benchmark = new Benchmark(std::move(cores), argument_parser.get<std::uint16_t>("-i"),
                                    std::move(build_file_name), std::move(probe_file_name),
                                    argument_parser.get<bool>("-p"), argument_parser.get<std::string>("-o"));

    auto prefetch_distance = mx::tasking::PrefetchDistance{argument_parser.get<std::uint8_t>("-pd")};
    if (argument_parser.get<bool>("--prefetch4me"))
    {
        prefetch_distance = mx::tasking::PrefetchDistance::make_automatic();
    }

    return std::make_tuple(benchmark, prefetch_distance);
}