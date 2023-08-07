#include "cpu.h"
#include <fstream>
#include <sstream>
#include <string>

using namespace mx::system;

bool cpu::is_smt_core(const std::uint16_t core_id)
{
    const auto logical_core_ids = cpu::logical_core_ids(core_id);
    return logical_core_ids.empty() == false && logical_core_ids.front() != core_id;
}

ecpp::static_vector<std::uint16_t, mx::tasking::config::max_smt_threads() - 1U> cpu::sibling_core_ids(
    const std::uint16_t core_id)
{
    auto sibling_core_ids = ecpp::static_vector<std::uint16_t, mx::tasking::config::max_smt_threads() - 1U>{};

    for (const auto logical_core_id : cpu::logical_core_ids(core_id))
    {
        if (logical_core_id != core_id)
        {
            sibling_core_ids.emplace_back(logical_core_id);
        }
    }

    return sibling_core_ids;
}

ecpp::static_vector<std::uint16_t, mx::tasking::config::max_smt_threads()> cpu::logical_core_ids(uint16_t core_id)
{
    if (auto iterator = cpu::_logical_core_sibling_ids.find(core_id); iterator != cpu::_logical_core_sibling_ids.end())
    {
        return iterator->second;
    }

    auto logical_core_ids = ecpp::static_vector<std::uint16_t, mx::tasking::config::max_smt_threads()>{};

    auto toplogy_file_name = std::string{"/sys/devices/system/cpu/cpu"} + std::to_string(core_id) +
                             std::string{"/topology/thread_siblings_list"};
    auto topology_file = std::ifstream{toplogy_file_name};
    if (topology_file.is_open())
    {
        std::string line;
        if (std::getline(topology_file, line))
        {
            std::string smt_thread_id;
            auto line_stream = std::istringstream{line};
            while (std::getline(line_stream, smt_thread_id, ','))
            {
                logical_core_ids.emplace_back(std::stoi(smt_thread_id));
            }
        }

        for (const auto logical_core_id : logical_core_ids)
        {
            cpu::_logical_core_sibling_ids.insert(std::make_pair(logical_core_id, logical_core_ids));
        }
    }

    return logical_core_ids;
}