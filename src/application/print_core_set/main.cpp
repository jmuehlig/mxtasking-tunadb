#include <argparse/argparse.hpp>
#include <iostream>
#include <mx/util/core_set.h>

int main(int count_arguments, char **arguments)
{
    auto argument_parser = argparse::ArgumentParser{"print_core_set"};
    argument_parser.add_argument("cores")
        .help("Number of used cores.")
        .default_value(std::uint16_t(1U))
        .action([](const std::string &value) { return std::uint16_t(std::stoi(value)); });
    argument_parser.add_argument("-co", "--core-order")
        .help("How to order cores (numa, smt, system).")
        .default_value(std::string{"numa"})
        .action([](const std::string &value) { return value; });

    try
    {
        argument_parser.parse_args(count_arguments, arguments);
    }
    catch (std::runtime_error &e)
    {
        std::cout << argument_parser << std::endl;
        return 1;
    }

    auto core_order = mx::util::core_set::Order::NUMAAware;
    const auto preferred_core_order = argument_parser.get<std::string>("-co");
    if (preferred_core_order == "smt")
    {
        core_order = mx::util::core_set::Order::Physical;
    }
    else if (preferred_core_order == "system")
    {
        core_order = mx::util::core_set::Order::Ascending;
    }

    auto core_set = mx::util::core_set::build(argument_parser.get<std::uint16_t>("cores"), core_order);
    std::cout << core_set.to_string() << std::endl;

    return 0;
}