#include "serialized_plan.h"
#include <fmt/core.h>
#include <fmt/format.h>
#include <iostream>

using namespace db::io;

std::string SerializedPlan::to_string() const noexcept
{
    auto text_table = util::TextTable{};
    text_table.header({"Operator", "Data", "Schema", "Cardinality"});

    SerializedPlan::add_plan_to_table(text_table, this->_plan, 0U);

    /// Convert to string.
    std::stringstream table_stream;
    table_stream << text_table << std::flush;

    return table_stream.str();
}

void SerializedPlan::add_plan_to_table(util::TextTable &table, const nlohmann::json &layer,
                                       const std::uint16_t depth) noexcept
{
    auto name = std::string(depth, ' ') + layer["name"].get<std::string>();
    auto cardinality = layer["cardinality"].get<std::uint64_t>();

    auto data = std::string{};
    if (layer.contains("data"))
    {
        auto data_items = std::vector<std::string>{};
        for (const auto &[key, value] : layer["data"].items())
        {
            data_items.emplace_back(fmt::format("{}: {}", key, value));
        }
        data = fmt::format("{}", fmt::join(std::move(data_items), " / "));
    }
    if (data.size() > 53U)
    {
        data = data.substr(0U, 50U) + "...";
    }

    auto output = layer["output"].get<std::string>();
    if (output.size() > 53U)
    {
        output = output.substr(0, 50U) + "...";
    }

    table.emplace_back({std::move(name), std::move(data), std::move(output), std::to_string(cardinality)});
    if (layer.contains("childs"))
    {
        for (const auto &child : layer["childs"])
        {
            add_plan_to_table(table, child, depth + 2U);
        }
    }
}

std::string SerializedPlan::to_dot() const noexcept
{
    auto dot_stream = std::stringstream{};

    auto current_node_id = 0UL;
    auto nodes = std::vector<std::tuple<std::uint64_t, std::string, std::optional<std::string>>>{};
    auto edges = std::vector<std::tuple<std::uint64_t, std::uint64_t, std::uint64_t>>{};
    SerializedPlan::add_plan_to_dot(this->_plan, current_node_id, nodes, edges);

    dot_stream << "digraph mxdb {\n"
               << "\trankdir=\"BT\";\n"
               << "\t// Nodes\n";
    for (const auto &[id, label, tooltip] : nodes)
    {
        dot_stream << fmt::format("\t{} [label=\" {} \",color=\"#118ab2\",fontcolor=\"#118ab2\",shape=\"box\"", id,
                                  label);
        if (tooltip.has_value())
        {
            dot_stream << fmt::format(",tooltip=\"{}\"", tooltip.value());
        }
        dot_stream << "];\n";
    }

    dot_stream << "\n"
               << "\t// Edges\n";

    for (const auto &[parent_id, child_id, cardinality] : edges)
    {
        auto cardinality_string = fmt::format("{}", cardinality);
        if (cardinality >= 1000000U)
        {
            cardinality_string = fmt::format("{:.1f}M", cardinality / 1000000.0);
        }
        else if (cardinality >= 10000U)
        {
            cardinality_string = fmt::format("{}0k", std::uint64_t(cardinality / 10000.0));
        }
        else if (cardinality >= 1000U)
        {
            cardinality_string = fmt::format("{:.1f}k", cardinality / 1000.0);
        }

        dot_stream << fmt::format("\t{} -> {} [label=\"{}\",color=\"#2a9d8f\",fontcolor=\"#e76f51\"];\n", child_id,
                                  parent_id, std::move(cardinality_string));
    }

    dot_stream << "}\n" << std::flush;
    return dot_stream.str();
}

std::uint64_t SerializedPlan::add_plan_to_dot(
    const nlohmann::json &layer, std::uint64_t &current_node_id,
    std::vector<std::tuple<std::uint64_t, std::string, std::optional<std::string>>> &nodes,
    std::vector<std::tuple<std::uint64_t, std::uint64_t, std::uint64_t>> &edges) noexcept
{
    const auto node_id = current_node_id++;
    auto node_name = layer["name"].get<std::string>();
    auto node_data = std::optional<std::string>{std::nullopt};

    if (layer.contains("output"))
    {
        node_data = std::make_optional(fmt::format("Schema: {}", layer["output"].get<std::string>()));
    }

    if (layer.contains("data"))
    {
        auto data = std::string{};
        if (layer.contains("data"))
        {
            auto data_items = std::vector<std::string>{};
            for (const auto &[key, value] : layer["data"].items())
            {
                data_items.emplace_back(fmt::format("{}: {}", key, value.get<std::string>()));
            }
            data = fmt::format("{}", fmt::join(std::move(data_items), "\n"));
        }

        if (node_data.has_value())
        {
            node_data = std::make_optional(fmt::format("{}\n{}", std::move(node_data.value()), std::move(data)));
        }
        else
        {
            node_data = std::make_optional(std::move(data));
        }
    }

    nodes.emplace_back(node_id, std::move(node_name), std::move(node_data));

    if (layer.contains("childs"))
    {
        for (const auto &child : layer["childs"])
        {
            const auto child_id = SerializedPlan::add_plan_to_dot(child, current_node_id, nodes, edges);
            edges.emplace_back(node_id, child_id, child["cardinality"].get<std::uint64_t>());
        }
    }

    return node_id;
}