#include "dataflow_graph.h"
#include <db/config.h>
#include <db/execution/compilation/compilation_node.h>
#include <fmt/core.h>
#include <sstream>
#include <unordered_map>

using namespace db::plan::physical;

std::string DataFlowGraph::to_dot(DataFlowGraph *graph, const bool is_include_emit_count)
{
    auto dot_stream = std::stringstream{};
    dot_stream << fmt::format("digraph {} {{\n\t// Pipelines and Nodes\n", config::name());

    std::unordered_map<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *, std::uint64_t> node_names;
    auto next_pipeline_id = std::uint64_t{0U};
    auto next_node_id = std::uint64_t{0U};

    for (auto *pipeline : graph->pipelines())
    {
        const auto pipeline_id = next_pipeline_id++;
        dot_stream << fmt::format(
            "\tsubgraph cluster_{0} {{\n\t\tcolor=\"#2a9d8f\";\n\t\tfontcolor=\"#2a9d8f\";\n\t\tlabel=\"Pipeline "
            "{0}\";\n",
            pipeline_id);

        for (auto *node : pipeline->nodes())
        {
            auto tooltip = std::string{};
            if (auto *compilation_node = dynamic_cast<execution::compilation::CompilationNode *>(node))
            {
                if (compilation_node->information().empty() == false)
                {
                    auto information = std::vector<std::string>{};
                    std::transform(compilation_node->information().begin(), compilation_node->information().end(),
                                   std::back_inserter(information),
                                   [](const auto &info) { return fmt::format("{} = {}", info.first, info.second); });
                    tooltip = fmt::format(",tooltip=\"{}\"", fmt::join(std::move(information), "\n"));
                }
            }

            const auto node_id = next_node_id++;
            node_names.insert(std::make_pair(node, node_id));
            dot_stream << fmt::format(
                "\t\tnode_{} [label=\"{}\",color=\"#118ab2\",fontcolor=\"#118ab2\",shape=\"box\"{}];\n", node_id,
                node->to_string(), std::move(tooltip));
        }
        dot_stream << "\t}\n";
    }

    dot_stream << "\n"
               << "\t// Edges\n";

    for (auto &node_name : node_names)
    {
        auto *node = std::get<0>(node_name);
        auto *outgoing = node->out();
        if (outgoing != nullptr)
        {
            auto label = std::string{};
            if (is_include_emit_count)
            {
                const auto emitted = graph->count_emitted(node);
                label = fmt::format("{}", emitted);
                if (emitted >= 1000000U)
                {
                    label = fmt::format("{}M", std::uint64_t(std::ceil(emitted / 1000000.0)));
                }
                else if (emitted >= 10000U)
                {
                    label = fmt::format("{}0k", std::uint64_t(std::ceil(emitted / 10000.0)));
                }
            }

            dot_stream << fmt::format("\tnode_{} -> node_{} [label=\"{}\",color=\"#2a9d8f\",fontcolor=\"#e76f51\"];\n",
                                      std::get<1>(node_name), node_names.at(outgoing), std::move(label));
        }
    }

    dot_stream << "\n"
               << "\t// Edges for dependencies\n";
    for (const auto &[node, node_to_wait_for] : graph->node_dependencies())
    {
        dot_stream << fmt::format(
            "\tnode_{} -> node_{} [color=\"#e76f51\",fontcolor=\"#e76f51\",label=\"wait for\"];\n", node_names.at(node),
            node_names.at(node_to_wait_for));
    }

    dot_stream << "}\n" << std::flush;

    return dot_stream.str();
}