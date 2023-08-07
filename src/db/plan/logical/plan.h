#pragma once
#include <db/parser/node.h>
#include <db/plan/logical/node/command_nodes.h>
#include <db/plan/logical/node/explain_node.h>
#include <db/plan/logical/node/node_interface.h>
#include <db/plan/logical/node/projection_node.h>
#include <db/plan/logical/node/sample_node.h>
#include <db/topology/database.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <vector>

namespace db::plan::logical {
class Plan
{
public:
    [[nodiscard]] static Plan build(const topology::Database &database,
                                    std::unique_ptr<parser::NodeInterface> &&abstract_syntax_tree);

    Plan(Plan &&) noexcept = default;
    explicit Plan(std::unique_ptr<NodeInterface> &&root_node) noexcept : _root_node(std::move(root_node)) {}

    ~Plan() noexcept = default;

    Plan &operator=(Plan &&) noexcept = default;

    [[nodiscard]] std::unique_ptr<NodeInterface> &root_node() noexcept { return _root_node; }

    [[nodiscard]] bool is_load_file() const noexcept { return typeid(*_root_node) == typeid(LoadFileNode); }
    [[nodiscard]] bool is_store() const noexcept { return typeid(*_root_node) == typeid(StoreNode); }
    [[nodiscard]] bool is_restore() const noexcept { return typeid(*_root_node) == typeid(RestoreNode); }
    [[nodiscard]] bool is_select_query() const noexcept;
    [[nodiscard]] bool is_explain_plan() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::Plan>(_root_node);
    }
    [[nodiscard]] bool is_explain_task_graph() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::TaskGraph>(_root_node);
    }
    [[nodiscard]] bool is_explain_data_flow_graph() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::DataFlowGraph>(_root_node);
    }
    [[nodiscard]] bool is_explain_performance() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::Performance>(_root_node);
    }
    [[nodiscard]] bool is_explain_task_load() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::TaskLoad>(_root_node);
    }
    [[nodiscard]] bool is_explain_task_traces() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::TaskTraces>(_root_node);
    }
    [[nodiscard]] bool is_explain_flounder() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::Flounder>(_root_node);
    }
    [[nodiscard]] bool is_explain_assembly() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::Assembly>(_root_node);
    }
    [[nodiscard]] bool is_explain_dram_bandwidth() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::DRAMBandwidth>(_root_node);
    }
    [[nodiscard]] bool is_explain_times() const noexcept
    {
        return Plan::is_explain<ExplainNode::Level::Times>(_root_node);
    }

    [[nodiscard]] bool is_sample() const noexcept { return sample_type().has_value(); }

    [[nodiscard]] std::optional<std::tuple<SampleNode::Level, SampleNode::CounterType, std::optional<std::uint64_t>>>
    sample_type() const noexcept
    {
        return Plan::sample_type(_root_node);
    }

    [[nodiscard]] bool is_stop() const noexcept { return _root_node->query_type() == NodeInterface::QueryType::STOP; }
    [[nodiscard]] bool is_configuration() const noexcept
    {
        return _root_node->query_type() == NodeInterface::QueryType::CONFIGURATION;
    }
    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const noexcept
    {
        return _root_node->to_json(database);
    }

private:
    std::unique_ptr<NodeInterface> _root_node;

    [[nodiscard]] static std::unique_ptr<NodeInterface> build_node(
        std::unique_ptr<parser::NodeInterface> &&parser_node);

    [[nodiscard]] static std::unique_ptr<NodeInterface> build_select_query(
        std::unique_ptr<parser::NodeInterface> &&parser_node);

    [[nodiscard]] static std::unique_ptr<NodeInterface> build_from(
        std::vector<plan::logical::TableReference> &&from,
        std::optional<std::vector<plan::logical::JoinReference>> &&join);

    [[nodiscard]] static std::vector<std::unique_ptr<expression::Operation>> split_logical_and(
        std::unique_ptr<expression::Operation> &&operation);

    static void split_logical_and(std::unique_ptr<expression::Operation> &&operation,
                                  std::vector<std::unique_ptr<expression::Operation>> &container);

    static void unnest_exists(std::vector<TableReference> &table_references,
                              std::optional<std::vector<JoinReference>> &join_references,
                              std::vector<std::unique_ptr<expression::Operation>> &where_parts);

    static void extract_aggregation_from_arithmetic(std::unique_ptr<expression::Operation> &arithmetic,
                                                    std::vector<std::unique_ptr<expression::Operation>> &aggregations);
    [[nodiscard]] static std::unique_ptr<expression::Operation> replace_aggregation_by_attribute(
        std::unique_ptr<expression::Operation> &&aggregation,
        std::vector<std::unique_ptr<expression::Operation>> &aggregations);

    [[nodiscard]] static ExplainNode::Level extract_explain_level(
        parser::SelectQuery::ExplainLevel explain_level) noexcept;

    [[nodiscard]] static std::pair<SampleNode::Level, SampleNode::CounterType> extract_sample_level_type(
        parser::SelectQuery::SampleLevel sample_level,
        parser::SelectQuery::SampleCounterType sample_counter_type) noexcept;

    [[nodiscard]] static std::optional<ExplainNode::Level> explain_level(
        const std::unique_ptr<NodeInterface> &node) noexcept
    {
        if (typeid(*node) == typeid(ExplainNode))
        {
            return std::make_optional(reinterpret_cast<ExplainNode *>(node.get())->level());
        }

        return std::nullopt;
    }

    [[nodiscard]] static std::optional<
        std::tuple<SampleNode::Level, SampleNode::CounterType, std::optional<std::uint64_t>>>
    sample_type(const std::unique_ptr<NodeInterface> &node) noexcept
    {
        if (typeid(*node) == typeid(SampleNode))
        {
            auto *sample_node = reinterpret_cast<SampleNode *>(node.get());
            return std::make_optional(
                std::make_tuple(sample_node->level(), sample_node->counter_type(), sample_node->frequency()));
        }

        return std::nullopt;
    }

    template <ExplainNode::Level L>
    [[nodiscard]] static bool is_explain(const std::unique_ptr<NodeInterface> &node) noexcept
    {
        const auto explain_level = Plan::explain_level(node);
        return explain_level.has_value() && explain_level.value() == L;
    }
};
} // namespace db::plan::logical