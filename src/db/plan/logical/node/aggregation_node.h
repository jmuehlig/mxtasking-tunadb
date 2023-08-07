#pragma once
#include "node_interface.h"
#include "selection_node.h"
#include <db/exception/plan_exception.h>
#include <db/expression/operation.h>
#include <db/expression/term.h>
#include <db/plan/logical/cardinality_estimator.h>
#include <fmt/format.h>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace db::plan::logical {
class AggregationNode final : public UnaryNode
{
public:
    enum Method
    {
        SimpleAggregation,
        HashAggregation,
        RadixAggregation,
    };

    AggregationNode(std::vector<std::unique_ptr<expression::Operation>> &&operations,
                    const std::optional<std::vector<expression::Term>> &groups)
        : AggregationNode(groups.has_value() ? Method::HashAggregation : Method::SimpleAggregation,
                          std::move(operations), groups)
    {
    }

    AggregationNode(const Method method, std::vector<std::unique_ptr<expression::Operation>> &&operations,
                    const std::optional<std::vector<expression::Term>> &groups)
        : UnaryNode("Aggregation"), _method(method), _aggregation_operations(std::move(operations)), _groups(groups)
    {
    }

    ~AggregationNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] Method method() const noexcept { return _method; }

    void method(const Method method) noexcept { _method = method; }

    [[nodiscard]] std::vector<std::unique_ptr<expression::Operation>> &aggregation_operations() noexcept
    {
        return _aggregation_operations;
    }

    [[nodiscard]] std::optional<std::vector<expression::Term>> &groups() noexcept { return _groups; }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = UnaryNode::to_json(database);

        switch (_method)
        {
        case Method::SimpleAggregation:
            json["name"] = "Aggregation";
            break;
        case Method::HashAggregation:
            json["name"] = "Hash Aggregation";
            break;
        case Method::RadixAggregation:
            json["name"] = "Radix Aggregation";
            break;
        }

        auto operations = std::vector<std::string>{};
        std::transform(_aggregation_operations.begin(), _aggregation_operations.end(), std::back_inserter(operations),
                       [](const auto &operation) { return operation->to_string(); });
        json["data"]["Aggregations"] = fmt::format("{}", fmt::join(std::move(operations), ", "));

        if (_groups.has_value())
        {
            auto groups = std::vector<std::string>{};
            std::transform(_groups.value().begin(), _groups.value().end(), std::back_inserter(groups),
                           [](const auto &term) { return term.to_string(); });
            json["data"]["Groups"] = fmt::format("{}", fmt::join(std::move(groups), ", "));
        }

        return json;
    }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database &database,
                                            const NodeChildIterator &child_iterator) const override
    {
        if (_groups.has_value() == false)
        {
            return 1U;
        }

        const auto child_cardinality = child_iterator.child(this)->relation().cardinality();

        /// Estimate cardinality from count distinct columns.
        const auto cardinality_from_count_distinct = this->cardinality_from_count_distinct(database, child_iterator);
        if (cardinality_from_count_distinct.has_value())
        {
            auto cardinality = std::min(child_cardinality, cardinality_from_count_distinct.value());
            return std::max<std::uint64_t>(2UL, cardinality < 1024 ? cardinality : cardinality * .5);
        }

        /// Estimate cardinality from number of groups.
        auto cardinality_factor = 256U;
        for (auto i = 1U; i < _groups.value().size() && cardinality_factor > 2U; ++i)
        {
            cardinality_factor /= 2U;
        }

        const auto estimated_cardinality = std::max(8UL, child_cardinality / cardinality_factor);

        /// Take the lower one.
        return std::min(child_cardinality, estimated_cardinality);
    }

    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const override
    {
        auto schema = topology::LogicalSchema{};
        schema.reserve(_aggregation_operations.size() + (_groups.has_value() ? _groups->size() : 0U));

        const auto &child_schema = child_iterator.child(this)->relation().schema();
        for (const auto &aggregation : _aggregation_operations)
        {
            const auto type = aggregation->type(child_schema);
            schema.emplace_back(aggregation->result().value(), type);
        }

        if (_groups.has_value())
        {
            for (const auto &group : _groups.value())
            {
                const auto index = child_schema.index(group);
                if (index.has_value())
                {
                    schema.emplace_back(child_schema.term(index.value()), child_schema.type(index.value()));
                }
            }
        }

        return schema;
    }

private:
    Method _method{Method::SimpleAggregation};
    std::vector<std::unique_ptr<expression::Operation>> _aggregation_operations;
    std::optional<std::vector<expression::Term>> _groups;

    [[nodiscard]] std::optional<std::uint64_t> cardinality_from_count_distinct(
        const topology::Database &database, const NodeChildIterator &child_iterator) const
    {
        const auto is_sources_available =
            std::all_of(_groups.value().begin(), _groups.value().end(), [](const auto &term) {
                return term.is_attribute() && term.template get<expression::Attribute>().source().has_value();
            });

        if (is_sources_available)
        {

            auto predicate_distinct_values = std::unordered_map<expression::Term, std::uint64_t>{};
            predicate_distinct_values.reserve(_groups->size());

            for (const auto &term : _groups.value())
            {
                const auto count_distinct =
                    CardinalityEstimator::estimate_distinct_values(database, term.get<expression::Attribute>());
                if (count_distinct.has_value())
                {
                    predicate_distinct_values.insert(std::make_pair(term, count_distinct.value()));
                }
            }

            if (predicate_distinct_values.size() == _groups->size())
            {
                this->update_distinct_values_from_predicates(child_iterator.child(this), database, child_iterator,
                                                             predicate_distinct_values);

                return std::make_optional(std::accumulate(
                    predicate_distinct_values.begin(), predicate_distinct_values.end(), 1U,
                    [](const auto result, const auto &distinct_values) { return result * distinct_values.second; }));
            }
        }

        return std::nullopt;
    }

    void update_distinct_values_from_predicates(
        NodeInterface *node, const topology::Database &database, const NodeChildIterator &child_iterator,
        std::unordered_map<expression::Term, std::uint64_t> &predicate_distinct_values) const
    {
        if (node->is_unary())
        {
            if (typeid(*node) == typeid(SelectionNode))
            {
                const auto &predicate = reinterpret_cast<SelectionNode *>(node)->predicate();
                expression::for_each_comparison(predicate, [&predicate_distinct_values](const auto &comparison) {
                    const auto &term = comparison->left_child()->result().value();
                    if (auto iterator = predicate_distinct_values.find(term);
                        iterator != predicate_distinct_values.end())
                    {
                        if (comparison->id() == expression::Operation::Id::Equals)
                        {
                            iterator->second = 1U;
                        }
                        else if (comparison->id() == expression::Operation::Id::In)
                        {
                            auto *nullary_list =
                                reinterpret_cast<expression::NullaryListOperation *>(comparison->right_child().get());

                            iterator->second = std::min(iterator->second, nullary_list->terms().size());
                        }
                    }
                });
            }

            this->update_distinct_values_from_predicates(child_iterator.child(reinterpret_cast<UnaryNode *>(node)),
                                                         database, child_iterator, predicate_distinct_values);
        }
        else if (node->is_binary())
        {
            auto [left_child, right_child] = child_iterator.children(reinterpret_cast<BinaryNode *>(node));
            this->update_distinct_values_from_predicates(left_child, database, child_iterator,
                                                         predicate_distinct_values);
            this->update_distinct_values_from_predicates(right_child, database, child_iterator,
                                                         predicate_distinct_values);
        }
    }
};
} // namespace db::plan::logical