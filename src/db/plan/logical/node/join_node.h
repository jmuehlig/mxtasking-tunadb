#pragma once
#include "node_interface.h"
#include <db/expression/operation.h>
#include <db/plan/logical/cardinality_estimator.h>
#include <memory>

namespace db::plan::logical {
class JoinNode final : public BinaryNode
{
public:
    enum Method
    {
        NestedLoopsJoin,
        HashJoin,
        RadixJoin,
        FilteredRadixJoin,
    };

    JoinNode(const Method method, std::unique_ptr<expression::Operation> &&predicate,
             std::unique_ptr<NodeInterface> &&left_child, std::unique_ptr<NodeInterface> &&right_child)
        : BinaryNode("Join", std::move(left_child), std::move(right_child)), _method(method),
          _predicate(std::move(predicate))
    {
    }

    JoinNode(std::unique_ptr<expression::Operation> &&predicate, std::unique_ptr<NodeInterface> &&left_child,
             std::unique_ptr<NodeInterface> &&right_child)
        : JoinNode(Method::NestedLoopsJoin, std::move(predicate), std::move(left_child), std::move(right_child))
    {
    }

    JoinNode(const Method method, std::unique_ptr<expression::Operation> &&predicate)
        : JoinNode(method, std::move(predicate), nullptr, nullptr)
    {
    }

    explicit JoinNode(std::unique_ptr<expression::Operation> &&predicate)
        : JoinNode(std::move(predicate), nullptr, nullptr)
    {
    }

    ~JoinNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database &database,
                                            const NodeChildIterator &child_iterator) const override
    {
        auto [left_child, right_child] = child_iterator.children(this);

        if (_predicate->id() == expression::Operation::Id::Equals)
        {
            auto *binary_predicate = reinterpret_cast<expression::BinaryOperation *>(_predicate.get());
            if (binary_predicate->left_child()->is_nullary() &&
                binary_predicate->left_child()->result()->is_attribute() &&
                binary_predicate->right_child()->is_nullary() &&
                binary_predicate->right_child()->result()->is_attribute())
            {
                const auto left_distinct = CardinalityEstimator::estimate_distinct_values(
                    database, binary_predicate->left_child()->result()->get<expression::Attribute>());
                const auto right_distinct = CardinalityEstimator::estimate_distinct_values(
                    database, binary_predicate->right_child()->result()->get<expression::Attribute>());

                if (left_distinct.has_value() && right_distinct.has_value())
                {
                    const auto cp_cardinality =
                        left_child->relation().cardinality() * right_child->relation().cardinality();
                    return std::max(1UL, cp_cardinality /
                                             (std::max(1UL, std::max(left_distinct.value(), right_distinct.value()))));
                }
            }
        }

        return (left_child->relation().cardinality() + right_child->relation().cardinality()) / 2U;
    }

    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const override
    {
        auto [left_child, right_child] = child_iterator.children(this);

        auto schema = left_child->relation().schema();
        schema.push_back(right_child->relation().schema());

        return schema;
    }

    [[nodiscard]] const std::unique_ptr<expression::Operation> &predicate() const noexcept { return _predicate; }
    [[nodiscard]] std::unique_ptr<expression::Operation> &predicate() noexcept { return _predicate; }
    [[nodiscard]] Method method() const noexcept { return _method; }
    void method(const Method method) noexcept { _method = method; }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = BinaryNode::to_json(database);
        switch (_method)
        {
        case NestedLoopsJoin:
            json["name"] = "NL Join";
            break;
        case HashJoin:
            json["name"] = "Hash Join";
            break;
        case RadixJoin:
            json["name"] = "Radix Join";
            break;
        case FilteredRadixJoin:
            json["name"] = "Filtered Radix Join";
            break;
        }

        json["data"]["Predicate"] = _predicate->to_string();

        return json;
    }

private:
    Method _method{Method::NestedLoopsJoin};
    std::unique_ptr<expression::Operation> _predicate;
};
} // namespace db::plan::logical