#pragma once
#include "node_interface.h"
#include <db/exception/plan_exception.h>
#include <db/expression/operation.h>
#include <db/plan/logical/cardinality_estimator.h>
#include <db/plan/logical/table.h>
#include <memory>

namespace db::plan::logical {
class SelectionNode final : public UnaryNode
{
public:
    explicit SelectionNode(std::unique_ptr<expression::Operation> &&predicate)
        : UnaryNode("Selection"), _predicate(std::move(predicate))
    {
    }

    ~SelectionNode() noexcept override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database &database,
                                            const NodeChildIterator &child_iterator) const override
    {
        const auto child_cardinality = child_iterator.child(this)->relation().cardinality();

        return std::max(1UL, CardinalityEstimator::estimate(child_cardinality, database, _predicate));
    }
    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const override
    {
        const auto &child_schema = child_iterator.child(this)->relation().schema();

        /// Check all predicates available.
        expression::for_each_term(_predicate, [&child_schema](const expression::Term &term) {
            if (term.is_attribute())
            {
                if (child_schema.contains(term) == false)
                {
                    throw exception::AttributeNotFoundException{term.get<expression::Attribute>().column_name()};
                }
            }
        });

        return child_schema;
    }

    [[nodiscard]] std::unique_ptr<expression::Operation> &predicate() { return _predicate; }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = UnaryNode::to_json(database);
        json["data"]["Predicate"] = _predicate->to_string();
        const auto selectivity = CardinalityEstimator::estimate_selectivity(database, _predicate);
        json["data"]["Selectivity"] = fmt::format("{:.3f} %", selectivity * 100.0F);
        return json;
    }

private:
    std::unique_ptr<expression::Operation> _predicate;
};
} // namespace db::plan::logical