#pragma once
#include "node_interface.h"
#include <db/expression/limit.h>
#include <db/plan/logical/table.h>
#include <memory>
#include <optional>

namespace db::plan::logical {
class LimitNode final : public UnaryNode
{
public:
    explicit LimitNode(expression::Limit &&limit) : UnaryNode("Limit"), _limit(std::move(limit)) {}
    explicit LimitNode(expression::Limit limit) : UnaryNode("Limit"), _limit(limit) {}

    ~LimitNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database & /*database*/,
                                            const NodeChildIterator & /*child_iterator*/) const override
    {
        return _limit.limit();
    }

    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const override
    {
        return child_iterator.child(this)->relation().schema();
    }

    [[nodiscard]] expression::Limit &limit() { return _limit; }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = UnaryNode::to_json(database);
        json["data"]["Limit"] = _limit.to_string();

        return json;
    }

private:
    expression::Limit _limit;
};
} // namespace db::plan::logical