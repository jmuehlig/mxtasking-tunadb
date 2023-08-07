#pragma once
#include "node_interface.h"

namespace db::plan::logical {
class CrossProductNode final : public BinaryNode
{
public:
    CrossProductNode(std::unique_ptr<NodeInterface> &&left_child, std::unique_ptr<NodeInterface> &&right_child)
        : BinaryNode("Cross Product", std::move(left_child), std::move(right_child))
    {
    }

    ~CrossProductNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database & /*database*/,
                                            const NodeChildIterator &child_iterator) const override
    {
        auto [left_child, right_child] = child_iterator.children(this);
        return left_child->relation().cardinality() * right_child->relation().cardinality();
    }

    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const override
    {
        auto [left_child, right_child] = child_iterator.children(this);

        auto schema = left_child->relation().schema();
        schema.push_back(right_child->relation().schema());

        return schema;
    }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = BinaryNode::to_json(database);
        return json;
    }
};
} // namespace db::plan::logical