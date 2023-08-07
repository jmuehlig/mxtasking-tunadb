#pragma once
#include "node_interface.h"
#include <db/plan/logical/table.h>
#include <memory>
#include <optional>

namespace db::plan::logical {
class MaterializeNode final : public UnaryNode
{
public:
    explicit MaterializeNode() : UnaryNode("Materialize") {}

    ~MaterializeNode() noexcept override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database & /*database*/,
                                            const NodeChildIterator &child_iterator) const override
    {
        return child_iterator.child(this)->relation().cardinality();
    }

    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const override
    {
        return child_iterator.child(this)->relation().schema();
    }
};
} // namespace db::plan::logical