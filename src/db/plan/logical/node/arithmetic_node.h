#pragma once
#include "node_interface.h"
#include <db/expression/operation.h>
#include <memory>

namespace db::plan::logical {
class ArithmeticNode final : public UnaryNode
{
public:
    ArithmeticNode(std::vector<std::unique_ptr<expression::Operation>> &&operations)
        : UnaryNode("Arithmetic"), _arithmetic_operations(std::move(operations))
    {
    }

    ~ArithmeticNode() noexcept override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::vector<std::unique_ptr<expression::Operation>> &arithmetic_operations() noexcept
    {
        return _arithmetic_operations;
    }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = UnaryNode::to_json(database);
        auto operations = std::vector<std::string>{};
        std::transform(_arithmetic_operations.begin(), _arithmetic_operations.end(), std::back_inserter(operations),
                       [](const auto &operation) { return operation->to_string(); });
        json["data"]["Projections"] = fmt::format("{}", fmt::join(std::move(operations), ", "));
        return json;
    }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database & /*database*/,
                                            const NodeChildIterator &child_iterator) const override
    {
        return child_iterator.child(this)->relation().cardinality();
    }

    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const override
    {
        auto schema = child_iterator.child(this)->relation().schema();

        for (const auto &operation : _arithmetic_operations)
        {
            schema.emplace_back(operation->result().value(), operation->type(schema));
        }

        return schema;
    }

private:
    std::vector<std::unique_ptr<expression::Operation>> _arithmetic_operations;
};
} // namespace db::plan::logical