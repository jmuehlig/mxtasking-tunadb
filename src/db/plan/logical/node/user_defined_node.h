#pragma once
#include "node_interface.h"
#include <db/expression/operation.h>
#include <memory>

namespace db::plan::logical {
class UserDefinedNode final : public UnaryNode
{
public:
    UserDefinedNode(std::vector<std::unique_ptr<expression::UserDefinedFunctionOperation>> &&user_defined_functions)
        : UnaryNode("User Defined"), _user_defined_functions(std::move(user_defined_functions))
    {
    }

    ~UserDefinedNode() noexcept override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::vector<std::unique_ptr<expression::UserDefinedFunctionOperation>>
        &user_defined_functions() noexcept
    {
        return _user_defined_functions;
    }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = UnaryNode::to_json(database);
        auto operations = std::vector<std::string>{};
        std::transform(_user_defined_functions.begin(), _user_defined_functions.end(), std::back_inserter(operations),
                       [](const auto &operation) { return operation->to_string(); });
        json["data"]["UDFs"] = fmt::format("{}", fmt::join(std::move(operations), ", "));
        return json;
    }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database & /*database*/,
                                            const NodeChildIterator &child_iterator) const override
    {
        return child_iterator.child(this)->relation().cardinality();
    }

    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator & /*child_iterator*/) const override
    {
        auto schema = topology::LogicalSchema{};
        schema.reserve(_user_defined_functions.size());

        for (const auto &operation : _user_defined_functions)
        {
            schema.emplace_back(operation->result().value(), operation->type(schema));
        }

        return schema;
    }

private:
    std::vector<std::unique_ptr<expression::UserDefinedFunctionOperation>> _user_defined_functions;
};
} // namespace db::plan::logical