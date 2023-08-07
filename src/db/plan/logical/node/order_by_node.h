#pragma once
#include "node_interface.h"
#include <db/expression/limit.h>
#include <db/expression/order_by.h>
#include <db/plan/logical/table.h>
#include <sstream>
#include <vector>

namespace db::plan::logical {
class OrderByNode final : public UnaryNode
{
public:
    enum Method
    {
        Sequential,
        Parallel
    };

    explicit OrderByNode(std::vector<expression::OrderBy> &&order_by)
        : OrderByNode(Method::Sequential, std::move(order_by))
    {
    }

    OrderByNode(const Method method, std::vector<expression::OrderBy> &&order_by)
        : OrderByNode(method, std::move(order_by), std::nullopt)
    {
    }

    OrderByNode(const Method method, std::vector<expression::OrderBy> &&order_by,
                std::optional<expression::Limit> limit)
        : UnaryNode("Order"), _order_by(std::move(order_by)), _limit(limit), _method(method)
    {
    }

    ~OrderByNode() override = default;

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

    [[nodiscard]] std::vector<expression::OrderBy> &order_by() { return _order_by; }

    [[nodiscard]] Method method() const noexcept { return _method; }

    void method(const Method method) noexcept { _method = method; }

    void limit(expression::Limit limit) noexcept { _limit = limit; }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = UnaryNode::to_json(database);
        switch (_method)
        {
        case Parallel:
            json["name"] = "Order By (parallel)";
            break;
        case Sequential:
            json["name"] = "Order By";
            break;
        }

        auto operations = std::vector<std::string>{};
        std::transform(_order_by.begin(), _order_by.end(), std::back_inserter(operations),
                       [](const auto &operation) { return operation.to_string(); });
        json["data"]["Sort"] = fmt::format("{}", fmt::join(std::move(operations), ", "));
        if (_limit.has_value())
        {
            json["data"]["Limit"] = fmt::format(" / LIMIT {}", _limit->to_string());
        }

        return json;
    }

private:
    std::vector<expression::OrderBy> _order_by;
    std::optional<expression::Limit> _limit{std::nullopt};
    Method _method{Method::Sequential};
};
} // namespace db::plan::logical