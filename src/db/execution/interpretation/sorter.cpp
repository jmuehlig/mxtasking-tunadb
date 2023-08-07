#include "sorter.h"

using namespace db::execution::interpretation;

std::vector<Order> SorterFactory::build_orders(std::vector<expression::OrderBy> &&order_by,
                                               const topology::PhysicalSchema &schema)
{
    auto order = std::vector<Order>{};
    order.reserve(order_by.size());
    for (auto &order_item : order_by)
    {
        if (order_item.expression()->is_nullary())
        {
            const auto index = schema.index(order_item.expression()->result().value());
            if (index.has_value())
            {
                order.emplace_back(Order{index.value(), order_item.direction() == expression::OrderBy::Direction::ASC});
            }
        }
    }

    return order;
}