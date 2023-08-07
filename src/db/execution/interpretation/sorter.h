#pragma once

#include "record_comparator.h"
#include <db/expression/order_by.h>
#include <db/topology/physical_schema.h>
#include <vector>

namespace db::execution::interpretation {
class SorterFactory
{
public:
    [[nodiscard]] static std::vector<Order> build_orders(std::vector<expression::OrderBy> &&order_by,
                                                         const topology::PhysicalSchema &schema);
};
} // namespace db::execution::interpretation