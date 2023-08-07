#pragma once

#include <db/topology/physical_schema.h>

namespace db::execution {

class OperatorInterface
{
public:
    constexpr OperatorInterface() noexcept = default;
    virtual ~OperatorInterface() noexcept = default;

    [[nodiscard]] virtual const topology::PhysicalSchema &schema() const = 0;
};
} // namespace db::execution