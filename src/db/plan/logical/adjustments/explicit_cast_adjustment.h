#pragma once

#include "adjustment_interface.h"
#include <db/expression/operation.h>
#include <db/topology/logical_schema.h>
#include <memory>

namespace db::plan::logical {
class ExplicitCastAdjustment final : public AdjustmentInterface
{
public:
    constexpr ExplicitCastAdjustment() noexcept = default;
    ~ExplicitCastAdjustment() noexcept override = default;

    void apply(std::unique_ptr<NodeInterface> &node) override;

private:
    static void apply(const topology::LogicalSchema &schema, std::unique_ptr<expression::Operation> &operation);
};
} // namespace db::plan::logical