#pragma once

#include "adjustment_interface.h"
#include <db/expression/operation.h>
#include <db/topology/logical_schema.h>
#include <memory>
#include <vector>

namespace db::plan::logical {
/**
 * This adjustment scans an aggregation node and adds
 * an arithmetic node as a child whenever the aggregation
 * has arithmetic included. This is only needed for compile
 * execution engine.
 */
class AddArithmeticNodeForAggregationNodeAdjustment final : public AdjustmentInterface
{
public:
    constexpr AddArithmeticNodeForAggregationNodeAdjustment() noexcept = default;
    ~AddArithmeticNodeForAggregationNodeAdjustment() noexcept override = default;

    void apply(std::unique_ptr<NodeInterface> &node) override;

private:
    /**
     * Extracts arithmetic expression from the predicate and inserts
     * them into the given vector of arithmetics. Expressions in the predicate
     * will be replaced by attributes accessing the generated arithmetic.
     *
     * @param predicate Predicate to extract arithmetics from.
     * @param arithmetics List of arithmetics.
     */
    static void extract_arithmetic(std::unique_ptr<expression::Operation> &predicate,
                                   std::vector<std::unique_ptr<expression::Operation>> &arithmetics);
};
} // namespace db::plan::logical