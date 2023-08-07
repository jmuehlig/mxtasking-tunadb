#pragma once

#include "adjustment_interface.h"
#include <db/expression/operation.h>
#include <memory>

namespace db::plan::logical {
/**
 * This adjustment verifies, that join predicates are build like
 *      JOIN foo on bar.id = foo.id
 *  WHERE bar is the left side of the join and foo is the right side.
 *  With other words, for a join operation (x, y) it will be guaranteed
 *  that the x-concerning predicate is left and the y-predicate is right.
 *  Back to the example (join(bar,foo)),
 *      JOIN foo on foo.id = bar.id will be replaced by
 *      JOIN foo on bar.id = foo.id
 */
class JoinPredicateLeftRightAdjustment final : public AdjustmentInterface
{
public:
    constexpr JoinPredicateLeftRightAdjustment() noexcept = default;
    ~JoinPredicateLeftRightAdjustment() noexcept override = default;

    void apply(std::unique_ptr<NodeInterface> &node) override;

private:
    static void apply(const std::unique_ptr<NodeInterface> &left, const std::unique_ptr<NodeInterface> &right,
                      std::unique_ptr<expression::Operation> &operation);
};
} // namespace db::plan::logical