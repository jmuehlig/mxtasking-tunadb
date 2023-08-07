#pragma once

#include "adjustment_interface.h"
#include <db/expression/operation.h>
#include <memory>

namespace db::plan::logical {
/**
 * This adjustment unifies operations before execution.
 * As a result, the attribute will be on the left hand site
 * and the static value will be on the right.
 * for example:
 *  SELECT ... WHERE a < 5 AND 5 > b
 *      => SELECT ... WHERE a < 5 AND b < 5
 */
class PredicateValueRightOfAttributeAdjustment final : public AdjustmentInterface
{
public:
    constexpr PredicateValueRightOfAttributeAdjustment() noexcept = default;
    ~PredicateValueRightOfAttributeAdjustment() noexcept override = default;

    void apply(std::unique_ptr<NodeInterface> &node) override;

private:
    static void apply(std::unique_ptr<expression::Operation> &operation);
};
} // namespace db::plan::logical