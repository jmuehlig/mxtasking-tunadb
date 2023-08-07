#pragma once

#include "plan_view.h"

namespace db::plan::optimizer {
class RuleInterface
{
public:
    constexpr RuleInterface() noexcept = default;
    virtual ~RuleInterface() = default;

    /**
     * Optimizes the given plan.
     *
     * @param plan Plan to optimize.
     * @return True, if an optimization was performed and probably a second step is needed.
     */
    [[nodiscard]] virtual bool apply(PlanView &plan) = 0;

    /**
     * @return True, if this rule affects the schema or cardinality.
     */
    [[nodiscard]] virtual bool is_affect_relation() const noexcept = 0;

    /**
     * @return True, if the rule requires multiple passes.
     */
    [[nodiscard]] virtual bool is_multi_pass() const noexcept = 0;
};
} // namespace db::plan::optimizer