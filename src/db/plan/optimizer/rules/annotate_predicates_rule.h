#pragma once
#include <db/expression/operation.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>

namespace db::plan::optimizer {
/**
 * This optimization calculates the selectivity of a
 * filter predicate and annotates it to the expression.
 */
class AnnotatePredicatesRule final : public RuleInterface
{
public:
    constexpr AnnotatePredicatesRule() noexcept = default;
    ~AnnotatePredicatesRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return false; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return false; }
};
} // namespace db::plan::optimizer