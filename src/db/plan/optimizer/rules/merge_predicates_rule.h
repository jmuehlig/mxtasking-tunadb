#pragma once
#include <db/expression/operation.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>

namespace db::plan::optimizer {
/**
 * This optimization merges predicates that are on top of each
 * other in the logical plan.
 */
class MergePredicatesRule final : public RuleInterface
{
public:
    constexpr MergePredicatesRule() noexcept = default;
    ~MergePredicatesRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return true; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return true; }

private:
    [[nodiscard]] static std::unique_ptr<expression::Operation> merge(
        std::vector<std::pair<float, std::unique_ptr<expression::Operation>>> &&predicates);

    [[nodiscard]] static bool is_join_predicate(const std::unique_ptr<expression::Operation> &predicate);
};
} // namespace db::plan::optimizer