#pragma once
#include <db/expression/operation.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>

namespace db::plan::optimizer {
/**
 * This optimization merges ORDER BY and LIMIT nodes and pushes
 * the LIMIT data into the ORDER BY node.
 */
class MergeOrderByLimitRule final : public RuleInterface
{
public:
    constexpr MergeOrderByLimitRule() noexcept = default;
    ~MergeOrderByLimitRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return true; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return false; }

private:
    [[nodiscard]] static bool apply(PlanView &plan, logical::NodeInterface *node);
};
} // namespace db::plan::optimizer