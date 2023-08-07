#pragma once
#include <db/expression/operation.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>

namespace db::plan::optimizer {
/**
 * This optimization removes the projection operator when its
 * child already produces the wanted schema.
 */
class RemoveProjectionRule final : public RuleInterface
{
public:
    constexpr RemoveProjectionRule() noexcept = default;
    ~RemoveProjectionRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return false; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return true; }
};
} // namespace db::plan::optimizer