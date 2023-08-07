#pragma once
#include <db/expression/operation.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>

namespace db::plan::optimizer {
/**
 * This optimization removes cast operations on fixed values like
 *  CAST(10 as decimal(5,2))
 * by the real value (10.00) since cast of fixed values can be evaluated at
 * planning time.
 */
class RemoveFixedValueCastRule final : public RuleInterface
{
public:
    constexpr RemoveFixedValueCastRule() noexcept = default;
    ~RemoveFixedValueCastRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return false; }
    [[nodiscard]] bool is_multi_pass() const noexcept override { return false; }

private:
    static void apply(std::unique_ptr<expression::Operation> &predicate);
    [[nodiscard]] static bool has_fixed_value_cast(const std::unique_ptr<expression::Operation> &predicate);
};
} // namespace db::plan::optimizer