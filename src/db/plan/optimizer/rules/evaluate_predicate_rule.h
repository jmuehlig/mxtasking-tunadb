#pragma once
#include <db/expression/operation.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>

namespace db::plan::optimizer {
/**
 * This optimization evaluates static operations before execution,
 * for example:
 *  SELECT ... WHERE a BETWEEN 0.05 - 0.01 AND 0.05 + 0.01
 *      => SELECT ... WHERE a BETWEEN 0.04 AND 0.06
 */
class EvaluatePredicateRule final : public RuleInterface
{
public:
    constexpr EvaluatePredicateRule() noexcept = default;
    ~EvaluatePredicateRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return false; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return false; }

private:
    [[nodiscard]] static std::unique_ptr<expression::Operation> evaluate(
        std::unique_ptr<expression::Operation> &&predicate);
    [[nodiscard]] static bool is_evaluable(std::unique_ptr<expression::Operation> &predicate) noexcept;
};
} // namespace db::plan::optimizer