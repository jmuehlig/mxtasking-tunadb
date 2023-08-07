#pragma once
#include <db/expression/operation.h>
#include <db/plan/logical/node/arithmetic_node.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>

namespace db::plan::optimizer {
/**
 * This optimization moves arithmetic near to the scan.
 */
class ArithmeticPushDownRule final : public RuleInterface
{
public:
    constexpr ArithmeticPushDownRule() noexcept = default;
    ~ArithmeticPushDownRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return true; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return true; }

private:
    [[nodiscard]] static PlanView::node_t lowest_position(const PlanView &plan, PlanView::node_t current_node,
                                                          const std::unique_ptr<expression::Operation> &predicate);

    [[nodiscard]] static bool provides_needed_attributes(PlanView::node_t node,
                                                         const std::unique_ptr<expression::Operation> &predicate);

    [[nodiscard]] static bool push_down_skips_join(const PlanView &plan, PlanView::node_t from, PlanView::node_t to);
};
} // namespace db::plan::optimizer