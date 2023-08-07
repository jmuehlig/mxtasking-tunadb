#pragma once
#include <db/expression/operation.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>

namespace db::plan::optimizer {
/**
 * This optimization moves a predicate near to the scan.
 */
class PredicatePushDownRule final : public RuleInterface
{
public:
    constexpr PredicatePushDownRule() noexcept = default;
    ~PredicatePushDownRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return true; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return true; }

private:
    [[nodiscard]] static PlanView::node_t lowest_position(const PlanView &plan, PlanView::node_t current_node,
                                                          std::unique_ptr<expression::Operation> &predicate);

    [[nodiscard]] static bool provides_needed_attributes(PlanView::node_t node,
                                                         const std::unique_ptr<expression::Operation> &predicate);

    [[nodiscard]] static bool push_down_is_worthwhile(const PlanView &plan, PlanView::node_t from, PlanView::node_t to);
};
} // namespace db::plan::optimizer