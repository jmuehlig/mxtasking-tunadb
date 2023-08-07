#pragma once
#include <db/expression/term.h>
#include <db/plan/optimizer/rule_interface.h>
#include <unordered_set>

namespace db::plan::optimizer {
/**
 * This optimization inserts projections before operators that do materialize
 * the records to reduce the amount of materialized data.
 * For example:
 *  SELECT t1.a, t2.b FROM t1, t2 WHERE t1.x = t2.x
 *      => before the JOIN, the schema will be reduced to (t1, [a,x]) and (t2, [b,x]).
 */
class EarlyProjectionRule final : public RuleInterface
{
public:
    constexpr EarlyProjectionRule() noexcept = default;
    ~EarlyProjectionRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return true; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return false; }

private:
    [[nodiscard]] static bool apply(PlanView &plan, PlanView::node_t node,
                                    std::unordered_set<expression::Term> &required_terms);
    [[nodiscard]] static bool insert_projection_after(PlanView &plan, PlanView::node_t node, PlanView::node_t parent,
                                                      const std::unordered_set<expression::Term> &required_terms);
};
} // namespace db::plan::optimizer