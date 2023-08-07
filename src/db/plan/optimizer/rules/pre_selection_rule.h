#pragma once
#include <db/data/value.h>
#include <db/expression/operation.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace db::plan::optimizer {
/**
 * This optimization adds extra predicates
 * whenever a predicate can not be pushed down to the
 * scan but partially tuples could be filtered out.
 */
class PreSelectionRule final : public RuleInterface
{
public:
    PreSelectionRule() noexcept = default;
    ~PreSelectionRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return true; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return true; }

private:
    std::unordered_set<PlanView::node_t> _optimized_nodes;
    [[nodiscard]] static bool has_multiple_sources(const std::unique_ptr<expression::Operation> &predicate);
    [[nodiscard]] static std::unordered_map<expression::Term, std::vector<expression::BinaryOperation *>>
    extract_predicates_per_attribute(const std::unique_ptr<expression::Operation> &predicate);

    /**
     * Tests if the given predicates contains a < or <=, a > or >=, and no = or !=.
     *
     * @param predicates List of predicates.
     * @return True, if this can be substituted by a range predicate.
     */
    [[nodiscard]] static bool is_range(const std::vector<expression::BinaryOperation *> &predicates);

    /**
     * Validates if the predicate is a BETWEEN and both operands are values.
     *
     * @param predicate Predicate to validate.
     * @return True, if both operands are values.
     */
    [[nodiscard]] static bool is_qualified_between(expression::BinaryOperation *predicate);

    [[nodiscard]] static data::Value adjust_to_lesser_equals(data::Value value);
    [[nodiscard]] static data::Value adjust_to_greater_equals(data::Value value);
};
} // namespace db::plan::optimizer