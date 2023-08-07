#pragma once
#include <db/expression/operation.h>
#include <db/expression/term.h>
#include <db/plan/optimizer/rule_interface.h>
#include <unordered_set>

namespace db::plan::optimizer {
/**
 * This optimization reduces two range predicates (i.e., a > b and a < c)
 * to between predicates (i.e., a between (b+1, c-1)) which are easier
 * to predict in terms of estimate_selectivity.
 */
class CondenseRangePredicatesToBetweenRule final : public RuleInterface
{
public:
    constexpr CondenseRangePredicatesToBetweenRule() noexcept = default;
    ~CondenseRangePredicatesToBetweenRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return false; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return true; }

private:
    [[nodiscard]] static bool is_lesser_or_lesser_equal(const expression::Operation::Id id)
    {
        return id == expression::Operation::Id::Lesser || id == expression::Operation::Id::LesserEquals;
    }

    [[nodiscard]] static bool is_greater_or_greater_equal(const expression::Operation::Id id)
    {
        return id == expression::Operation::Id::Greater || id == expression::Operation::Id::GreaterEquals;
    }

    [[nodiscard]] static data::Value adjust_to_lesser_equals(data::Value value);
    [[nodiscard]] static data::Value adjust_to_greater_equals(data::Value value);
};
} // namespace db::plan::optimizer