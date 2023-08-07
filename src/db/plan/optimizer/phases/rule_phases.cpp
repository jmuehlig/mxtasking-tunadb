#include "rule_phases.h"
#include <db/plan/optimizer/rules/annotate_predicates_rule.h>
#include <db/plan/optimizer/rules/arithmetic_push_down_rule.h>
#include <db/plan/optimizer/rules/condense_range_predicates_to_between_rule.h>
#include <db/plan/optimizer/rules/early_projection_rule.h>
#include <db/plan/optimizer/rules/evaluate_predicate_rule.h>
#include <db/plan/optimizer/rules/merge_order_by_limit_rule.h>
#include <db/plan/optimizer/rules/merge_predicates_rule.h>
#include <db/plan/optimizer/rules/merge_table_selection_rule.h>
#include <db/plan/optimizer/rules/physical_operator_rule.h>
#include <db/plan/optimizer/rules/pre_selection_rule.h>
#include <db/plan/optimizer/rules/predicate_push_down_rule.h>
#include <db/plan/optimizer/rules/remove_fixed_value_cast_rule.h>
#include <db/plan/optimizer/rules/remove_projection_rule.h>
#include <db/plan/optimizer/rules/split_arithmetic_rule.h>

using namespace db::plan::optimizer;

std::pair<bool, PlanView> RulePhaseInterface::apply(db::plan::optimizer::PlanView &&plan_view)
{
    bool is_optimized = false;
    auto child_iterator = PlanViewNodeChildIterator{plan_view};

    for (auto &optimizer_rule : this->_rules)
    {
        const auto is_affect_relation = optimizer_rule->is_affect_relation();

        if (optimizer_rule->is_multi_pass())
        {
            while (optimizer_rule->apply(plan_view))
            {
                is_optimized = true;
                if (is_affect_relation)
                {
                    std::ignore = plan_view.root()->emit_relation(plan_view.database(), child_iterator, false);
                }
            }
        }
        else
        {
            const auto is_applied = optimizer_rule->apply(plan_view);
            if (is_applied && is_affect_relation)
            {
                std::ignore = plan_view.root()->emit_relation(plan_view.database(), child_iterator, false);
            }
            is_optimized |= is_applied;
        }
    }

    return std::make_pair(is_optimized, std::move(plan_view));
}

ExpressionSimplificationPhase::ExpressionSimplificationPhase()
{
    this->add<RemoveFixedValueCastRule>();
    this->add<EvaluatePredicateRule>();
    this->add<MergeOrderByLimitRule>();
}

PredicatePushdownPhase::PredicatePushdownPhase()
{
    this->add<PredicatePushDownRule>();
    this->add<CondenseRangePredicatesToBetweenRule>();
    this->add<AnnotatePredicatesRule>();
    this->add<MergePredicatesRule>();
}

EarlySelectionPhase::EarlySelectionPhase()
{
    this->add<PreSelectionRule>();

    /// The PreSelectionOptimization may produce new predicates
    /// that can be merged and pushed down.
    this->add<PredicatePushDownRule>();
    this->add<MergePredicatesRule>();

    this->add<MergeTableSelectionRule>();
}

EarlyProjectionPhase::EarlyProjectionPhase()
{
    /// Early Projection
    this->add<SplitArithmeticRule>();
    this->add<ArithmeticPushDownRule>();
    this->add<EarlyProjectionRule>();

    /// Redundant Projection
    //    if (is_remove_redundant_projection)
    //    {
    //        this->add<RemoveProjectionRule>();
    //    }
}

PhysicalOperatorMappingPhase::PhysicalOperatorMappingPhase()
{
    this->add<PhysicalOperatorRule>();
}