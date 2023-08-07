#include "annotate_predicates_rule.h"
#include <db/expression/operation_builder.h>
#include <db/plan/logical/cardinality_estimator.h>
#include <db/plan/logical/node/selection_node.h>

using namespace db::plan::optimizer;

bool AnnotatePredicatesRule::apply(PlanView &plan)
{
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(logical::SelectionNode))
        {
            auto *selection_node = reinterpret_cast<logical::SelectionNode *>(node);
            const auto selectivity =
                logical::CardinalityEstimator::estimate_selectivity(plan.database(), selection_node->predicate());
            selection_node->predicate()->annotation().selectivity(selectivity);
        }
    }

    return false;
}