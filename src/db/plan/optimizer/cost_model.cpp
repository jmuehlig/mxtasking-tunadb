#include "cost_model.h"
#include <db/plan/logical/node/join_node.h>

using namespace db::plan::optimizer;

std::uint64_t CostModel::estimate(const PlanView &plan)
{
    auto child_iterator = PlanViewNodeChildIterator{plan};
    std::ignore = plan.root()->emit_relation(plan.database(), child_iterator, true);

    auto cost = 0UL;
    for (const auto &[node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(logical::JoinNode))
        {
            const auto &children = plan.children(node);

            /// Only the build cost.
            cost += std::get<0>(children)->relation().cardinality();
        }
    }

    return cost;
}
