#include "remove_projection_rule.h"
#include <db/plan/logical/node/projection_node.h>

using namespace db::plan::optimizer;

bool RemoveProjectionRule::apply(PlanView &plan)
{
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(logical::ProjectionNode))
        {
            auto *projection = reinterpret_cast<logical::ProjectionNode *>(node);
            const auto &projection_schema = projection->relation().schema();
            auto *const child = std::get<0>(plan.children(projection));
            const auto &child_schema = child->relation().schema();

            if (projection_schema == child_schema)
            {
                plan.erase(projection);
                return true;
            }
        }
    }

    return false;
}
