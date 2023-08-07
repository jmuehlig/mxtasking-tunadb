#include "merge_order_by_limit_rule.h"
#include <db/plan/logical/node/limit_node.h>
#include <db/plan/logical/node/order_by_node.h>
#include <db/plan/logical/table.h>

using namespace db::plan::optimizer;

bool MergeOrderByLimitRule::apply(PlanView &plan)
{
    return MergeOrderByLimitRule::apply(plan, plan.root());
}

bool MergeOrderByLimitRule::apply(db::plan::optimizer::PlanView &plan, logical::NodeInterface *node)
{
    auto child_iterator = PlanViewNodeChildIterator{plan};

    if (node->is_unary())
    {
        auto *child = child_iterator.child(reinterpret_cast<logical::UnaryNode *>(node));

        if (typeid(*node) == typeid(logical::LimitNode) && typeid(*child) == typeid(logical::OrderByNode))
        {
            auto *limit = reinterpret_cast<logical::LimitNode *>(node);
            auto *order_by = reinterpret_cast<logical::OrderByNode *>(child);

            order_by->limit(limit->limit());
            plan.erase(node);

            return true;
        }

        return MergeOrderByLimitRule::apply(plan, child);
    }

    if (node->is_binary())
    {
        const auto children = child_iterator.children(reinterpret_cast<logical::BinaryNode *>(node));

        const auto is_left_applied = MergeOrderByLimitRule::apply(plan, std::get<0>(children));
        const auto is_right_applied = MergeOrderByLimitRule::apply(plan, std::get<1>(children));

        return is_left_applied || is_right_applied;
    }

    return false;
}