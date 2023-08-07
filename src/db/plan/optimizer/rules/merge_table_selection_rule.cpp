#include "merge_table_selection_rule.h"
#include <db/plan/logical/node/selection_node.h>
#include <db/plan/logical/node/table_node.h>
#include <db/plan/logical/node/table_selection_node.h>
#include <db/plan/logical/table.h>

using namespace db::plan::optimizer;

bool MergeTableSelectionRule::apply(PlanView &plan)
{
    return MergeTableSelectionRule::apply(plan, plan.root());
}

bool MergeTableSelectionRule::apply(db::plan::optimizer::PlanView &plan, logical::NodeInterface *node)
{
    auto child_iterator = PlanViewNodeChildIterator{plan};

    if (node->is_unary())
    {
        auto *child = child_iterator.child(reinterpret_cast<logical::UnaryNode *>(node));

        if (typeid(*node) == typeid(logical::SelectionNode) && typeid(*child) == typeid(logical::TableNode))
        {
            auto *selection = reinterpret_cast<logical::SelectionNode *>(node);
            auto *table = reinterpret_cast<logical::TableNode *>(child);

            plan.replace(table, plan.make_node<logical::TableSelectionNode>(logical::TableReference{table->table()},
                                                                            selection->predicate()->copy()));
            plan.erase(node);

            return true;
        }

        return MergeTableSelectionRule::apply(plan, child);
    }

    if (node->is_binary())
    {
        const auto children = child_iterator.children(reinterpret_cast<logical::BinaryNode *>(node));

        const auto is_left_applied = MergeTableSelectionRule::apply(plan, std::get<0>(children));
        const auto is_right_applied = MergeTableSelectionRule::apply(plan, std::get<1>(children));

        return is_left_applied || is_right_applied;
    }

    return false;
}