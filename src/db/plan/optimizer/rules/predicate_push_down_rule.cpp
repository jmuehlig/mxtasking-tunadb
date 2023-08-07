#include "predicate_push_down_rule.h"
#include <db/plan/logical/node/selection_node.h>

using namespace db::plan::optimizer;

bool PredicatePushDownRule::apply(PlanView &plan)
{
    for (auto *node : plan.extract_nodes())
    {
        if (typeid(*node) == typeid(logical::SelectionNode))
        {
            auto *selection = reinterpret_cast<logical::SelectionNode *>(node);
            auto *const child = std::get<0>(plan.children(node));
            auto *const target = PredicatePushDownRule::lowest_position(plan, child, selection->predicate());

            if (target != nullptr && target != child &&
                PredicatePushDownRule::push_down_is_worthwhile(plan, node, target))
            {
                if (plan.move_between(plan.parent(target), target, node))
                {
                    return true;
                }
            }
        }
    }

    return false;
}

bool PredicatePushDownRule::provides_needed_attributes(PlanView::node_t node,
                                                       const std::unique_ptr<expression::Operation> &predicate)
{
    auto provides_needed_attributes = true;
    const auto &schema = node->relation().schema();
    expression::for_each_term(predicate, [&provides_needed_attributes, &schema](const expression::Term &term) {
        if (term.is_attribute())
        {
            provides_needed_attributes &= schema.contains(term);
        }
    });
    return provides_needed_attributes;
}

PlanView::node_t PredicatePushDownRule::lowest_position(const PlanView &plan, PlanView::node_t current_node,
                                                        std::unique_ptr<expression::Operation> &predicate)
{
    if (current_node == nullptr)
    {
        return current_node;
    }

    if (current_node->is_unary())
    {
        auto *child = std::get<0>(plan.children(current_node));
        if (PredicatePushDownRule::provides_needed_attributes(child, predicate))
        {
            return PredicatePushDownRule::lowest_position(plan, child, predicate);
        }
    }
    else if (current_node->is_binary())
    {
        const auto &children = plan.children(current_node);
        if (PredicatePushDownRule::provides_needed_attributes(std::get<0>(children), predicate))
        {
            return PredicatePushDownRule::lowest_position(plan, std::get<0>(children), predicate);
        }

        if (PredicatePushDownRule::provides_needed_attributes(std::get<1>(children), predicate))
        {
            return PredicatePushDownRule::lowest_position(plan, std::get<1>(children), predicate);
        }
    }

    return current_node;
}

bool PredicatePushDownRule::push_down_is_worthwhile(const PlanView &plan, PlanView::node_t from, PlanView::node_t to)
{
    auto *current = from;
    while (current != to)
    {
        if (typeid(*current) != typeid(logical::SelectionNode))
        {
            return true;
        }

        current = std::get<0>(plan.children(current));
    }

    return false;
}