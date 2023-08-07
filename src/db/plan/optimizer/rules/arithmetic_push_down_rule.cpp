#include "arithmetic_push_down_rule.h"
#include <db/plan/logical/node/join_node.h>
#include <db/plan/logical/node/selection_node.h>
#include <db/plan/logical/node/table_node.h>
#include <db/plan/logical/node/table_selection_node.h>

using namespace db::plan::optimizer;

bool ArithmeticPushDownRule::apply(PlanView &plan)
{
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(logical::ArithmeticNode))
        {
            auto *arithmetic = reinterpret_cast<logical::ArithmeticNode *>(node);
            auto *const child = std::get<0>(plan.children(node));

            if (typeid(*child) != typeid(logical::SelectionNode) && typeid(*child) != typeid(logical::TableNode) &&
                typeid(*child) != typeid(logical::TableSelectionNode))
            {
                for (const auto &operation : arithmetic->arithmetic_operations())
                {
                    auto *const target = ArithmeticPushDownRule::lowest_position(plan, child, operation);

                    if (target != nullptr && target != child)
                    {
                        if (typeid(*target) == typeid(logical::ArithmeticNode))
                        {
                            /// Add to the target arithmetic node
                            auto *target_arithmetic_node = reinterpret_cast<logical::ArithmeticNode *>(target);
                            auto arithmetic_operations = std::vector<std::unique_ptr<expression::Operation>>{};
                            arithmetic_operations.reserve(target_arithmetic_node->arithmetic_operations().size() + 1U);
                            for (const auto &arithmetic_operation : target_arithmetic_node->arithmetic_operations())
                            {
                                arithmetic_operations.emplace_back(arithmetic_operation->copy());
                            }
                            arithmetic_operations.emplace_back(operation->copy());
                            plan.replace(target_arithmetic_node,
                                         plan.make_node<logical::ArithmeticNode>(std::move(arithmetic_operations)));

                            /// Remove the operation from "old" artihmetic node.
                            if (arithmetic->arithmetic_operations().size() == 1U)
                            {
                                plan.erase(arithmetic);
                            }
                            else
                            {
                                arithmetic->arithmetic_operations().erase(
                                    std::remove_if(arithmetic->arithmetic_operations().begin(),
                                                   arithmetic->arithmetic_operations().end(),
                                                   [&operation](const auto &op) { return op == operation; }),
                                    arithmetic->arithmetic_operations().end());
                            }

                            return true;
                        }

                        if (ArithmeticPushDownRule::push_down_skips_join(plan, node, target))
                        {
                            /// Create a new arithmetic node
                            auto arithmetic_operations = std::vector<std::unique_ptr<expression::Operation>>{};
                            arithmetic_operations.emplace_back(operation->copy());
                            plan.insert_between(
                                plan.parent(target), target,
                                plan.make_node<logical::ArithmeticNode>(std::move(arithmetic_operations)));

                            /// Remove the operation from "old" arithmetic node.
                            if (arithmetic->arithmetic_operations().size() == 1U)
                            {
                                plan.erase(arithmetic);
                            }
                            else
                            {
                                arithmetic->arithmetic_operations().erase(
                                    std::remove_if(arithmetic->arithmetic_operations().begin(),
                                                   arithmetic->arithmetic_operations().end(),
                                                   [&operation](const auto &op) { return op == operation; }),
                                    arithmetic->arithmetic_operations().end());
                            }

                            return true;
                        }
                    }
                }
            }
        }
    }

    return false;
}

bool ArithmeticPushDownRule::provides_needed_attributes(PlanView::node_t node,
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

PlanView::node_t ArithmeticPushDownRule::lowest_position(const PlanView &plan, PlanView::node_t current_node,
                                                         const std::unique_ptr<expression::Operation> &predicate)
{
    if (current_node == nullptr)
    {
        return current_node;
    }

    if (typeid(*current_node) == typeid(logical::SelectionNode) ||
        typeid(*current_node) == typeid(logical::TableNode) ||
        typeid(*current_node) == typeid(logical::TableSelectionNode) ||
        typeid(*current_node) == typeid(logical::ArithmeticNode))
    {
        return current_node;
    }

    if (current_node->is_unary())
    {
        auto *child = std::get<0>(plan.children(current_node));

        if (ArithmeticPushDownRule::provides_needed_attributes(child, predicate))
        {
            return ArithmeticPushDownRule::lowest_position(plan, child, predicate);
        }
    }
    else if (current_node->is_binary())
    {
        const auto &children = plan.children(current_node);
        if (ArithmeticPushDownRule::provides_needed_attributes(std::get<0>(children), predicate))
        {
            return ArithmeticPushDownRule::lowest_position(plan, std::get<0>(children), predicate);
        }

        if (ArithmeticPushDownRule::provides_needed_attributes(std::get<1>(children), predicate))
        {
            return ArithmeticPushDownRule::lowest_position(plan, std::get<1>(children), predicate);
        }
    }

    return current_node;
}

bool ArithmeticPushDownRule::push_down_skips_join(const PlanView &plan, PlanView::node_t from, PlanView::node_t to)
{
    auto *current = from;
    while (current != to)
    {
        if (typeid(*current) != typeid(logical::JoinNode))
        {
            return true;
        }

        current = std::get<0>(plan.children(current));
    }

    return false;
}