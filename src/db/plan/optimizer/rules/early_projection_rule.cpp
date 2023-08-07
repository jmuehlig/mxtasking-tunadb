#include "early_projection_rule.h"
#include <db/plan/logical/node/aggregation_node.h>
#include <db/plan/logical/node/arithmetic_node.h>
#include <db/plan/logical/node/join_node.h>
#include <db/plan/logical/node/order_by_node.h>
#include <db/plan/logical/node/projection_node.h>
#include <db/plan/logical/node/selection_node.h>
#include <iostream>

using namespace db::plan::optimizer;

bool EarlyProjectionRule::apply(PlanView &plan)
{
    auto required_terms = std::unordered_set<expression::Term>{};
    return EarlyProjectionRule::apply(plan, plan.root(), required_terms);
}

bool EarlyProjectionRule::apply(PlanView &plan, PlanView::node_t node,
                                std::unordered_set<expression::Term> &required_terms)
{
    if (node->is_binary())
    {
        auto inserted_early_projection = false;

        if (typeid(*node) == typeid(logical::JoinNode))
        {
            auto *join_node = reinterpret_cast<logical::JoinNode *>(node);

            expression::for_each_term(join_node->predicate(), [&required_terms](const auto &term) {
                required_terms.insert(expression::Term{term});
            });

            /// We try to insert a projection before the join because joins materialize data.
            const auto &children = plan.children(node);
            inserted_early_projection |=
                EarlyProjectionRule::insert_projection_after(plan, std::get<0>(children), node, required_terms);
            inserted_early_projection |=
                EarlyProjectionRule::insert_projection_after(plan, std::get<1>(children), node, required_terms);
        }

        {
            const auto &children = plan.children(node);
            inserted_early_projection |= EarlyProjectionRule::apply(plan, std::get<0>(children), required_terms);
            inserted_early_projection |= EarlyProjectionRule::apply(plan, std::get<1>(children), required_terms);
        }

        return inserted_early_projection;
    }

    if (node->is_unary())
    {
        if (typeid(*node) == typeid(logical::SelectionNode))
        {
            expression::for_each_term(reinterpret_cast<logical::SelectionNode *>(node)->predicate(),
                                      [&required_terms](const auto &term) {
                                          if (term.is_attribute())
                                          {
                                              required_terms.insert(expression::Term{term});
                                          }
                                      });
        }
        else if (typeid(*node) == typeid(logical::ArithmeticNode))
        {
            for (const auto &operation : reinterpret_cast<logical::ArithmeticNode *>(node)->arithmetic_operations())
            {
                expression::for_each_term(operation, [&required_terms](const auto &term) {
                    if (term.is_attribute() && term.template get<expression::Attribute>().is_asterisk() == false)
                    {
                        required_terms.insert(expression::Term{term});
                    }
                });
            }

            auto inserted_early_projection = false;
            {
                const auto &children = plan.children(node);
                inserted_early_projection =
                    EarlyProjectionRule::insert_projection_after(plan, std::get<0>(children), node, required_terms);
            }

            const auto &children = plan.children(node);
            return EarlyProjectionRule::apply(plan, std::get<0>(children), required_terms) || inserted_early_projection;
        }
        else if (typeid(*node) == typeid(logical::AggregationNode))
        {
            for (const auto &operation : reinterpret_cast<logical::AggregationNode *>(node)->aggregation_operations())
            {
                expression::for_each_term(operation, [&required_terms](const auto &term) {
                    if (term.is_attribute() && term.template get<expression::Attribute>().is_asterisk() == false)
                    {
                        required_terms.insert(expression::Term{term});
                    }
                });
            }

            const auto &groups = reinterpret_cast<logical::AggregationNode *>(node)->groups();
            if (groups.has_value())
            {
                for (const auto &term : groups.value())
                {
                    required_terms.insert(expression::Term{term});
                }
            }

            auto inserted_early_projection = false;
            {
                const auto &children = plan.children(node);
                inserted_early_projection =
                    EarlyProjectionRule::insert_projection_after(plan, std::get<0>(children), node, required_terms);
            }

            const auto &children = plan.children(node);
            return EarlyProjectionRule::apply(plan, std::get<0>(children), required_terms) || inserted_early_projection;
        }
        else if (typeid(*node) == typeid(logical::ProjectionNode))
        {
            const auto &relation = node->relation();
            for (const auto &term : relation.schema().terms())
            {
                if (term.is_attribute())
                {
                    required_terms.insert(expression::Term{term});
                }
            }
        }

        const auto &children = plan.children(node);
        return EarlyProjectionRule::apply(plan, std::get<0>(children), required_terms);
    }

    return false;
}

bool EarlyProjectionRule::insert_projection_after(PlanView &plan, PlanView::node_t node, PlanView::node_t parent,
                                                  const std::unordered_set<expression::Term> &required_terms)
{
    const auto &relation = node->relation();
    const auto &schema = relation.schema();
    auto needed_terms = std::vector<expression::Term>{};
    needed_terms.reserve(schema.size());

    for (auto i = 0U; i < schema.size(); ++i)
    {
        if (required_terms.contains(schema.term(i)))
        {
            needed_terms.push_back(schema.term(i));
        }
    }

    if (needed_terms.size() < schema.size())
    {
        plan.insert_between(parent, node, plan.make_node<logical::ProjectionNode>(std::move(needed_terms)));
        return true;
    }

    return false;
}