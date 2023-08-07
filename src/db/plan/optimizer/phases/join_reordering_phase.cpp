#include "join_reordering_phase.h"
#include <db/expression/operation_builder.h>
#include <db/plan/logical/node/cross_product_node.h>
#include <db/plan/logical/node/selection_node.h>
#include <db/plan/logical/node/table_node.h>
#include <db/plan/logical/node/table_selection_node.h>
#include <db/plan/optimizer/cost_model.h>
#include <iostream>
using namespace db::plan::optimizer;

std::pair<bool, PlanView> JoinReorderingPhase::apply(db::plan::optimizer::PlanView &&plan_view)
{
    auto child_iterator = PlanViewNodeChildIterator{plan_view};

    /// Extract the initial pipelines with TABLE at the end and all joins.
    auto source_pipelines = std::vector<logical::NodeInterface *>{};
    auto join_predicate_nodes = std::vector<std::pair<logical::NodeInterface *, expression::Operation *>>{};
    JoinReorderingPhase::extract_source_pipelines_and_join_predicates(child_iterator, plan_view.root(),
                                                                      source_pipelines, join_predicate_nodes);

    /// No joins found.
    if (source_pipelines.size() < 2U || join_predicate_nodes.empty())
    {
        return std::make_pair(false, std::move(plan_view));
    }

    /// Create real join predicates.
    auto join_predicates = std::vector<std::unique_ptr<expression::Operation>>{};
    std::transform(join_predicate_nodes.begin(), join_predicate_nodes.end(), std::back_inserter(join_predicates),
                   [](const auto join_predicate_node) { return std::get<1>(join_predicate_node)->copy(); });

    /// Extract a set of all needed terms.
    auto needed_term_set = JoinReorderingPhase::extract_needed_terms(join_predicates);

    /// Add transitive join predicates.
    JoinReorderingPhase::add_transitive_predicates(join_predicates);

    /// Build the initial join plans (A JOIN B, [C, D]), (A JOIN C, [B, D]), ...
    auto join_plans = JoinReorderingPhase::make_initial_step(plan_view, source_pipelines, join_predicates);

    /// Build up join plans: In every step take one additional pipeline and join,
    /// until every join plan has no pending pipelines.
    while (JoinReorderingPhase::has_included_all_pipelines(join_plans) == false)
    {
        auto join_plans_step = std::vector<JoinPlan>{};
        join_plans_step.reserve(512U);

        for (auto &join_plan : join_plans)
        {
            if (join_plan.has_included_all_pipelines() == false)
            {
                JoinReorderingPhase::make_step(std::move(join_plan), plan_view, join_plans_step);
            }
            else
            {
                join_plans_step.emplace_back(std::move(join_plan));
            }
        }

        join_plans.clear();
        std::sort(join_plans_step.begin(), join_plans_step.end(),
                  [](const auto &left, const auto &right) { return left.cost() < right.cost(); });

        const auto plans_to_keep =
            join_plans_step.size() < 25 ? join_plans_step.size() : (join_plans_step.size() * .55);
        std::move(join_plans_step.begin(), join_plans_step.begin() + plans_to_keep, std::back_inserter(join_plans));
    }

    if (join_plans.empty())
    {
        return std::make_pair(false, std::move(plan_view));
    }

    /// Find the join plan with minimal costs.
    auto &min_join_plan = join_plans.front();

    /// Add missing join predicates.
    JoinReorderingPhase::complement_missing_join_predicates(min_join_plan, std::move(needed_term_set));

    /// Get a sub plan of the original plan, containing only nodes
    /// from the top until the first join.
    auto top_plan = plan_view.subplan_until_join();

    /// Erase all join predicate nodes that where used for reordering.
    while (join_predicate_nodes.empty() == false)
    {
        auto [original_node, _] = join_predicate_nodes.back();
        join_predicate_nodes.pop_back();

        for (auto [node, _2] : top_plan.nodes_and_parent())
        {
            if (node == original_node)
            {
                top_plan.erase(node);
                break;
            }
        }
    }

    /// Append the join plan to the top plan.
    for (auto [node, _] : top_plan.nodes_and_parent())
    {
        /// Find the last element in the sub plan...
        if (top_plan.has_children(node) == false || std::get<0>(top_plan.children(node)) == nullptr)
        {
            /// ... and append the join plan.
            top_plan.insert_after(node, std::move(min_join_plan.plan()));
            return std::make_pair(true, std::move(top_plan));
        }
    }

    return std::make_pair(false, std::move(plan_view));
}

std::vector<db::plan::optimizer::JoinReorderingPhase::JoinPlan> JoinReorderingPhase::make_initial_step(
    const db::plan::optimizer::PlanView &plan, const std::vector<logical::NodeInterface *> &source_pipelines,
    const std::vector<std::unique_ptr<expression::Operation>> &join_predicates)
{
    auto join_plans = std::vector<JoinPlan>{};

    /// Build the plans for the first pass.
    for (auto i = 0U; i < source_pipelines.size() - 1U; ++i)
    {
        for (auto j = i + 1U; j < source_pipelines.size(); ++j)
        {
            auto *first = source_pipelines[i];
            auto *second = source_pipelines[j];

            for (const auto &join_predicate : join_predicates)
            {
                if (JoinReorderingPhase::is_join_possible(first, second, join_predicate))
                {
                    auto [left_child, right_child] = JoinReorderingPhase::join_child_order(first, second);

                    /// Build the join plan view.
                    auto *new_join_node = new logical::JoinNode{join_predicate->copy()};
                    auto join_plan_view = PlanView{plan.database(), new_join_node};
                    join_plan_view.insert_after(new_join_node, plan.subplan(left_child));
                    join_plan_view.insert_after(new_join_node, plan.subplan(right_child));

                    /// Calculate the cost.
                    const auto join_plan_cost = CostModel::estimate(join_plan_view);

                    /// Create the plan and copy pending pipelines and joins.
                    auto join_plan = JoinPlan{std::move(join_plan_view), join_plan_cost};
                    join_plan.copy_pipelines_without(source_pipelines, left_child, right_child);
                    join_plan.copy_predicates_without(join_predicates, join_predicate);
                    join_plans.emplace_back(std::move(join_plan));
                }
            }
        }
    }

    return join_plans;
}

void JoinReorderingPhase::make_step(db::plan::optimizer::JoinReorderingPhase::JoinPlan &&join_plan,
                                    const PlanView &original_plan_view,
                                    std::vector<db::plan::optimizer::JoinReorderingPhase::JoinPlan> &plans)
{
    auto *root = join_plan.plan().root();

    for (auto *pending_pipeline : join_plan.pending_pipelines())
    {
        for (auto *pending_predicate : join_plan.pending_join_predicates())
        {
            if (JoinReorderingPhase::is_join_possible(root, pending_pipeline, pending_predicate))
            {
                auto [left_child, right_child] = JoinReorderingPhase::join_child_order(root, pending_pipeline);
                auto *new_join_node = new logical::JoinNode{pending_predicate->copy()};

                auto join_plan_view = PlanView{original_plan_view.database(), new_join_node};
                if (left_child == root)
                {
                    join_plan_view.insert_after(new_join_node, PlanView{join_plan.plan()});
                    join_plan_view.insert_after(new_join_node, original_plan_view.subplan(right_child));
                }
                else
                {
                    join_plan_view.insert_after(new_join_node, original_plan_view.subplan(left_child));
                    join_plan_view.insert_after(new_join_node, PlanView{join_plan.plan()});
                }

                const auto join_plan_cost = CostModel::estimate(join_plan_view);
                auto new_join_plan = JoinPlan{std::move(join_plan_view), join_plan_cost};
                new_join_plan.copy_pipelines_without(join_plan.pending_pipelines(), pending_pipeline);
                new_join_plan.copy_predicates_without(join_plan.pending_join_predicates(), pending_predicate);

                plans.emplace_back(std::move(new_join_plan));
            }
        }
    }
}

void JoinReorderingPhase::extract_source_pipelines_and_join_predicates(
    const PlanViewNodeChildIterator &child_iterator, logical::NodeInterface *node,
    std::vector<logical::NodeInterface *> &source_pipelines,
    std::vector<std::pair<logical::NodeInterface *, expression::Operation *>> &join_predicate_nodes)
{
    auto *current_node = node;
    while (typeid(*current_node) != typeid(logical::TableNode) &&
           typeid(*current_node) != typeid(logical::TableSelectionNode))
    {
        if (current_node->is_binary())
        {
            auto *binary_node = reinterpret_cast<logical::BinaryNode *>(current_node);

            if (typeid(*current_node) == typeid(logical::JoinNode))
            {
                join_predicate_nodes.emplace_back(
                    current_node, reinterpret_cast<logical::JoinNode *>(current_node)->predicate().get());
            }

            const auto children = child_iterator.children(binary_node);
            JoinReorderingPhase::extract_source_pipelines_and_join_predicates(child_iterator, std::get<0>(children),
                                                                              source_pipelines, join_predicate_nodes);
            JoinReorderingPhase::extract_source_pipelines_and_join_predicates(child_iterator, std::get<1>(children),
                                                                              source_pipelines, join_predicate_nodes);
            return;
        }

        if (current_node->is_unary())
        {
            if (typeid(*current_node) == typeid(logical::SelectionNode))
            {
                const auto &predicate = reinterpret_cast<logical::SelectionNode *>(current_node)->predicate();
                if (JoinReorderingPhase::is_join_predicate(predicate))
                {
                    join_predicate_nodes.emplace_back(current_node, predicate.get());
                }
            }

            current_node = child_iterator.child(reinterpret_cast<logical::UnaryNode *>(current_node));
        }
    }

    source_pipelines.emplace_back(node);
}

bool JoinReorderingPhase::is_join_predicate(const std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->id() == expression::Operation::Id::Equals)
    {
        auto *binary_expression = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        return binary_expression->left_child()->result()->is_attribute() &&
               binary_expression->right_child()->result()->is_attribute();
    }

    return false;
}

std::unordered_set<db::expression::Term> JoinReorderingPhase::extract_needed_terms(
    const std::vector<std::unique_ptr<expression::Operation>> &join_predicates)
{
    auto term_set = std::unordered_set<expression::Term>{};
    for (const auto &join_predicate : join_predicates)
    {
        expression::for_each_term(join_predicate, [&term_set](const auto &term) {
            if (term_set.contains(term) == false)
            {
                term_set.insert(term);
            }
        });
    }

    return term_set;
}

void JoinReorderingPhase::add_transitive_predicates(
    std::vector<std::unique_ptr<expression::Operation>> &join_predicates)
{
    auto transitive_join_predicates = std::vector<std::unique_ptr<expression::Operation>>{};

    for (const auto &outer_join_predicate : join_predicates)
    {
        if (outer_join_predicate->id() == expression::Operation::Id::Equals)
        {
            auto *outer_binary_predicate = reinterpret_cast<expression::BinaryOperation *>(outer_join_predicate.get());

            for (const auto &inner_join_predicate : join_predicates)
            {
                if (outer_join_predicate != inner_join_predicate)
                {
                    if (inner_join_predicate->id() == expression::Operation::Id::Equals)
                    {
                        auto inner_term = JoinReorderingPhase::contains_term(
                            inner_join_predicate, outer_binary_predicate->left_child()->result().value());
                        if (inner_term.has_value())
                        {
                            auto transitive_predicate = expression::OperationBuilder::make_eq(
                                std::make_unique<expression::NullaryOperation>(
                                    expression::Term{outer_binary_predicate->right_child()->result().value()}),
                                std::make_unique<expression::NullaryOperation>(std::move(inner_term.value())));
                            transitive_join_predicates.emplace_back(std::move(transitive_predicate));
                        }
                        else
                        {
                            inner_term = JoinReorderingPhase::contains_term(
                                inner_join_predicate, outer_binary_predicate->right_child()->result().value());
                            if (inner_term.has_value())
                            {
                                auto transitive_predicate = expression::OperationBuilder::make_eq(
                                    std::make_unique<expression::NullaryOperation>(
                                        expression::Term{outer_binary_predicate->left_child()->result().value()}),
                                    std::make_unique<expression::NullaryOperation>(std::move(inner_term.value())));
                                transitive_join_predicates.emplace_back(std::move(transitive_predicate));
                            }
                        }
                    }
                }
            }
        }
    }

    if (transitive_join_predicates.empty() == false)
    {
        std::move(transitive_join_predicates.begin(), transitive_join_predicates.end(),
                  std::back_inserter(join_predicates));
    }
}

std::optional<db::expression::Term> JoinReorderingPhase::contains_term(
    const std::unique_ptr<expression::Operation> &predicate, const expression::Term &term)
{
    if (predicate->id() == expression::Operation::Id::Equals)
    {
        auto *binary_predicate = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        if (binary_predicate->left_child()->result().value() == term &&
            binary_predicate->right_child()->result()->is_attribute())
        {
            return std::make_optional(binary_predicate->right_child()->result().value());
        }

        if (binary_predicate->right_child()->result().value() == term &&
            binary_predicate->left_child()->result()->is_attribute())
        {
            return std::make_optional(binary_predicate->left_child()->result().value());
        }
    }

    return std::nullopt;
}

bool JoinReorderingPhase::is_join_possible(logical::NodeInterface *first, logical::NodeInterface *second,
                                           expression::Operation *join_predicate)
{
    auto *binary_predicate = reinterpret_cast<expression::BinaryOperation *>(join_predicate);

    if (join_predicate->id() == expression::Operation::Id::Equals)
    {
        const auto &left_term = binary_predicate->left_child()->result().value();
        const auto &right_term = binary_predicate->right_child()->result().value();

        return (first->relation().schema().contains(left_term) && second->relation().schema().contains(right_term)) ||
               (first->relation().schema().contains(right_term) && second->relation().schema().contains(left_term));
    }

    if (join_predicate->id() == expression::Operation::Id::And)
    {
        return JoinReorderingPhase::is_join_possible(first, second, binary_predicate->left_child()) &&
               JoinReorderingPhase::is_join_possible(first, second, binary_predicate->right_child());
    }

    return false;
}

std::pair<db::plan::logical::NodeInterface *, db::plan::logical::NodeInterface *> JoinReorderingPhase::join_child_order(
    logical::NodeInterface *first, logical::NodeInterface *second)
{
    const auto first_cardinality = first->relation().cardinality();
    const auto second_cardinality = second->relation().cardinality();

    /// Lower is build side ( = left ).
    if (second_cardinality < first_cardinality)
    {
        return std::make_pair(second, first);
    }

    return std::make_pair(first, second);
}

void JoinReorderingPhase::complement_missing_join_predicates(
    db::plan::optimizer::JoinReorderingPhase::JoinPlan &join_plan, std::unordered_set<expression::Term> &&needed_terms)
{
    if (needed_terms.empty()) [[unlikely]]
    {
        return;
    }

    /// Remove all terms that are actually in a join predicate.
    for (auto [node, _] : join_plan.plan().nodes_and_parent())
    {
        if (typeid(*node) == typeid(logical::JoinNode))
        {
            const auto &predicate = reinterpret_cast<logical::JoinNode *>(node)->predicate();
            if (predicate->id() == expression::Operation::Id::Equals)
            {
                auto *binary_expression = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
                needed_terms.erase(binary_expression->left_child()->result().value());
                needed_terms.erase(binary_expression->right_child()->result().value());
            }
        }
    }

    if (needed_terms.empty() == false)
    {
        /// Find all join predicates that are missing.
        auto *root_node = join_plan.plan().root();
        auto child_iterator = PlanViewNodeChildIterator{join_plan.plan()};

        for (auto *join_predicate : join_plan.pending_join_predicates())
        {
            if (JoinReorderingPhase::contains_missing_term(join_predicate, needed_terms))
            {
                JoinReorderingPhase::complement_missing_join_predicate(child_iterator, root_node,
                                                                       join_predicate->copy());
            }
        }
    }
}

bool JoinReorderingPhase::contains_missing_term(expression::Operation *join_predicate,
                                                std::unordered_set<expression::Term> &needed_terms)
{
    if (join_predicate->id() == expression::Operation::Id::Equals)
    {
        auto *binary_expression = reinterpret_cast<expression::BinaryOperation *>(join_predicate);
        const auto &left_term = binary_expression->left_child()->result().value();
        const auto &right_term = binary_expression->right_child()->result().value();

        if (needed_terms.contains(left_term) || needed_terms.contains(right_term))
        {
            needed_terms.erase(left_term);
            needed_terms.erase(right_term);
            return true;
        }
    }

    return false;
}

void JoinReorderingPhase::complement_missing_join_predicate(
    const db::plan::optimizer::PlanViewNodeChildIterator &child_iterator, logical::NodeInterface *node,
    std::unique_ptr<expression::Operation> &&join_predicate)
{
    if (node->is_unary())
    {
        JoinReorderingPhase::complement_missing_join_predicate(
            child_iterator, child_iterator.child(reinterpret_cast<logical::UnaryNode *>(node)),
            std::move(join_predicate));
    }
    else if (node->is_binary())
    {
        auto *binary_predicate = reinterpret_cast<expression::BinaryOperation *>(join_predicate.get());
        const auto &left_attribute = binary_predicate->left_child()->result().value();
        const auto &right_attribute = binary_predicate->right_child()->result().value();

        const auto [left_child, right_child] = child_iterator.children(reinterpret_cast<logical::BinaryNode *>(node));
        const auto &left_child_schema = left_child->relation().schema();
        const auto &right_child_schema = right_child->relation().schema();

        if (left_child_schema.contains(left_attribute) && left_child_schema.contains(right_attribute))
        {
            JoinReorderingPhase::complement_missing_join_predicate(child_iterator, left_child,
                                                                   std::move(join_predicate));
        }
        else if (right_child_schema.contains(left_attribute) && right_child_schema.contains(right_attribute))
        {
            JoinReorderingPhase::complement_missing_join_predicate(child_iterator, right_child,
                                                                   std::move(join_predicate));
        }
        else if ((left_child_schema.contains(left_attribute) && right_child_schema.contains(right_attribute)) ||
                 (right_child_schema.contains(left_attribute) && left_child_schema.contains(right_attribute)))
        {
            if (typeid(*node) == typeid(logical::JoinNode))
            {
                auto *join_node = reinterpret_cast<logical::JoinNode *>(node);
                join_node->predicate() = expression::OperationBuilder::make_and(std::move(join_node->predicate()),
                                                                                std::move(join_predicate));
            }
        }
    }
}