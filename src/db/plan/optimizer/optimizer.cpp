#include "optimizer.h"
#include "phases/join_reordering_phase.h"
#include "phases/rule_phases.h"
#include <db/plan/logical/adjuster.h>
#include <db/plan/logical/adjustments/join_predicate_left_right_adjustment.h>

using namespace db::plan::optimizer;

db::plan::logical::Plan Optimizer::optimize(logical::Plan &&logical_plan)
{
    auto original_root_node = std::move(logical_plan.root_node());
    auto plan_view = PlanView{this->_database, original_root_node};

    auto is_optimized = false;
    auto is_last_optimized = false;

    for (auto &phase : this->_phases)
    {
        if (is_last_optimized)
        {
            auto child_iterator = PlanViewNodeChildIterator{plan_view};
            std::ignore =
                plan_view.root()->emit_relation(this->_database, child_iterator, phase->is_require_cardinality());
        }

        std::tie(is_last_optimized, plan_view) = phase->apply(std::move(plan_view));
        is_optimized |= is_last_optimized;
    }

    if (is_optimized)
    {
        auto optimized_plan_root = Optimizer::commit(std::move(plan_view), std::move(original_root_node));

        /// Rebuild schema for optimized plan.
        auto child_iterator = logical::TreeNodeChildIterator{};
        std::ignore = optimized_plan_root->emit_relation(this->_database, child_iterator, true);

        /// Do some adjustments for the optimized plan, that may be lost by optimizations (join replacing i.e.).
        auto adjuster = logical::Adjuster{};
        adjuster.add(std::make_unique<logical::JoinPredicateLeftRightAdjustment>());
        adjuster.adjust(optimized_plan_root);

        /// Finally the plan is ready for execution.
        return logical::Plan{std::move(optimized_plan_root)};
    }

    return logical::Plan{std::move(original_root_node)};
}

std::unique_ptr<db::plan::logical::NodeInterface> Optimizer::commit(PlanView &&plan_view,
                                                                    std::unique_ptr<logical::NodeInterface> &&plan)
{
    /// Steal all nodes from plan. Child-Nodes are null from now.
    auto stolen_nodes = Optimizer::steal_nodes(std::move(plan));

    /// Create all nodes, that are produced by optimizations and where not in the plan before.
    for (const auto &[node, _] : plan_view.nodes_and_parent())
    {
        if (stolen_nodes.find(std::uintptr_t(node)) == stolen_nodes.end())
        {
            stolen_nodes.insert(std::make_pair(std::uintptr_t(node), std::unique_ptr<logical::NodeInterface>{node}));
        }
    }

    /// Start at the root of the optimized plan.
    return Optimizer::commit(plan_view, stolen_nodes);
}

std::unique_ptr<db::plan::logical::NodeInterface> Optimizer::commit(
    PlanView::node_t node, const PlanView &plan_view,
    std::unordered_map<std::uintptr_t, std::unique_ptr<logical::NodeInterface>> &original_nodes)
{
    auto real_node = std::move(original_nodes.at(std::uintptr_t(node)));

    if (real_node->is_unary())
    {
        const auto &children = plan_view.children(node);
        auto child = Optimizer::commit(std::get<0>(children), plan_view, original_nodes);
        reinterpret_cast<logical::UnaryNode *>(real_node.get())->child(std::move(child));
    }
    else if (real_node->is_binary())
    {
        const auto &children = plan_view.children(node);
        auto left_child = Optimizer::commit(std::get<0>(children), plan_view, original_nodes);
        auto right_child = Optimizer::commit(std::get<1>(children), plan_view, original_nodes);
        reinterpret_cast<logical::BinaryNode *>(real_node.get())->left_child(std::move(left_child));
        reinterpret_cast<logical::BinaryNode *>(real_node.get())->right_child(std::move(right_child));
    }

    return real_node;
}

void Optimizer::steal_nodes(std::unique_ptr<logical::NodeInterface> &&node,
                            std::unordered_map<std::uintptr_t, std::unique_ptr<logical::NodeInterface>> &node_container)
{
    if (node->is_unary())
    {
        Optimizer::steal_nodes(std::move(reinterpret_cast<logical::UnaryNode *>(node.get())->child()), node_container);
    }
    else if (node->is_binary())
    {
        Optimizer::steal_nodes(std::move(reinterpret_cast<logical::BinaryNode *>(node.get())->left_child()),
                               node_container);
        Optimizer::steal_nodes(std::move(reinterpret_cast<logical::BinaryNode *>(node.get())->right_child()),
                               node_container);
    }

    node_container.insert(std::make_pair(std::uintptr_t(node.get()), std::move(node)));
}

ConfigurableOptimizer::ConfigurableOptimizer(topology::Database &database) : Optimizer(database)
{
    this->add(std::make_unique<ExpressionSimplificationPhase>());
    this->add(std::make_unique<PredicatePushdownPhase>());
    this->add(std::make_unique<JoinReorderingPhase>());
    this->add(std::make_unique<EarlySelectionPhase>());
    this->add(std::make_unique<EarlyProjectionPhase>());
    this->add(std::make_unique<PhysicalOperatorMappingPhase>());
}