#pragma once

#include "phase_interface.h"
#include "rule_interface.h"
#include <db/plan/logical/node/node_interface.h>
#include <db/plan/logical/plan.h>
#include <db/topology/database.h>
#include <memory>
#include <unordered_map>
#include <vector>

namespace db::plan::optimizer {
class Optimizer
{
public:
    explicit Optimizer(topology::Database &database) noexcept : _database(database) {}
    virtual ~Optimizer() = default;

    /**
     * Optimizes the given plan.
     *
     * @param logical_plan Plan to optimize.
     *
     * @return The optimized plan.
     */
    [[nodiscard]] logical::Plan optimize(logical::Plan &&logical_plan);

    /**
     * Adds an optimization phase to this optimizer.
     *
     * @param rule Phase that will be passed on the plan during optimization.
     */
    void add(std::unique_ptr<PhaseInterface> &&phase) { _phases.emplace_back(std::move(phase)); }

private:
    /// Database needed to emit new schema.
    topology::Database &_database;

    /// List of optimizer rules, that will be performed to optimize the plan.
    std::vector<std::unique_ptr<PhaseInterface>> _phases;

    /**
     * Commits the given plan view to the given plan.
     * The plan view is an optimized version of the original plan.
     *
     * @param plan_view View containing the optimized plan.
     * @param plan Original plan that will be rebuild.
     *
     * @return The real plan, build like the plan view said.
     */
    [[nodiscard]] static std::unique_ptr<logical::NodeInterface> commit(PlanView &&plan_view,
                                                                        std::unique_ptr<logical::NodeInterface> &&plan);

    /**
     * Commits the given plan view for the root node and recursively for its children.
     *
     * @param plan_view Plan view to commit.
     * @param original_nodes List of nodes in the plan.
     *
     * @return Optimized plan like given by the plan view.
     */
    static std::unique_ptr<logical::NodeInterface> commit(
        const PlanView &plan_view,
        std::unordered_map<std::uintptr_t, std::unique_ptr<logical::NodeInterface>> &original_nodes)
    {
        return Optimizer::commit(plan_view.root(), plan_view, original_nodes);
    }

    /**
     * Commits the given plan view for the given node and recursively for its children.
     *
     * @param node Node to commit.
     * @param plan_view Plan view to commit.
     * @param original_nodes List of nodes in the plan.
     *
     * @return Optimized plan like given by the plan view.
     */
    [[nodiscard]] static std::unique_ptr<logical::NodeInterface> commit(
        PlanView::node_t node, const PlanView &plan_view,
        std::unordered_map<std::uintptr_t, std::unique_ptr<logical::NodeInterface>> &original_nodes);

    /**
     * Takes out all nodes recursively for the given node and its children.
     * Nodes are stored in the given container.
     *
     * @param node Node that should be stolen recursively.
     * @param node_container Container to store the stolen nodes.
     */
    static void steal_nodes(
        std::unique_ptr<logical::NodeInterface> &&node,
        std::unordered_map<std::uintptr_t, std::unique_ptr<logical::NodeInterface>> &node_container);

    /**
     * Takes out all nodes recursively for the given node and its children.
     *
     * @param node Node that should be stolen recursively.
     *
     * @return Map containing all stolen nodes.
     */
    [[nodiscard]] static std::unordered_map<std::uintptr_t, std::unique_ptr<logical::NodeInterface>> steal_nodes(
        std::unique_ptr<logical::NodeInterface> &&node)
    {
        auto stolen_nodes = std::unordered_map<std::uintptr_t, std::unique_ptr<logical::NodeInterface>>{};
        Optimizer::steal_nodes(std::move(node), stolen_nodes);
        return stolen_nodes;
    }
};

class ConfigurableOptimizer final : public Optimizer
{
public:
    explicit ConfigurableOptimizer(topology::Database &database);
    ~ConfigurableOptimizer() override = default;
};
} // namespace db::plan::optimizer