#pragma once
#include <db/plan/logical/node/join_node.h>
#include <db/plan/optimizer/phase_interface.h>
#include <memory>
#include <utility>
#include <vector>

namespace db::plan::optimizer {
class JoinReorderingPhase final : public PhaseInterface
{
public:
    constexpr JoinReorderingPhase() = default;
    ~JoinReorderingPhase() override = default;

    [[nodiscard]] std::pair<bool, PlanView> apply(PlanView &&plan_view) override;

    [[nodiscard]] bool is_require_cardinality() const noexcept override { return true; }

private:
    /**
     * The JoinPlan is one of many join plans.
     * It holds the current join plan and a set of pipelines
     * and join nodes that are not included at the moment but
     * part of the original plan.
     */
    class JoinPlan
    {
    public:
        JoinPlan(PlanView &&plan, const std::uint64_t cost) : _plan(std::move(plan)), _cost(cost) {}

        JoinPlan(JoinPlan &&) noexcept = default;

        ~JoinPlan() = default;

        JoinPlan &operator=(JoinPlan &&) noexcept = default;

        [[nodiscard]] std::uint64_t cost() const noexcept { return _cost; }
        [[nodiscard]] const PlanView &plan() const noexcept { return _plan; }
        [[nodiscard]] PlanView &plan() noexcept { return _plan; }
        [[nodiscard]] const std::vector<logical::NodeInterface *> &pending_pipelines() const noexcept
        {
            return _pending_pipelines;
        }
        [[nodiscard]] const std::vector<expression::Operation *> &pending_join_predicates() const noexcept
        {
            return _pending_join_predicates;
        }

        /**
         * @return True, if all pipelines are included.
         */
        [[nodiscard]] bool has_included_all_pipelines() const noexcept { return _pending_pipelines.empty(); }

        /**
         * Copies the given join predicates into the pending join predicates, excluding the given predicate.
         *
         * @param join_predicates Predicates to copy from.
         * @param join_predicate Predicate to exclude.
         */
        void copy_predicates_without(const std::vector<std::unique_ptr<expression::Operation>> &join_predicates,
                                     const std::unique_ptr<expression::Operation> &join_predicate)
        {
            for (const auto &predicate : join_predicates)
            {
                if (predicate != join_predicate)
                {
                    _pending_join_predicates.emplace_back(predicate.get());
                }
            }
        }

        /**
         * Copies the given join predicates into the pending join predicates, excluding the given predicate.
         *
         * @param join_predicates Predicates to copy from.
         * @param join_predicate Predicate to exclude.
         */
        void copy_predicates_without(const std::vector<expression::Operation *> &join_predicates,
                                     expression::Operation *join_predicate)
        {
            for (const auto &predicate : join_predicates)
            {
                if (predicate != join_predicate)
                {
                    _pending_join_predicates.emplace_back(predicate);
                }
            }
        }

        /**
         * Copies the given pipelines into the pending pipelines, excluding the given one.
         *
         * @param pipelines Pipelines to copy from.
         * @param pipeline Pipeline to exclude.
         */
        void copy_pipelines_without(const std::vector<logical::NodeInterface *> &pipelines,
                                    logical::NodeInterface *pipeline)
        {
            std::copy_if(pipelines.begin(), pipelines.end(), std::back_inserter(_pending_pipelines),
                         [pipeline](const auto *node) { return node != pipeline; });
        }

        /**
         * Copies the given pipelines into the pending pipelines, excluding the given ones.
         *
         * @param pipelines Pipelines to copy from.
         * @param first_pipeline Pipeline to exclude.
         * @param second_pipeline Pipeline to exclude.
         */
        void copy_pipelines_without(const std::vector<logical::NodeInterface *> &pipelines,
                                    logical::NodeInterface *first_pipeline, logical::NodeInterface *second_pipeline)
        {
            std::copy_if(pipelines.begin(), pipelines.end(), std::back_inserter(_pending_pipelines),
                         [first_pipeline, second_pipeline](const auto *node) {
                             return node != first_pipeline && node != second_pipeline;
                         });
        }

    private:
        /// Real plan.
        PlanView _plan;

        /// Estimated costs of the plan.
        std::uint64_t _cost;

        /// Pipelines that are not included into the plan (but in the original plan).
        std::vector<logical::NodeInterface *> _pending_pipelines;

        /// Joins that are not included into the plan (but in the original plan).
        std::vector<expression::Operation *> _pending_join_predicates;
    };

    /**
     * Extracts all pipelines (without join but including a table) and
     * all join predicates from the original plan.
     *
     * @param child_iterator Child iterator.
     * @param node Node to start.
     * @param source_pipelines List to append pipelines.
     * @param join_predicate_nodes List to append Selection nodes holding a join predicate and JoinNodes.
     */
    static void extract_source_pipelines_and_join_predicates(
        const PlanViewNodeChildIterator &child_iterator, logical::NodeInterface *node,
        std::vector<logical::NodeInterface *> &source_pipelines,
        std::vector<std::pair<logical::NodeInterface *, expression::Operation *>> &join_predicate_nodes);

    /**
     * Tests if the given predicate is a join predicate (a = b).
     *
     * @param predicate Predicate to test.
     * @return True, if the predicate is a join predicate.
     */
    [[nodiscard]] static bool is_join_predicate(const std::unique_ptr<expression::Operation> &predicate);

    /**
     * Extracts a set of all terms from the predicates.
     * In case we have more predicates than joins, we need
     * to add some predicates additionally, if they are not included
     * in the join plan.
     *
     * @param join_predicates List of all join predicates.
     * @return List of all sets that are needed.
     */
    [[nodiscard]] static std::unordered_set<expression::Term> extract_needed_terms(
        const std::vector<std::unique_ptr<expression::Operation>> &join_predicates);

    /**
     * Adds transitive joins (JOIN a = b, JOIN b = c) => JOIN a = c.
     *
     * @param join_nodes All join nodes.
     */
    static void add_transitive_predicates(std::vector<std::unique_ptr<expression::Operation>> &join_predicates);

    /**
     * Builds the initial join plans. Every initial join plan has only
     * one join and two pipelines.
     *
     * @param plan Original plan.
     * @param source_pipelines All raw pipelines.
     * @param join_predicates All raw join predicates.
     * @return List of initial join plans.
     */
    [[nodiscard]] static std::vector<JoinPlan> make_initial_step(
        const PlanView &plan, const std::vector<logical::NodeInterface *> &source_pipelines,
        const std::vector<std::unique_ptr<expression::Operation>> &join_predicates);

    /**
     * Checks if a join is possible between two nodes.
     * A join is possible of all attributes needed for the join predicated
     * are delivered by at least one of the two nodes.
     *
     * @param first First node to join.
     * @param second Second node to join.
     * @param join_predicate Join predicate.
     * @return True, if all attributes are delivered by the node pair.
     */
    [[nodiscard]] static bool is_join_possible(logical::NodeInterface *first, logical::NodeInterface *second,
                                               const std::unique_ptr<expression::Operation> &join_predicate)
    {
        return is_join_possible(first, second, join_predicate.get());
    }

    /**
     * Checks if a join is possible between two nodes.
     * A join is possible of all attributes needed for the join predicated
     * are delivered by at least one of the two nodes.
     *
     * @param first First node to join.
     * @param second Second node to join.
     * @param join_predicate Join predicate.
     * @return True, if all attributes are delivered by the node pair.
     */
    [[nodiscard]] static bool is_join_possible(logical::NodeInterface *first, logical::NodeInterface *second,
                                               expression::Operation *join_predicate);

    /**
     * Tests if the given predicate contains the given term and returns the other term.
     * This is only valid for Equals Operations.
     *
     * @param predicate Equal predicate to test.
     * @param term Term to test.
     * @return The other term of the equal predicate, if the predicate contains the given term.
     */
    [[nodiscard]] static std::optional<expression::Term> contains_term(
        const std::unique_ptr<expression::Operation> &predicate, const expression::Term &term);

    /**
     * Returns the nodes in an order that the first is the one with lower cardinality
     * since hash joins are build over the left side.
     *
     * @param first First node to join.
     * @param second Second node to join.
     * @return Nodes ordered for a hash join.
     */
    [[nodiscard]] static std::pair<logical::NodeInterface *, logical::NodeInterface *> join_child_order(
        logical::NodeInterface *first, logical::NodeInterface *second);

    /**
     * Takes the join plan and produces a set of new join plans
     * including one more pipeline.
     *
     * @param join_plan Join plan to process.
     * @param original_plan_view Original plan.
     * @plans List to append new plans.
     */
    static void make_step(JoinPlan &&join_plan, const PlanView &original_plan_view, std::vector<JoinPlan> &plans);

    /**
     * Checks if all join plans are finished.
     * A join plan is finished if there is no pending pipeline
     * and all pipelines are included into the join plan.
     *
     * @param plans Plans to check.
     * @return True, if all join plans are finished.
     */
    [[nodiscard]] static bool has_included_all_pipelines(const std::vector<JoinPlan> &plans) noexcept
    {
        auto unfinished_iterator = std::find_if(
            plans.begin(), plans.end(), [](const auto &plan) { return plan.has_included_all_pipelines() == false; });

        return unfinished_iterator == plans.end();
    }

    /**
     * Scans the list of needed join terms of all joins in the given plan.
     * Whenever a term is missing, the predicate containing the term will be
     * inserted to an existing join node.
     *
     * @param plan Plan containing all joins.
     * @param needed_terms List of terms that are required.
     * @param join_predicates List of all join predicates.
     */
    static void complement_missing_join_predicates(JoinPlan &join_plan,
                                                   std::unordered_set<expression::Term> &&needed_terms);

    /**
     * Checks if the given predicates has terms that are needed but still missing.
     * If so, both join attributes will be removed from the list.
     *
     * @param join_predicate Predicate to check.
     * @param needed_terms List of missing and needed terms.
     * @return True, if at least one attribute is missing.
     */
    [[nodiscard]] static bool contains_missing_term(expression::Operation *join_predicate,
                                                    std::unordered_set<expression::Term> &needed_terms);

    static void complement_missing_join_predicate(const PlanViewNodeChildIterator &child_iterator,
                                                  logical::NodeInterface *node,
                                                  std::unique_ptr<expression::Operation> &&join_predicate);
};
} // namespace db::plan::optimizer