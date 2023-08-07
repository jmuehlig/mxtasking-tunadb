#include "merge_predicates_rule.h"
#include <db/expression/operation_builder.h>
#include <db/plan/logical/cardinality_estimator.h>
#include <db/plan/logical/node/selection_node.h>

using namespace db::plan::optimizer;

bool MergePredicatesRule::apply(PlanView &plan)
{
    auto main_selection_nodes = std::vector<logical::NodeInterface *>{};
    main_selection_nodes.reserve(plan.nodes_and_parent().size());

    for (auto [node, parent] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(logical::SelectionNode) && typeid(*parent) != typeid(logical::SelectionNode))
        {
            main_selection_nodes.push_back(node);
        }
    }

    for (auto *node : main_selection_nodes)
    {
        /// Collect all selections in a row.
        auto selections = std::vector<logical::SelectionNode *>{};

        auto *current_node = node;
        do
        {
            auto *selection_node = reinterpret_cast<logical::SelectionNode *>(current_node);
            if (MergePredicatesRule::is_join_predicate(selection_node->predicate()) == false)
            {
                selections.emplace_back(selection_node);
            }
            current_node = std::get<0>(plan.children(current_node));
        } while (current_node != nullptr && typeid(*current_node) == typeid(logical::SelectionNode));

        /// Merge them together, if multiple.
        if (selections.size() > 1U)
        {
            auto predicates = std::vector<std::pair<float, std::unique_ptr<expression::Operation>>>{};
            predicates.reserve(selections.size());

            /// Copy predicates and estimate selectivity.
            std::transform(selections.begin(), selections.end(), std::back_inserter(predicates),
                           [&database = plan.database()](auto *selection) {
                               const auto selectivity = logical::CardinalityEstimator::estimate_selectivity(
                                   database, selection->predicate());
                               return std::make_pair(selectivity, selection->predicate()->copy());
                           });

            /// Sort by selectivity.
            std::sort(predicates.begin(), predicates.end(),
                      [](const auto &left, const auto &right) { return std::get<0>(left) > std::get<0>(right); });

            /// Remove other selections.
            for (auto i = 1U; i < selections.size(); ++i)
            {
                plan.erase(selections[i]);
            }

            /// Replace the first selection.
            selections.front()->predicate() = MergePredicatesRule::merge(std::move(predicates));
        }
    }

    return false;
}

std::unique_ptr<db::expression::Operation> MergePredicatesRule::merge(
    std::vector<std::pair<float, std::unique_ptr<expression::Operation>>> &&predicates)
{
    if (predicates.size() > 2U)
    {
        /// The vector is ordered "reverse" to use "pop_back"
        /// instead of removing the first element (which is costly on a vector).
        auto left = std::move(predicates.back());
        predicates.pop_back();

        auto right = MergePredicatesRule::merge(std::move(predicates));
        return expression::OperationBuilder::make_and(std::move(std::get<1>(left)), std::move(right));
    }

    /// Notice the reverse order (1 left; 0 right).
    return expression::OperationBuilder::make_and(std::move(std::get<1>(predicates[1U])),
                                                  std::move(std::get<1>(predicates[0U])));
}

bool MergePredicatesRule::is_join_predicate(const std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->id() == expression::Operation::Id::Equals)
    {
        auto *binary_expression = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        return binary_expression->left_child()->result()->is_attribute() &&
               binary_expression->right_child()->result()->is_attribute();
    }

    return false;
}