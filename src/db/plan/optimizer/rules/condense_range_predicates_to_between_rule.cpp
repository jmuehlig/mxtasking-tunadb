#include "condense_range_predicates_to_between_rule.h"
#include <db/expression/operation_builder.h>
#include <db/plan/logical/node/selection_node.h>
#include <unordered_map>

using namespace db::plan::optimizer;

bool CondenseRangePredicatesToBetweenRule::apply(db::plan::optimizer::PlanView &plan)
{
    auto range_predicates = std::unordered_map<expression::Attribute, logical::SelectionNode *>{};
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(logical::SelectionNode))
        {
            auto *selection_node = reinterpret_cast<logical::SelectionNode *>(node);
            auto &predicate = selection_node->predicate();

            /// Check if the predicate is <, <=, >, or >=; these qualifies for BETWEEN predicates.
            if (CondenseRangePredicatesToBetweenRule::is_lesser_or_lesser_equal(predicate->id()) ||
                CondenseRangePredicatesToBetweenRule::is_greater_or_greater_equal(predicate->id()))
            {
                auto *comparison_predicate = reinterpret_cast<expression::BinaryOperation *>(predicate.get());

                /// Check if the left part is an attribute...
                if (comparison_predicate->left_child()->is_nullary() &&
                    comparison_predicate->left_child()->result()->is_attribute())
                {
                    /// ... and the right is a value.
                    if (comparison_predicate->right_child()->is_nullary() &&
                        comparison_predicate->right_child()->result()->is_value())
                    {
                        /// Use BETWEEN only for INT, BIGINT, DECIMAL, and DATE.
                        auto value = comparison_predicate->right_child()->result()->get<data::Value>();
                        if (value.type() == type::Id::INT || value.type() == type::Id::BIGINT ||
                            value.type() == type::Id::DECIMAL || value.type() == type::Id::DATE)
                        {
                            const auto &attribute =
                                comparison_predicate->left_child()->result()->get<expression::Attribute>();

                            auto iterator = range_predicates.find(attribute);

                            /// Check if this is the first predicate for the attribute.
                            if (iterator == range_predicates.end())
                            {
                                range_predicates.insert(std::make_pair(attribute, selection_node));
                            }
                            else
                            {
                                /// If we already have a predicate, build the BETWEEN predicate.
                                const auto &matching_predicate = iterator->second->predicate();
                                auto *matching_predicate_comparison =
                                    reinterpret_cast<expression::BinaryOperation *>(matching_predicate.get());
                                auto matching_predicate_value = reinterpret_cast<expression::NullaryOperation *>(
                                                                    matching_predicate_comparison->right_child().get())
                                                                    ->result()
                                                                    ->get<data::Value>();

                                if (value.type() == matching_predicate_value.type())
                                {
                                    const auto is_leq_and_geq =
                                        CondenseRangePredicatesToBetweenRule::is_lesser_or_lesser_equal(
                                            matching_predicate->id()) &&
                                        CondenseRangePredicatesToBetweenRule::is_greater_or_greater_equal(
                                            predicate->id());
                                    const auto is_geq_and_leq =
                                        CondenseRangePredicatesToBetweenRule::is_greater_or_greater_equal(
                                            matching_predicate->id()) &&
                                        CondenseRangePredicatesToBetweenRule::is_lesser_or_lesser_equal(
                                            predicate->id());

                                    /// Exclude, that both are >/>= or </<=.
                                    if (is_leq_and_geq || is_geq_and_leq)
                                    {
                                        /// Matching Predicate is the greater one.
                                        if (is_leq_and_geq)
                                        {
                                            if (matching_predicate_value >= value)
                                            {
                                                /// Since between includes the value, we need to adjust to LesserEquals
                                                /// (-1).
                                                if (matching_predicate->id() == expression::Operation::Id::Lesser)
                                                {
                                                    matching_predicate_value =
                                                        CondenseRangePredicatesToBetweenRule::adjust_to_lesser_equals(
                                                            matching_predicate_value);
                                                }

                                                /// Since between includes the value, we need to adjust to GreaterEquals
                                                /// (+1).
                                                if (predicate->id() == expression::Operation::Id::Greater)
                                                {
                                                    value =
                                                        CondenseRangePredicatesToBetweenRule::adjust_to_greater_equals(
                                                            value);
                                                }

                                                /// Create the new between with (value, matching_value).
                                                auto between_predicate = expression::OperationBuilder::make_between(
                                                    comparison_predicate->left_child()->copy(), std::move(value),
                                                    std::move(matching_predicate_value));

                                                /// Erase the matching node.
                                                plan.erase(iterator->second);

                                                /// Replace predicate of this node.
                                                predicate = std::move(between_predicate);

                                                return true;
                                            }
                                        }

                                        /// Predicate is the greater one.
                                        else
                                        {
                                            if (value >= matching_predicate_value)
                                            {
                                                /// Since between includes the value, we need to adjust to LesserEquals
                                                /// (-1).
                                                if (predicate->id() == expression::Operation::Id::Lesser)
                                                {
                                                    value =
                                                        CondenseRangePredicatesToBetweenRule::adjust_to_lesser_equals(
                                                            value);
                                                }

                                                /// Since between includes the value, we need to adjust to GreaterEquals
                                                /// (+1).
                                                if (matching_predicate->id() == expression::Operation::Id::Greater)
                                                {
                                                    matching_predicate_value =
                                                        CondenseRangePredicatesToBetweenRule::adjust_to_greater_equals(
                                                            matching_predicate_value);
                                                }

                                                /// Create the new between with (matching_value, value).
                                                auto between_predicate = expression::OperationBuilder::make_between(
                                                    comparison_predicate->left_child()->copy(),
                                                    std::move(matching_predicate_value), std::move(value));

                                                /// Erase the matching node.
                                                plan.erase(iterator->second);

                                                /// Replace predicate of this node.
                                                predicate = std::move(between_predicate);

                                                return true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    return false;
}

db::data::Value CondenseRangePredicatesToBetweenRule::adjust_to_lesser_equals(data::Value value)
{
    if (value.type() == type::Id::INT)
    {
        return data::Value{value.type(), value.get<type::Id::INT>() - 1};
    }

    if (value.type() == type::Id::BIGINT)
    {
        return data::Value{value.type(), value.get<type::Id::BIGINT>() - 1};
    }

    if (value.type() == type::Id::DECIMAL)
    {
        return data::Value{value.type(), value.get<type::Id::DECIMAL>() - 1};
    }

    if (value.type() == type::Id::DATE)
    {
        return data::Value{value.type(), value.get<type::Id::DATE>() - type::Date::make_interval_from_days(1U)};
    }

    return value;
}

db::data::Value CondenseRangePredicatesToBetweenRule::adjust_to_greater_equals(data::Value value)
{
    if (value.type() == type::Id::INT)
    {
        return data::Value{value.type(), value.get<type::Id::INT>() + 1};
    }

    if (value.type() == type::Id::BIGINT)
    {
        return data::Value{value.type(), value.get<type::Id::BIGINT>() + 1};
    }

    if (value.type() == type::Id::DECIMAL)
    {
        return data::Value{value.type(), value.get<type::Id::DECIMAL>() + 1};
    }

    if (value.type() == type::Id::DATE)
    {
        return data::Value{value.type(), value.get<type::Id::DATE>() + type::Date::make_interval_from_days(1U)};
    }

    return value;
}
