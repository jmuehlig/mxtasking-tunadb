#include "pre_selection_rule.h"
#include <db/expression/operation_builder.h>
#include <db/plan/logical/node/selection_node.h>
#include <unordered_set>

using namespace db::plan::optimizer;

bool PreSelectionRule::apply(PlanView &plan)
{
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(logical::SelectionNode))
        {
            auto *selection_node = reinterpret_cast<logical::SelectionNode *>(node);
            if (this->_optimized_nodes.contains(node) == false &&
                PreSelectionRule::has_multiple_sources(selection_node->predicate()))
            {
                auto attribute_predicates =
                    PreSelectionRule::extract_predicates_per_attribute(selection_node->predicate());
                auto optimized_node = false;
                for (auto &[attribute, predicates] : attribute_predicates)
                {
                    if (predicates.empty())
                    {
                        continue;
                    }

                    const auto index = selection_node->relation().schema().index(attribute);
                    if (index.has_value())
                    {
                        const auto type = selection_node->relation().schema().type(index.value());
                        if (type == type::Id::CHAR)
                        {
                            /// Collect all values that will be in the "IN" list.
                            auto in_terms = std::vector<expression::Term>{};
                            in_terms.reserve(predicates.size());
                            for (auto *predicate : predicates)
                            {
                                if (predicate->right_child()->is_nullary() &&
                                    predicate->right_child()->result()->is_value())
                                {
                                    in_terms.emplace_back(predicate->right_child()->result().value());
                                }
                            }

                            /// Build the IN list.
                            if (in_terms.empty() == false)
                            {
                                auto nullary_list =
                                    std::make_unique<expression::NullaryListOperation>(std::move(in_terms));
                                auto attribute_expression =
                                    std::make_unique<expression::NullaryOperation>(expression::Term{attribute});
                                auto in_predicate = expression::OperationBuilder::make_in(
                                    std::move(attribute_expression), std::move(nullary_list));

                                plan.insert_between(node, std::get<0>(plan.children(node)),
                                                    plan.make_node<logical::SelectionNode>(std::move(in_predicate)));
                                optimized_node = true;
                            }
                        }
                        else if (type == type::Id::INT || type == type::Id::BIGINT || type == type::Id::DECIMAL)
                        {
                            /// Multiple <,<=,>,>= comparisons are substitute to BETWEEN.
                            if (PreSelectionRule::is_range(predicates))
                            {
                                auto min_value = predicates.front()->right_child()->result()->get<data::Value>();
                                auto max_value = min_value;

                                for (auto i = 1U; i < predicates.size(); ++i)
                                {
                                    auto *predicate = predicates[i];
                                    auto value = predicate->right_child()->result()->get<data::Value>();

                                    if (value <= min_value)
                                    {
                                        if (predicate->id() == expression::Operation::Id::Greater)
                                        {
                                            value = PreSelectionRule::adjust_to_greater_equals(value);
                                        }
                                        min_value = value;
                                    }

                                    if (value >= max_value)
                                    {
                                        if (predicate->id() == expression::Operation::Id::Lesser)
                                        {
                                            value = PreSelectionRule::adjust_to_lesser_equals(value);
                                        }

                                        max_value = value;
                                    }
                                }

                                /// Build range
                                auto range_predicate = expression::OperationBuilder::make_between(
                                    std::make_unique<expression::NullaryOperation>(expression::Term{attribute}),
                                    std::move(min_value), std::move(max_value));
                                plan.insert_between(node, std::get<0>(plan.children(node)),
                                                    plan.make_node<logical::SelectionNode>(std::move(range_predicate)));
                                optimized_node = true;
                            }

                            else
                            {
                                /// Check if all comparisons are BETWEEN.
                                const auto is_all_between =
                                    std::all_of(predicates.begin(), predicates.end(), [](auto *pred) {
                                        return PreSelectionRule::is_qualified_between(pred);
                                    });
                                if (is_all_between)
                                {
                                    auto min_value = reinterpret_cast<expression::BinaryOperation *>(
                                                         predicates.front()->right_child().get())
                                                         ->left_child()
                                                         ->result()
                                                         ->get<data::Value>();
                                    auto max_value = reinterpret_cast<expression::BinaryOperation *>(
                                                         predicates.front()->right_child().get())
                                                         ->right_child()
                                                         ->result()
                                                         ->get<data::Value>();

                                    for (auto i = 1U; i < predicates.size(); ++i)
                                    {
                                        auto *predicate = predicates[i];
                                        auto *between_operands_expression =
                                            reinterpret_cast<expression::BinaryOperation *>(
                                                predicate->right_child().get());

                                        auto left_value =
                                            between_operands_expression->left_child()->result()->get<data::Value>();
                                        auto right_value =
                                            between_operands_expression->right_child()->result()->get<data::Value>();

                                        if (left_value < min_value)
                                        {
                                            min_value = left_value;
                                        }

                                        if (right_value > max_value)
                                        {
                                            max_value = right_value;
                                        }
                                    }

                                    /// Build range
                                    auto range_predicate = expression::OperationBuilder::make_between(
                                        std::make_unique<expression::NullaryOperation>(expression::Term{attribute}),
                                        std::move(min_value), std::move(max_value));
                                    plan.insert_between(
                                        node, std::get<0>(plan.children(node)),
                                        plan.make_node<logical::SelectionNode>(std::move(range_predicate)));
                                    optimized_node = true;
                                }
                            }
                        }
                    }
                }

                this->_optimized_nodes.insert(node);
                if (optimized_node)
                {
                    return true;
                }
            }
        }
    }

    return false;
}

bool PreSelectionRule::has_multiple_sources(const std::unique_ptr<expression::Operation> &predicate)
{
    auto sources = std::unordered_set<expression::Attribute::Source>{};
    expression::for_each_attribute(predicate, [&sources](const expression::Attribute &attribute) {
        if (attribute.source().has_value())
        {
            sources.insert(attribute.source().value());
        }
    });

    return sources.size() > 1U;
}

std::unordered_map<db::expression::Term, std::vector<db::expression::BinaryOperation *>> PreSelectionRule::
    extract_predicates_per_attribute(const std::unique_ptr<expression::Operation> &predicate)
{
    auto attribute_predicates = std::unordered_map<db::expression::Term, std::vector<expression::BinaryOperation *>>{};
    attribute_predicates.reserve(32U);

    expression::for_each_comparison(
        predicate, [&attribute_predicates](const std::unique_ptr<expression::BinaryOperation> &comparison_operation) {
            if (comparison_operation->left_child()->is_nullary() &&
                comparison_operation->left_child()->result()->is_attribute())
            {
                const auto is_right_value = comparison_operation->right_child()->is_nullary() &&
                                            comparison_operation->right_child()->result()->is_value();
                if (is_right_value || PreSelectionRule::is_qualified_between(comparison_operation.get()))
                {
                    const auto &attribute = comparison_operation->left_child()->result().value();
                    auto iterator = attribute_predicates.find(attribute);
                    if (iterator == attribute_predicates.end())
                    {
                        iterator = attribute_predicates
                                       .insert(std::make_pair(attribute, std::vector<expression::BinaryOperation *>{}))
                                       .first;
                    }

                    iterator->second.emplace_back(comparison_operation.get());
                }
            }
        });

    return attribute_predicates;
}

bool PreSelectionRule::is_range(const std::vector<expression::BinaryOperation *> &predicates)
{
    const auto has_lt_or_leq = std::find_if(predicates.begin(), predicates.end(), [](const auto &pred) {
                                   return pred->id() == expression::Operation::Id::Lesser ||
                                          pred->id() == expression::Operation::Id::LesserEquals;
                               }) != predicates.end();

    const auto has_gt_or_geq = std::find_if(predicates.begin(), predicates.end(), [](const auto &pred) {
                                   return pred->id() == expression::Operation::Id::Greater ||
                                          pred->id() == expression::Operation::Id::GreaterEquals;
                               }) != predicates.end();

    const auto has_eq_or_neq = std::find_if(predicates.begin(), predicates.end(), [](const auto &pred) {
                                   return pred->id() == expression::Operation::Id::Equals ||
                                          pred->id() == expression::Operation::Id::NotEquals;
                               }) != predicates.end();

    return has_lt_or_leq && has_gt_or_geq && has_eq_or_neq == false;
}

bool PreSelectionRule::is_qualified_between(expression::BinaryOperation *predicate)
{
    if (predicate->id() == expression::Operation::Id::Between)
    {
        if (predicate->right_child()->id() == expression::Operation::Id::BetweenOperands)
        {
            auto *between_operands = reinterpret_cast<expression::BinaryOperation *>(predicate->right_child().get());
            return between_operands->left_child()->is_nullary() &&
                   between_operands->left_child()->result()->is_value() &&
                   between_operands->right_child()->is_nullary() &&
                   between_operands->right_child()->result()->is_value();
        }
    }

    return false;
}

db::data::Value PreSelectionRule::adjust_to_lesser_equals(data::Value value)
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

db::data::Value PreSelectionRule::adjust_to_greater_equals(data::Value value)
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
