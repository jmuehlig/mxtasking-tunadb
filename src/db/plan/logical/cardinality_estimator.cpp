#include "cardinality_estimator.h"
#include <db/statistic/equi_depth_histogram.h>

using namespace db::plan::logical;

float CardinalityEstimator::estimate_selectivity(const topology::Database &database,
                                                 const std::unique_ptr<expression::Operation> &predicate)
{
    if (predicate->id() == expression::Operation::Id::And)
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        return CardinalityEstimator::estimate_selectivity(database, binary_operation->left_child()) *
               CardinalityEstimator::estimate_selectivity(database, binary_operation->right_child());
    }

    if (predicate->id() == expression::Operation::Id::Or)
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        return CardinalityEstimator::estimate_selectivity(database, binary_operation->left_child()) +
               CardinalityEstimator::estimate_selectivity(database, binary_operation->right_child());
    }

    if (predicate->is_comparison())
    {
        auto *comparison_predicate = reinterpret_cast<expression::BinaryOperation *>(predicate.get());
        if (comparison_predicate->left_child()->is_nullary() &&
            comparison_predicate->left_child()->result()->is_attribute())
        {
            const auto &attribute = comparison_predicate->left_child()->result()->get<expression::Attribute>();
            const auto &left_attribute_histogram = CardinalityEstimator::histogram(database, attribute);
            const auto count = CardinalityEstimator::count_rows(database, attribute);
            if (left_attribute_histogram != nullptr && count.has_value())
            {
                /// Comparison foo::attribute = 123::value
                if (comparison_predicate->right_child()->is_nullary() &&
                    comparison_predicate->right_child()->result()->is_value())
                {
                    const auto &value = comparison_predicate->right_child()->result()->get<data::Value>();

                    if (comparison_predicate->id() == expression::Operation::Id::Equals)
                    {
                        return float(left_attribute_histogram->approximate_equals(value)) / count.value();
                    }

                    if (comparison_predicate->id() == expression::Operation::Id::NotEquals)
                    {
                        return float(left_attribute_histogram->approximate_not_equals(value)) / count.value();
                    }

                    if (comparison_predicate->id() == expression::Operation::Id::Lesser)
                    {
                        return float(left_attribute_histogram->approximate_lesser(value)) / count.value();
                    }

                    if (comparison_predicate->id() == expression::Operation::Id::LesserEquals)
                    {
                        return float(left_attribute_histogram->approximate_lesser_equals(value)) / count.value();
                    }

                    if (comparison_predicate->id() == expression::Operation::Id::Greater)
                    {
                        return float(left_attribute_histogram->approximate_greater(value)) / count.value();
                    }

                    if (comparison_predicate->id() == expression::Operation::Id::GreaterEquals)
                    {
                        return float(left_attribute_histogram->approximate_greater_equals(value)) / count.value();
                    }
                }

                /// Between(attr, (123,456))
                else if (comparison_predicate->id() == expression::Operation::Id::Between &&
                         comparison_predicate->right_child()->id() == expression::Operation::Id::BetweenOperands)
                {
                    auto *between_operands =
                        reinterpret_cast<expression::BinaryOperation *>(comparison_predicate->right_child().get());
                    const auto &left = between_operands->left_child();
                    const auto &right = between_operands->right_child();

                    if (left->is_nullary() && left->result()->is_value() && right->is_nullary() &&
                        right->result()->is_value())
                    {
                        const auto &left_value = left->result()->get<data::Value>();
                        const auto &right_value = right->result()->get<data::Value>();

                        return float(left_attribute_histogram->approximate_between(left_value, right_value)) /
                               count.value();
                    }
                }

                else if (comparison_predicate->id() == expression::Operation::Id::In &&
                         comparison_predicate->right_child()->is_nullary_list())
                {
                    auto *nullary_list =
                        reinterpret_cast<expression::NullaryListOperation *>(comparison_predicate->right_child().get());
                    const auto sum_count_equals =
                        std::accumulate(nullary_list->terms().begin(), nullary_list->terms().end(), 0ULL,
                                        [&left_attribute_histogram](const auto sum, const auto &value_term) {
                                            if (value_term.is_value())
                                            {
                                                return sum + left_attribute_histogram->approximate_equals(
                                                                 value_term.template get<data::Value>());
                                            }

                                            return sum;
                                        });

                    return float(sum_count_equals) / count.value();
                }

                /// Comparison foo::attribute = bar::attribute
                else if (comparison_predicate->right_child()->is_nullary() &&
                         comparison_predicate->right_child()->result()->is_attribute())
                {
                    if (comparison_predicate->id() == expression::Operation::Id::Equals)
                    {
                        const auto &right_attribute_histogram = CardinalityEstimator::histogram(
                            database, comparison_predicate->right_child()->result()->get<expression::Attribute>());
                        if (left_attribute_histogram->type() == statistic::HistogramInterface::Type::EquiDepth &&
                            right_attribute_histogram != nullptr &&
                            right_attribute_histogram->type() == statistic::HistogramInterface::Type::EquiDepth)
                        {
                            auto *left_equi_depth_histogram =
                                reinterpret_cast<statistic::EquiDepthHistogram *>(left_attribute_histogram.get());
                            auto *right_equi_depth_histogram =
                                reinterpret_cast<statistic::EquiDepthHistogram *>(right_attribute_histogram.get());

                            return (float(left_equi_depth_histogram->depth()) / left_equi_depth_histogram->count()) +
                                   (float(right_equi_depth_histogram->depth()) / right_equi_depth_histogram->count());
                        }
                    }
                }
            }

            if (comparison_predicate->id() == expression::Operation::Id::Equals)
            {
                /// Histogram is not available; fall back to max distinct value.
                const auto count_distinct = CardinalityEstimator::estimate_distinct_values(database, attribute);
                if (count_distinct.has_value())
                {
                    return 1.0 / count_distinct.value();
                }
            }

            if (comparison_predicate->id() == expression::Operation::Id::In)
            {
                if (comparison_predicate->right_child()->id() == expression::Operation::Id::IdentityList)
                {
                    /// Histogram is not available; fall back to max distinct value.
                    const auto count_distinct = CardinalityEstimator::estimate_distinct_values(database, attribute);
                    if (count_distinct.has_value())
                    {
                        auto *nullary_list = reinterpret_cast<expression::NullaryListOperation *>(
                            comparison_predicate->right_child().get());
                        return nullary_list->terms().size() * (1.0 / count_distinct.value());
                    }
                }
            }
        }
    }
    else if (predicate->is_nullary() && predicate->result().has_value() && predicate->result()->is_attribute())
    {
        const auto &attribute_histogram =
            CardinalityEstimator::histogram(database, predicate->result()->get<expression::Attribute>());
        if (attribute_histogram != nullptr &&
            attribute_histogram->type() == statistic::HistogramInterface::Type::EquiDepth)
        {
            auto *equi_depth_histogram = reinterpret_cast<statistic::EquiDepthHistogram *>(attribute_histogram.get());
            return float(equi_depth_histogram->depth()) / equi_depth_histogram->count();
        }
    }

    return .5F; /// Default estimate_selectivity.
}