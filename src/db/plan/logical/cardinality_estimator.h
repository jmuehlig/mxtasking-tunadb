#pragma once
#include <db/expression/operation.h>
#include <db/statistic/histogram.h>
#include <db/topology/database.h>
#include <memory>
#include <optional>

namespace db::plan::logical {
class CardinalityEstimator
{
public:
    /**
     * Estimates the cardinality for the given predicate
     * based on the given incoming number of rows.
     *
     * @param incoming_cardinality Base cardinality.
     * @param database Database to get histograms.
     * @param predicate Predicate to estimate estimate_selectivity for.
     * @return Estimated cardinality.
     */
    [[nodiscard]] static std::uint64_t estimate(const std::uint64_t incoming_cardinality,
                                                const topology::Database &database,
                                                const std::unique_ptr<expression::Operation> &predicate)
    {
        const auto selectivity = CardinalityEstimator::estimate_selectivity(database, predicate);
        return incoming_cardinality * selectivity;
    }

    [[nodiscard]] static std::optional<std::uint64_t> count_rows(const topology::Database &database,
                                                                 const expression::Attribute &attribute)
    {
        if (attribute.source().has_value())
        {
            if (database.is_table(attribute.source()->name()))
            {
                const auto &table = database[attribute.source()->name()];
                return std::make_optional(table.statistics().count_rows());
            }
        }

        return std::nullopt;
    }

    /**
     * Returns the number of rows for a specified table.
     *
     * @param database Database.
     * @param table_name Name of the specified table.
     * @return Number of table rows.
     */
    [[nodiscard]] static std::optional<std::uint64_t> count_rows(const topology::Database &database,
                                                                 const std::string &table_name)
    {
        if (database.is_table(table_name))
        {
            const auto &table = database[table_name];
            return std::make_optional(table.statistics().count_rows());
        }

        return std::nullopt;
    }

    /**
     * Estimates the number of distinct values of a specific (table) attribute.
     *
     * @param database Database.
     * @param attribute Table attribute (table and column).
     * @return Number of estimated distinct values.
     */
    [[nodiscard]] static std::optional<std::uint64_t> estimate_distinct_values(const topology::Database &database,
                                                                               const expression::Attribute &attribute)
    {
        if (attribute.source().has_value())
        {
            if (database.is_table(attribute.source()->name()))
            {
                const auto &table = database[attribute.source()->name()];
                const auto index = table.schema().index(attribute.column_name());
                if (index.has_value())
                {
                    return std::make_optional(table.statistics().count_distinct(index.value()));
                }
            }
        }

        return std::nullopt;
    }

    /**
     * Estimates the estimate_selectivity for the given predicate.
     *
     * @param database Database to get histograms.
     * @param predicate Predicate to estimate estimate_selectivity for.
     * @return Selectivity (0 <= sel <= 1) for the given predicate.
     */
    [[nodiscard]] static float estimate_selectivity(const topology::Database &database,
                                                    const std::unique_ptr<expression::Operation> &predicate);

    [[nodiscard]] static const std::unique_ptr<statistic::HistogramInterface> &histogram(
        const topology::Database &database, const expression::Attribute &attribute)
    {
        if (attribute.source().has_value())
        {
            if (database.is_table(attribute.source()->name()))
            {
                const auto &table = database[attribute.source()->name()];
                const auto index = table.schema().index(attribute.column_name());
                if (index.has_value())
                {
                    return table.statistics().histogram(index.value());
                }
            }
        }

        return EMPTY_HISTOGRAM;
    }

private:
    inline static std::unique_ptr<statistic::HistogramInterface> EMPTY_HISTOGRAM{nullptr};
};
} // namespace db::plan::logical