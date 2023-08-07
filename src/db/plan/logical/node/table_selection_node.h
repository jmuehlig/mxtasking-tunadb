#pragma once
#include "node_interface.h"
#include <db/exception/plan_exception.h>
#include <db/expression/operation.h>
#include <db/plan/logical/cardinality_estimator.h>
#include <db/plan/logical/table.h>
#include <fmt/core.h>

namespace db::plan::logical {
class TableSelectionNode final : public NullaryNode
{
public:
    TableSelectionNode(TableReference &&table, std::unique_ptr<expression::Operation> &&predicate) noexcept
        : NullaryNode(fmt::format("Selected {}", table.name())), _table_reference(std::move(table)),
          _predicate(std::move(predicate))
    {
    }

    ~TableSelectionNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database &database) const override
    {
        if (database.is_table(_table_reference.name()) == false)
        {
            throw exception::TableNotFoundException{_table_reference.name()};
        }

        const auto &table = database[_table_reference.name()];

        return std::max(1UL, CardinalityEstimator::estimate(table.statistics().count_rows(), database, _predicate));
    }

    [[nodiscard]] topology::LogicalSchema schema(const topology::Database &database) const override
    {
        if (database.is_table(_table_reference.name()) == false)
        {
            throw exception::TableNotFoundException{_table_reference.name()};
        }

        const auto &table = database[_table_reference.name()];
        auto schema = topology::LogicalSchema{};
        schema.reserve(table.schema().size());
        for (auto i = 0U; i < table.schema().size(); ++i)
        {
            const auto &term = table.schema().term(i);
            schema.emplace_back(
                expression::Term::make_attribute(expression::Attribute::Source{table.name(), _table_reference.alias()},
                                                 term.get<expression::Attribute>().column_name()),
                table.schema().type(i));
        }

        /// Check all predicates available.
        expression::for_each_term(_predicate, [&schema](const expression::Term &term) {
            if (term.is_attribute())
            {
                if (schema.contains(term) == false)
                {
                    throw exception::AttributeNotFoundException{term.get<expression::Attribute>().column_name()};
                }
            }
        });

        return schema;
    }

    [[nodiscard]] const TableReference &table() const noexcept { return _table_reference; }
    [[nodiscard]] std::unique_ptr<expression::Operation> &predicate() { return _predicate; }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = NodeInterface::to_json(database);
        auto table_name = _table_reference.name();
        if (_table_reference.alias().has_value())
        {
            table_name = fmt::format("{} as {}", _table_reference.name(), _table_reference.alias().value());
        }

        json["data"]["Table"] = _table_reference.name();
        if (_table_reference.alias().has_value())
        {
            json["data"]["Alias"] = _table_reference.alias().value();
        }

        const auto rows = CardinalityEstimator::count_rows(database, _table_reference.name());
        if (rows.has_value())
        {
            json["data"]["Rows"] = std::to_string(rows.value());
        }

        json["data"]["Predicate"] = _predicate->to_string();
        const auto selectivity = CardinalityEstimator::estimate_selectivity(database, _predicate);
        json["data"]["Selectivity"] = fmt::format("{:.3f} %", selectivity * 100.0F);

        return json;
    }

private:
    TableReference _table_reference;
    std::unique_ptr<expression::Operation> _predicate;
};
} // namespace db::plan::logical