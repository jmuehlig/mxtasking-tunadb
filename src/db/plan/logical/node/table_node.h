#pragma once
#include "node_interface.h"
#include <db/exception/plan_exception.h>
#include <db/plan/logical/cardinality_estimator.h>
#include <db/plan/logical/table.h>

namespace db::plan::logical {
class TableNode final : public NullaryNode
{
public:
    explicit TableNode(TableReference &&table) noexcept
        : NullaryNode(std::string{table.name()}), _table(std::move(table))
    {
    }
    TableNode(const TableNode &) = default;

    ~TableNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database &database) const override
    {
        if (database.is_table(_table.name()) == false)
        {
            throw exception::TableNotFoundException{_table.name()};
        }

        const auto &table = database[_table.name()];

        return table.statistics().count_rows();
    }

    [[nodiscard]] topology::LogicalSchema schema(const topology::Database &database) const override
    {
        if (database.is_table(_table.name()) == false)
        {
            throw exception::TableNotFoundException{_table.name()};
        }

        const auto &table = database[_table.name()];
        auto schema = topology::LogicalSchema{};
        schema.reserve(table.schema().size());
        for (auto i = 0U; i < table.schema().size(); ++i)
        {
            const auto &term = table.schema().term(i);
            schema.emplace_back(
                expression::Term::make_attribute(expression::Attribute::Source{table.name(), _table.alias()},
                                                 term.get<expression::Attribute>().column_name()),
                table.schema().type(i));
        }

        return schema;
    }

    [[nodiscard]] const TableReference &table() const noexcept { return _table; }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = NodeInterface::to_json(database);
        json["data"]["Table"] = _table.name();
        if (_table.alias().has_value())
        {
            json["data"]["Alias"] = _table.alias().value();
        }

        const auto rows = CardinalityEstimator::count_rows(database, _table.name());
        if (rows.has_value())
        {
            json["data"]["Rows"] = std::to_string(rows.value());
        }

        return json;
    }

private:
    TableReference _table;
};
} // namespace db::plan::logical