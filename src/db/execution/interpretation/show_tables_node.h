#pragma once
#include <algorithm>
#include <cstdint>
#include <db/execution/operator_interface.h>
#include <db/execution/record_token.h>
#include <db/execution/scan_generator.h>
#include <db/topology/database.h>
#include <db/type/type.h>
#include <mx/tasking/dataflow/node.h>
#include <optional>
#include <string>
#include <vector>

namespace db::execution::interpretation {
class ShowTablesNode final : public mx::tasking::dataflow::ProducingNodeInterface<RecordSet>, public OperatorInterface
{
public:
    ShowTablesNode(const topology::Database &database) noexcept : _database(database)
    {
        _schema.emplace_back(expression::Term::make_attribute("Table"), type::Type::make_char(64U));
        _schema.emplace_back(expression::Term::make_attribute("#Tiles"), type::Type::make_bigint());
        _schema.emplace_back(expression::Term::make_attribute("#Records"), type::Type::make_bigint());

        NodeInterface<RecordSet>::annotation().produces(std::make_unique<DisponsableGenerator>());
    }

    ~ShowTablesNode() noexcept override = default;

    void consume(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                 RecordToken && /*data*/) override
    {
        /// Create a temporary tile in the record set for number of tables "records".
        // TODO: Bug when number of tables > config::tuples_per_tile().
        auto records = RecordSet::make_record_set(_schema, worker_id);

        /// Allocate a record for each table and set the name.
        for (const auto &[name, table] : _database.tables())
        {
            auto record_view = records.tile().get<data::PaxTile>()->allocate();
            record_view->set(0U, name);
            record_view->set(1U, type::underlying<type::BIGINT>::value(table.tiles().size()));

            auto count_records = table.statistics().count_rows();
            record_view->set(2U, type::underlying<type::BIGINT>::value(count_records));
        }

        graph.emit(worker_id, this, RecordToken{std::move(records)});
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

    [[nodiscard]] std::string to_string() const noexcept override { return "Show Tables"; }

private:
    /// Database to read the tables from.
    const topology::Database &_database;

    /// Schema for this operator, just a char for the table name.
    topology::PhysicalSchema _schema;
};
} // namespace db::execution::interpretation