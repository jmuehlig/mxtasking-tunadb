#pragma once
#include <cstdint>
#include <db/execution/operator_interface.h>
#include <db/execution/record_token.h>
#include <db/execution/scan_generator.h>
#include <db/topology/database.h>
#include <db/topology/table.h>
#include <mx/tasking/dataflow/node.h>
#include <optional>
#include <string>
#include <vector>

namespace db::execution::interpretation {
class CreateTableNode final : public mx::tasking::dataflow::ProducingNodeInterface<RecordSet>, public OperatorInterface
{
public:
    CreateTableNode(topology::Database &database, std::string &&table_name, topology::PhysicalSchema &&schema) noexcept
        : _database(database), _table_name(std::move(table_name)), _table_schema(std::move(schema))
    {
        NodeInterface<RecordSet>::annotation().produces(std::make_unique<DisponsableGenerator>());
    }

    ~CreateTableNode() noexcept override = default;

    void consume(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                 RecordToken && /*token*/) override
    {
        if (this->_database.is_table(this->_table_name) == false)
        {
            this->_database.insert(std::move(this->_table_name), std::move(this->_table_schema));
        }
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _empty_schema; }

    [[nodiscard]] std::string to_string() const noexcept override { return "Create Table"; }

private:
    /// Schema of this operator which is empty, since this operator yields not records.
    const topology::PhysicalSchema _empty_schema;

    /// Database to create the table in.
    topology::Database &_database;

    /// Name of the table.
    std::string _table_name;

    /// Schema of the table.
    topology::PhysicalSchema _table_schema;
};
} // namespace db::execution::interpretation