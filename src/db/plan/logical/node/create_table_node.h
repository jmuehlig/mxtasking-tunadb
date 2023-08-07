#pragma once

#include "node_interface.h"
#include <db/exception/plan_exception.h>
#include <db/topology/database.h>
#include <db/topology/physical_schema.h>

namespace db::plan::logical {
class CreateTableNode final : public NotSchematizedNode
{
public:
    CreateTableNode(std::string &&table_name, topology::PhysicalSchema &&physical_schema, const bool if_not_exists)
        : NotSchematizedNode("Create Table"), _table_name(std::move(table_name)),
          _physical_schema(std::move(physical_schema)), _if_not_exists(if_not_exists)
    {
    }
    ~CreateTableNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::CREATE; }

    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }

    [[nodiscard]] topology::PhysicalSchema &physical_schema() noexcept { return _physical_schema; }

    [[nodiscard]] topology::LogicalSchema schema(const topology::Database &database) const override
    {
        if (database.is_table(_table_name) && _if_not_exists == false)
        {
            throw exception::PlanningException{std::string{"Table "} + _table_name + " already exists."};
        }

        return NotSchematizedNode::schema(database);
    }

private:
    std::string _table_name;
    topology::PhysicalSchema _physical_schema;
    const bool _if_not_exists;
};
} // namespace db::plan::logical