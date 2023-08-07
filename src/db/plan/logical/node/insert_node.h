#pragma once
#include "node_interface.h"
#include <db/data/value.h>
#include <db/exception/plan_exception.h>
#include <db/topology/database.h>
#include <string>
#include <vector>

namespace db::plan::logical {
class InsertNode final : public NotSchematizedNode
{
public:
    InsertNode(std::string &&table_name, std::vector<std::string> &&column_names,
               std::vector<std::vector<data::Value>> &&value_lists) noexcept
        : NotSchematizedNode("Insert"), _table_name(std::move(table_name)), _column_names(std::move(column_names)),
          _value_lists(std::move(value_lists))
    {
    }
    ~InsertNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::INSERT; }

    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }
    [[nodiscard]] std::vector<std::string> &column_names() noexcept { return _column_names; }
    [[nodiscard]] std::vector<std::vector<data::Value>> &value_lists() noexcept { return _value_lists; }

    [[nodiscard]] topology::LogicalSchema schema(const topology::Database &database) const override
    {
        if (database.is_table(_table_name) == false)
        {
            throw exception::TableNotFoundException{_table_name};
        }

        return NotSchematizedNode::schema(database);
    }

private:
    std::string _table_name;
    std::vector<std::string> _column_names;
    std::vector<std::vector<data::Value>> _value_lists;
};
} // namespace db::plan::logical