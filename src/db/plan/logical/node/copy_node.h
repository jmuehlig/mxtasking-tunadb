#pragma once
#include "node_interface.h"
#include <db/data/value.h>
#include <db/exception/plan_exception.h>
#include <db/topology/database.h>
#include <string>
#include <vector>

namespace db::plan::logical {
class CopyNode final : public NotSchematizedNode
{
public:
    CopyNode(std::string &&table_name, std::string &&file_name, std::string &&separator) noexcept
        : NotSchematizedNode("COPY"), _table_name(std::move(table_name)), _file_name(std::move(file_name)),
          _separator(std::move(separator))
    {
    }
    ~CopyNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::COMMAND; }

    [[nodiscard]] topology::LogicalSchema schema(const topology::Database &database) const override
    {
        if (database.is_table(_table_name) == false)
        {
            throw exception::TableNotFoundException(_table_name);
        }

        return NotSchematizedNode::schema(database);
    }

    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }
    [[nodiscard]] std::string &file_name() noexcept { return _file_name; }
    [[nodiscard]] std::string &separator() noexcept { return _separator; }

private:
    std::string _table_name;
    std::string _file_name;
    std::string _separator;
};
} // namespace db::plan::logical