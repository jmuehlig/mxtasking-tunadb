#pragma once

#include "node_interface.h"
#include <db/exception/plan_exception.h>
#include <string>

namespace db::plan::logical {
class StopNode final : public NotSchematizedNode
{
public:
    StopNode() noexcept : NotSchematizedNode("Stop") {}
    ~StopNode() noexcept override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::STOP; }
};

class ShowTablesNode final : public NotSchematizedNode
{
public:
    ShowTablesNode() : NotSchematizedNode("Show Tables") {}
    ~ShowTablesNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::COMMAND; }
};

class DescribeTableNode final : public NotSchematizedNode
{
public:
    DescribeTableNode(std::string &&table_name)
        : NotSchematizedNode("Describe Table"), _table_name(std::move(table_name))
    {
    }
    ~DescribeTableNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::COMMAND; }
    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }

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
};

class LoadFileNode final : public NotSchematizedNode
{
public:
    LoadFileNode(std::string &&file_name) noexcept : NotSchematizedNode("Load File"), _file_name(std::move(file_name))
    {
    }
    ~LoadFileNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::COMMAND; }

    [[nodiscard]] std::string &file_name() noexcept { return _file_name; }

private:
    std::string _file_name;
};

class StoreNode final : public NotSchematizedNode
{
public:
    StoreNode(std::string &&file_name) noexcept : NotSchematizedNode("Store"), _file_name(std::move(file_name)) {}
    ~StoreNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::COMMAND; }

    [[nodiscard]] std::string &file_name() noexcept { return _file_name; }

private:
    std::string _file_name;
};

class RestoreNode final : public NotSchematizedNode
{
public:
    RestoreNode(std::string &&file_name) noexcept : NotSchematizedNode("Restore"), _file_name(std::move(file_name)) {}
    ~RestoreNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::COMMAND; }

    [[nodiscard]] std::string &file_name() noexcept { return _file_name; }

private:
    std::string _file_name;
};

class GetConfigurationNode final : public NotSchematizedNode
{
public:
    GetConfigurationNode() : NotSchematizedNode("Get Configuration") {}
    ~GetConfigurationNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::CONFIGURATION; }
};

class SetCoresNode final : public NotSchematizedNode
{
public:
    SetCoresNode(const std::uint16_t count_cores) : NotSchematizedNode("Set Cores"), _count_cores(count_cores) {}
    ~SetCoresNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::CONFIGURATION; }
    [[nodiscard]] std::uint16_t count_cores() const noexcept { return _count_cores; }

private:
    const std::uint16_t _count_cores;
};

class UpdateStatisticsNode final : public NotSchematizedNode
{
public:
    UpdateStatisticsNode(std::string &&table_name)
        : NotSchematizedNode("Update Statistics"), _table_name(std::move(table_name))
    {
    }
    ~UpdateStatisticsNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::COMMAND; }
    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }

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
};

} // namespace db::plan::logical