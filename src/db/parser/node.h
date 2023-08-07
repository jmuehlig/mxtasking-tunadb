#pragma once
#include "node_interface.h"
#include <db/data/value.h>
#include <db/expression/limit.h>
#include <db/expression/operation.h>
#include <db/expression/order_by.h>
#include <db/expression/term.h>
#include <db/plan/logical/table.h>
#include <db/topology/physical_schema.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace db::parser {
class CreateStatement final : public NodeInterface
{
public:
    CreateStatement(std::string &&table_name, const bool if_not_exists, db::topology::PhysicalSchema &&schema) noexcept
        : _table_name(std::move(table_name)), _if_not_exists(if_not_exists), _schema(std::move(schema))
    {
    }
    ~CreateStatement() noexcept override = default;

    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }
    [[nodiscard]] bool if_not_exists() const noexcept { return _if_not_exists; }
    [[nodiscard]] db::topology::PhysicalSchema &schema() { return _schema; }

private:
    std::string _table_name;
    bool _if_not_exists;
    db::topology::PhysicalSchema _schema;
};

class InsertStatement final : public NodeInterface
{
public:
    InsertStatement(std::string &&table_name, std::vector<std::string> &&column_names,
                    std::vector<std::vector<data::Value>> &&values) noexcept
        : _table_name(std::move(table_name)), _column_names(std::move(column_names)), _values(std::move(values))
    {
    }
    ~InsertStatement() noexcept override = default;

    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }
    [[nodiscard]] std::vector<std::string> &column_names() noexcept { return _column_names; }
    [[nodiscard]] std::vector<std::vector<data::Value>> &values() noexcept { return _values; }

private:
    std::string _table_name;
    std::vector<std::string> _column_names;
    std::vector<std::vector<data::Value>> _values;
};

class SelectQuery final : public NodeInterface
{
public:
    enum class ExplainLevel : std::uint8_t
    {
        Plan,
        TaskGraph,
        DataFlowGraph,
        Performance,
        TaskLoad,
        TaskTraces,
        Flounder,
        Assembly,
        DRAMBandwidth,
        Times
    };

    enum class SampleCounterType : std::uint8_t
    {
        Branches,
        BranchMisses,
        Cycles,
        Instructions,
        CacheMisses,
        CacheReferences,
        StallsMemAny,
        StallsL3Miss,
        StallsL2Miss,
        StallsL1DMiss,
        CyclesL3Miss,
        DTLBMiss,
        L3MissRemote,
        FillBufferFull,
        LoadHitL1DFillBuffer,
        BAClearsAny,
        MemRetiredLoads,
        MemRetiredStores,
        MemRetiredLoadL1Miss,
        MemRetiredLoadL2Miss,
        MemRetiredLoadL3Miss,
    };

    enum class SampleLevel : std::uint8_t
    {
        Assembly,
        Operators,
        Memory,
        HistoricalMemory
    };

    SelectQuery(std::vector<std::unique_ptr<expression::Operation>> &&attributes,
                std::vector<plan::logical::TableReference> &&from,
                std::optional<std::vector<plan::logical::JoinReference>> &&join,
                std::unique_ptr<expression::Operation> &&where, std::optional<std::vector<expression::Term>> &&group_by,
                std::optional<std::vector<expression::OrderBy>> &&order_by,
                std::optional<expression::Limit> &&limit) noexcept
        : _attributes(std::move(attributes)), _from(std::move(from)), _join(std::move(join)), _where(std::move(where)),
          _group_by(std::move(group_by)), _order_by(std::move(order_by)), _limit(std::move(limit))
    {
    }

    ~SelectQuery() noexcept override = default;

    [[nodiscard]] std::optional<ExplainLevel> explain_level() const noexcept { return _explain_level; }
    [[nodiscard]] std::optional<SampleLevel> sample_level() const noexcept { return _sample_level; }
    [[nodiscard]] std::optional<SampleCounterType> sample_counter_type() const noexcept { return _sample_counter_type; }
    [[nodiscard]] std::optional<std::uint64_t> sample_frequency() const noexcept { return _sample_frequency; }
    [[nodiscard]] std::vector<std::unique_ptr<expression::Operation>> &attributes() noexcept { return _attributes; }
    [[nodiscard]] std::vector<plan::logical::TableReference> &from() noexcept { return _from; }
    [[nodiscard]] std::optional<std::vector<plan::logical::JoinReference>> &join() noexcept { return _join; }
    [[nodiscard]] std::unique_ptr<expression::Operation> &where() noexcept { return _where; }
    [[nodiscard]] std::optional<std::vector<expression::Term>> &group_by() noexcept { return _group_by; }
    [[nodiscard]] std::optional<std::vector<expression::OrderBy>> &order_by() noexcept { return _order_by; }
    [[nodiscard]] std::optional<expression::Limit> &limit() noexcept { return _limit; }

    void explain_level(const ExplainLevel explain_level) noexcept { _explain_level = explain_level; }
    void sample(const SampleLevel sample_level, const SampleCounterType sample_counter_type,
                const std::optional<std::uint64_t> sample_frequency) noexcept
    {
        _sample_level = sample_level;
        _sample_counter_type = sample_counter_type;
        _sample_frequency = sample_frequency;
    }

private:
    std::optional<ExplainLevel> _explain_level{std::nullopt};
    std::optional<SampleLevel> _sample_level{std::nullopt};
    std::optional<SampleCounterType> _sample_counter_type{std::nullopt};
    std::optional<std::uint64_t> _sample_frequency{std::nullopt};
    std::vector<std::unique_ptr<expression::Operation>> _attributes;
    std::vector<plan::logical::TableReference> _from;
    std::optional<std::vector<plan::logical::JoinReference>> _join;
    std::unique_ptr<expression::Operation> _where;
    std::optional<std::vector<expression::Term>> _group_by;
    std::optional<std::vector<expression::OrderBy>> _order_by;
    std::optional<expression::Limit> _limit;
};

class StopCommand final : public NodeInterface
{
public:
    StopCommand() noexcept = default;
    ~StopCommand() noexcept override = default;
};

class ShowTablesCommand final : public NodeInterface
{
public:
    ShowTablesCommand() noexcept = default;
    ~ShowTablesCommand() noexcept override = default;
};

class DescribeTableCommand final : public NodeInterface
{
public:
    DescribeTableCommand(std::string &&table_name) noexcept : _table_name(std::move(table_name)) {}
    ~DescribeTableCommand() noexcept override = default;

    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }

private:
    std::string _table_name;
};

class LoadFileCommand final : public NodeInterface
{
public:
    LoadFileCommand(std::string &&file) noexcept : _file(std::move(file)) {}

    ~LoadFileCommand() noexcept override = default;

    [[nodiscard]] std::string &file() noexcept { return _file; }

private:
    std::string _file;
};

class CopyStatement final : public NodeInterface
{
public:
    CopyStatement(std::string &&table_name, std::string &&file, std::string &&separator) noexcept
        : _table_name(std::move(table_name)), _file(std::move(file)), _separator(std::move(separator))
    {
    }

    ~CopyStatement() noexcept override = default;

    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }
    [[nodiscard]] std::string &file() noexcept { return _file; }
    [[nodiscard]] std::string &separator() noexcept { return _separator; }

private:
    std::string _table_name;
    std::string _file;
    std::string _separator;
};

class StoreCommand final : public NodeInterface
{
public:
    StoreCommand(std::string &&file_name) noexcept : _file_name(std::move(file_name)) {}

    ~StoreCommand() noexcept override = default;

    [[nodiscard]] std::string &file_name() noexcept { return _file_name; }

private:
    std::string _file_name;
};

class RestoreCommand final : public NodeInterface
{
public:
    RestoreCommand(std::string &&file_name) noexcept : _file_name(std::move(file_name)) {}

    ~RestoreCommand() noexcept override = default;

    [[nodiscard]] std::string &file_name() noexcept { return _file_name; }

private:
    std::string _file_name;
};

class SetCoresCommand final : public NodeInterface
{
public:
    constexpr SetCoresCommand(const std::uint16_t count_cores) noexcept : _count_cores(count_cores) {}

    ~SetCoresCommand() noexcept override = default;

    [[nodiscard]] std::uint16_t count_cores() const noexcept { return _count_cores; }

private:
    const std::uint16_t _count_cores;
};

class GetConfigurationCommand final : public NodeInterface
{
public:
    constexpr GetConfigurationCommand() noexcept = default;
    ~GetConfigurationCommand() noexcept override = default;
};

class UpdateStatisticsCommand final : public NodeInterface
{
public:
    UpdateStatisticsCommand(std::string &&table_name) noexcept : _table_name(std::move(table_name)) {}
    ~UpdateStatisticsCommand() noexcept override = default;

    [[nodiscard]] std::string &table_name() noexcept { return _table_name; }

private:
    std::string _table_name;
};
} // namespace db::parser