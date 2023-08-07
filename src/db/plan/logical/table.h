#pragma once

#include <db/expression/operation.h>
#include <memory>
#include <string>
#include <variant>

namespace db::plan::logical {

class TableReference
{
public:
    TableReference() noexcept = default;

    TableReference(std::string &&name) noexcept : _name(std::move(name)), _alias(std::nullopt) {}

    TableReference(std::string &&name, std::string &&alias) noexcept : _name(std::move(name)), _alias(std::move(alias))
    {
    }

    TableReference(TableReference &&) noexcept = default;
    TableReference(const TableReference &) noexcept = default;

    TableReference &operator=(TableReference &&) noexcept = default;

    [[nodiscard]] std::string &name() noexcept { return _name; }
    [[nodiscard]] const std::string &name() const noexcept { return _name; }
    [[nodiscard]] std::optional<std::string> &alias() noexcept { return _alias; }
    [[nodiscard]] const std::optional<std::string> &alias() const noexcept { return _alias; }

private:
    std::string _name;
    std::optional<std::string> _alias;
};

class JoinReference
{
public:
    JoinReference() noexcept = default;

    JoinReference(TableReference &&table, std::unique_ptr<expression::Operation> &&predicate) noexcept
        : _join_table(std::move(table)), _join_predicate(std::move(predicate))
    {
    }

    explicit JoinReference(TableReference &&table) noexcept : _join_table(std::move(table)) {}

    JoinReference(JoinReference &&) noexcept = default;

    ~JoinReference() noexcept = default;

    JoinReference &operator=(JoinReference &&) noexcept = default;

    [[nodiscard]] const TableReference &join_table() const noexcept { return _join_table; }
    [[nodiscard]] TableReference &join_table() noexcept { return _join_table; }
    [[nodiscard]] const std::unique_ptr<expression::Operation> &predicate() const noexcept { return _join_predicate; }
    [[nodiscard]] std::unique_ptr<expression::Operation> &predicate() noexcept { return _join_predicate; }

private:
    TableReference _join_table;
    std::unique_ptr<expression::Operation> _join_predicate{nullptr};
};
} // namespace db::plan::logical