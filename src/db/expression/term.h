#pragma once

#include "attribute.h"
#include <cstdint>
#include <db/data/value.h>
#include <optional>
#include <string>
#include <variant>

namespace db::expression {
using NullValue = std::nullptr_t;
class Term
{
public:
    Term() noexcept = default;
    explicit Term(Attribute &&reference, const bool is_generated = false)
        : _attribute_or_value(std::move(reference)), _is_generated(is_generated)
    {
    }
    explicit Term(const Attribute &reference) : Term(Attribute{reference}) {}
    explicit Term(data::Value &&value) : _attribute_or_value(std::move(value)) {}
    explicit Term(const NullValue /*null*/) : _attribute_or_value(NullValue{}) {}

    Term(Attribute &&reference, std::optional<std::string> &&alias)
        : _attribute_or_value(std::move(reference)), _alias(std::move(alias))
    {
    }

    Term(Attribute &&reference, std::optional<std::string> &&alias, bool is_generated)
        : _attribute_or_value(std::move(reference)), _alias(std::move(alias)), _is_generated(is_generated)
    {
    }

    Term(const Attribute &reference, std::optional<std::string> &&alias) : Term(Attribute{reference}, std::move(alias))
    {
    }
    Term(data::Value &&value, std::optional<std::string> &&alias) noexcept
        : _attribute_or_value(std::move(value)), _alias(std::move(alias))
    {
    }
    Term(data::Value &&value, const bool is_generated) noexcept
        : _attribute_or_value(std::move(value)), _is_generated(is_generated)
    {
    }

    Term(const NullValue /*null*/, std::optional<std::string> &&alias) noexcept
        : _attribute_or_value(NullValue{}), _alias(std::move(alias))
    {
    }
    Term(Term &&) = default;
    Term(const Term &) = default;

    ~Term() = default;

    Term &operator=(Term &&) noexcept = default;
    Term &operator=(const Term &) noexcept = default;

    [[nodiscard]] static Term make_attribute(Attribute::Source &&source, std::string &&column_name,
                                             const bool is_generated = false)
    {
        return Term{Attribute{std::move(source), std::move(column_name)}, is_generated};
    }

    [[nodiscard]] static Term make_attribute(const Attribute::Source &source, std::string &&column_name)
    {
        return Term{Attribute{source, std::move(column_name)}};
    }

    [[nodiscard]] static Term make_attribute(const std::optional<Attribute::Source> &source,
                                             const std::string &column_name)
    {
        return Term{Attribute{source, column_name}};
    }

    [[nodiscard]] static Term make_attribute(std::string &&source_name, std::string &&name,
                                             const bool is_generated = false)
    {
        return make_attribute(Attribute::Source{std::move(source_name)}, std::move(name), is_generated);
    }

    [[nodiscard]] static Term make_attribute(std::string &&name, const bool is_generated = false)
    {
        return Term{Attribute{std::nullopt, std::move(name)}, is_generated};
    }

    [[nodiscard]] static Term make_attribute(const std::string &name, const bool is_generated = false)
    {
        return make_attribute(std::string{name}, is_generated);
    }

    [[nodiscard]] const std::variant<Attribute, data::Value, NullValue> &attribute_or_value() const
    {
        return _attribute_or_value;
    }

    [[nodiscard]] const std::optional<std::string> &alias() const { return _alias; }
    void alias(const std::string &alias) { _alias = alias; }
    void alias(std::string &&alias) noexcept { _alias = std::move(alias); }

    [[nodiscard]] bool is_attribute() const { return std::holds_alternative<Attribute>(_attribute_or_value); }
    [[nodiscard]] bool is_null() const { return std::holds_alternative<NullValue>(_attribute_or_value); }
    [[nodiscard]] bool is_value() const { return std::holds_alternative<data::Value>(_attribute_or_value); }

    template <typename T> [[nodiscard]] const T &get() const { return std::get<T>(_attribute_or_value); }

    template <typename T> [[nodiscard]] T &get() { return std::get<T>(_attribute_or_value); }

    [[nodiscard]] bool is_generated() const { return _is_generated; }

    bool operator==(const Term &other) const
    {
        return _attribute_or_value == other._attribute_or_value; // && _alias == other._alias;
    }

    [[nodiscard]] std::string to_string() const noexcept
    {
        if (_alias.has_value())
        {
            return _alias.value();
        }

        if (std::holds_alternative<Attribute>(_attribute_or_value))
        {
            return std::get<Attribute>(_attribute_or_value).to_string();
        }

        if (std::holds_alternative<data::Value>(_attribute_or_value))
        {
            return std::get<data::Value>(_attribute_or_value).to_string();
        }

        if (std::holds_alternative<NullValue>(_attribute_or_value))
        {
            return std::string{"NULL"};
        }

        return "";
    }

private:
    std::variant<Attribute, data::Value, NullValue> _attribute_or_value{NullValue{}};
    std::optional<std::string> _alias{std::nullopt};
    bool _is_generated{false};
};
} // namespace db::expression

namespace std {
template <> struct hash<db::expression::Term>
{
public:
    std::size_t operator()(const db::expression::Term &term) const
    {
        return std::hash<std::string>()(term.to_string());
    }
};
} // namespace std