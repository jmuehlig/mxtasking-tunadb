#pragma once

#include <fmt/core.h>
#include <optional>
#include <string>

namespace db::expression {
class Attribute
{
public:
    class Source
    {
    public:
        explicit Source(std::string &&name) noexcept : _name(std::move(name)) {}

        Source(std::string &&name, std::optional<std::string> &&alias) noexcept
            : _name(std::move(name)), _alias(std::move(alias))
        {
        }

        Source(const std::string &name, const std::optional<std::string> &alias) noexcept : _name(name), _alias(alias)
        {
        }

        Source(Source &&) noexcept = default;

        Source(const Source &) = default;

        ~Source() = default;

        Source &operator=(Source &&) noexcept = default;
        Source &operator=(const Source &) = default;

        [[nodiscard]] const std::string &name() const noexcept { return _name; }
        [[nodiscard]] const std::optional<std::string> &alias() const noexcept { return _alias; }

        [[nodiscard]] std::string to_string() const { return _alias.value_or(_name); }

        bool operator==(const Source &other) const noexcept
        {
            if (_alias.has_value())
            {
                return _alias == other._alias;
            }

            return _alias.value_or(_name) == other._alias.value_or(other._name);
        }

    private:
        std::string _name;
        std::optional<std::string> _alias{std::nullopt};
    };

    explicit Attribute(std::string &&name) noexcept : _name(std::move(name)) {}
    explicit Attribute(const std::string &name) : Attribute(std::string{name}) {}

    Attribute(Source &&source, std::string &&name) noexcept : _source(std::move(source)), _name(std::move(name)) {}
    Attribute(const Source &source, const std::string &name) : Attribute(Source{source}, std::string{name}) {}
    Attribute(const Source &source, std::string &&name) : Attribute(Source{source}, std::move(name)) {}
    Attribute(std::optional<Source> &&source, std::string &&name) noexcept
        : _source(std::move(source)), _name(std::move(name))
    {
    }
    Attribute(std::optional<Source> &&source, std::string &&name, const bool print_table_name) noexcept
        : _source(std::move(source)), _name(std::move(name)), _print_table_name(print_table_name)
    {
    }
    Attribute(const std::optional<Source> &source, std::string &&name)
        : Attribute(std::optional<Source>{source}, std::move(name))
    {
    }
    Attribute(const std::optional<Source> &source, const std::string &name) : Attribute(source, std::string{name}) {}
    Attribute(const Attribute &other, const bool print_table_name)
        : _source(other._source), _name(other._name), _print_table_name(print_table_name)
    {
    }

    Attribute(Attribute &&) noexcept = default;
    Attribute(const Attribute &) = default;
    ~Attribute() = default;

    Attribute &operator=(Attribute &&) noexcept = default;
    Attribute &operator=(const Attribute &) noexcept = default;

    bool operator==(const Attribute &other) const noexcept
    {
        if (_source.has_value() && other.source().has_value())
        {
            return _source == other._source && _name == other._name;
        }

        return _name == other._name;
    }

    [[nodiscard]] const std::optional<Source> &source() const noexcept { return _source; }
    void source(const Source &source) { _source = source; }
    [[nodiscard]] const std::string &column_name() const noexcept { return _name; }
    [[nodiscard]] bool is_asterisk() const noexcept { return _name == "*"; }
    [[nodiscard]] bool is_print_table_name() const noexcept { return _print_table_name; }

    [[nodiscard]] std::string to_string() const
    {
        if (_print_table_name && _source.has_value())
        {
            return fmt::format("{}.{}", _source->to_string(), _name);
        }

        return _name;
    }

private:
    std::optional<Source> _source{std::nullopt};
    std::string _name;
    bool _print_table_name{false};
};
} // namespace db::expression

namespace std {
template <> struct hash<db::expression::Attribute>
{
public:
    std::size_t operator()(const db::expression::Attribute &attribute) const
    {
        return std::hash<std::string>()(attribute.to_string());
    }
};

template <> struct hash<db::expression::Attribute::Source>
{
public:
    std::size_t operator()(const db::expression::Attribute::Source &source) const
    {
        return std::hash<std::string>()(source.to_string());
    }
};
} // namespace std