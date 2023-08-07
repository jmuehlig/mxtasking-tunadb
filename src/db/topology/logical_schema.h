#pragma once

#include <cstdint>
#include <db/expression/term.h>
#include <db/type/type.h>
#include <fmt/format.h>
#include <optional>
#include <vector>

namespace db::topology {
class LogicalSchema
{
public:
    LogicalSchema() = default;
    LogicalSchema(LogicalSchema &&) noexcept = default;
    LogicalSchema(const LogicalSchema &) = default;

    virtual ~LogicalSchema() noexcept = default;

    LogicalSchema &operator=(LogicalSchema &&) noexcept = default;
    LogicalSchema &operator=(const LogicalSchema &) = default;

    bool operator==(const LogicalSchema &) const noexcept = default;

    /**
     * Adds the given term and type to the end of the schema.
     *
     * @param term Term to add.
     * @param type Type of the term.
     */
    void emplace_back(expression::Term &&term, const type::Type type)
    {
        _terms.emplace_back(std::move(term));
        _types.emplace_back(type);
    }

    /**
     * Adds the given term and type to the end of the schema.
     *
     * @param term Term to add.
     * @param type Type of the term.
     */
    void emplace_back(const expression::Term &term, const type::Type type)
    {
        emplace_back(expression::Term{term}, type);
    }

    /**
     * Adds the given schema to the end of this schema.
     *
     * @param other Reference to the schema that should be added.
     */
    virtual void push_back(const LogicalSchema &other)
    {
        reserve(_terms.size() + other._terms.size());
        _terms.insert(_terms.end(), other._terms.begin(), other._terms.end());
        _types.insert(_types.end(), other._types.begin(), other._types.end());
    }

    /**
     * Resets all terms and types.
     */
    void clear() noexcept
    {
        _terms.clear();
        _types.clear();
    }

    /**
     * Reserves space for given number of elements.
     *
     * @param count Number of elements.
     */
    virtual void reserve(const std::size_t count)
    {
        _terms.reserve(count);
        _types.reserve(count);
    }

    /**
     * @return Number of terms in this schema.
     */
    [[nodiscard]] virtual std::uint16_t size() const noexcept { return _terms.size(); }

    /***
     * Locates the index of the given term in the schema.
     *
     * @param term Term to locate.
     * @return Optional index, which is std::nullopt when the term was not found.
     */
    [[nodiscard]] virtual std::optional<std::uint16_t> index(const expression::Term &term) const noexcept
    {
        const auto iterator = std::find(_terms.begin(), _terms.end(), term);
        if (iterator != _terms.end())
        {
            return std::make_optional(std::distance(_terms.begin(), iterator));
        }

        return std::nullopt;
    }

    /***
     * Locates the index of the given term in the schema.
     * When the term has an alias, the schema will locate the alias
     * instead of the attribute.
     *
     * @param term Term to locate.
     * @return Optional index, which is std::nullopt when the term was not found.
     */
    [[nodiscard]] virtual std::optional<std::uint16_t> index_include_alias(const expression::Term &term) const noexcept
    {
        if (term.alias().has_value())
        {
            const auto iterator = std::find_if(_terms.begin(), _terms.end(),
                                               [&term](const auto &t) { return t.alias() == term.alias(); });
            if (iterator != _terms.end())
            {
                return std::make_optional(std::distance(_terms.begin(), iterator));
            }

            return std::nullopt;
        }

        return index(term);
    }

    /***
     * Locates the given term and checks if the term is part of the schemas terms.
     *
     * @param term Term to check.
     * @return True, if the term was found in the schemas terms. False, otherwise.
     */
    [[nodiscard]] virtual bool contains(const expression::Term &term) const noexcept
    {
        return std::find(_terms.begin(), _terms.end(), term) != _terms.end();
    }

    /**
     * @return All terms in this schema.
     */
    [[nodiscard]] virtual const std::vector<expression::Term> &terms() const noexcept { return _terms; }

    /**
     * @return All types in this schema.
     */
    [[nodiscard]] virtual const std::vector<type::Type> &types() const noexcept { return _types; }

    /**
     * Accesses a specific term in this schema.
     *
     * @param index Index of the term.
     * @return Term
     */
    [[nodiscard]] virtual const expression::Term &term(const std::uint16_t index) const noexcept
    {
        return _terms[index];
    }

    /**
     * Accesses a specific term in this schema.
     *
     * @param index Index of the term.
     * @return Term
     */
    [[nodiscard]] virtual expression::Term &term(const std::uint16_t index) noexcept { return _terms[index]; }

    /**
     * Accesses a specific type in this schema.
     *
     * @param index Index of the term.
     * @return Type
     */
    [[nodiscard]] virtual const type::Type &type(const std::uint16_t index) const noexcept { return _types[index]; }

    [[nodiscard]] std::string to_string() const
    {
        auto term_names = std::vector<std::string>{};
        std::transform(_terms.begin(), _terms.end(), std::back_inserter(term_names),
                       [](const auto &term) { return term.to_string(); });

        return fmt::format("{}", fmt::join(std::move(term_names), ","));
    }

protected:
    std::vector<expression::Term> _terms;
    std::vector<type::Type> _types;
};
} // namespace db::topology