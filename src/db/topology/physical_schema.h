#pragma once
#include "logical_schema.h"
#include <db/config.h>
#include <mx/memory/alignment_helper.h>
#include <optional>
#include <utility>
#include <vector>

namespace db::topology {
class PhysicalSchema final : private LogicalSchema
{
public:
    [[nodiscard]] static PhysicalSchema from_logical(const LogicalSchema &logical_schema)
    {
        auto schema = PhysicalSchema{};
        schema.push_back(logical_schema);
        return schema;
    }

    [[nodiscard]] static PhysicalSchema from_logical(const LogicalSchema &logical_schema,
                                                     const std::vector<expression::Term> &terms,
                                                     const bool is_include_terms)
    {
        auto schema = PhysicalSchema{};
        schema.push_back(logical_schema, terms, is_include_terms);
        return schema;
    }

    [[nodiscard]] static PhysicalSchema make_combination(const PhysicalSchema &left_schema,
                                                         const PhysicalSchema &right_schema)
    {
        auto schema = PhysicalSchema{left_schema};
        schema.push_back(right_schema);

        return schema;
    }

    PhysicalSchema() noexcept = default;
    PhysicalSchema(PhysicalSchema &&) noexcept = default;
    PhysicalSchema(const PhysicalSchema &) noexcept = default;
    ~PhysicalSchema() noexcept override = default;

    PhysicalSchema &operator=(PhysicalSchema &&) noexcept = default;

    /**
     * Adds the given term and type to the end of the schema.
     *
     * @param term Term to add.
     * @param type Type of the term.
     * @param is_nullable True, if the column could contain null values.
     */
    void emplace_back(expression::Term &&term, const type::Type type, const bool is_nullable = false,
                      const bool is_primary_key = false)
    {
        LogicalSchema::emplace_back(std::move(term), type);
        _nullables.emplace_back(is_nullable);
        _primary_keys.emplace_back(is_primary_key);
        _order.emplace_back(_terms.size() - 1U);

        const auto row_offset = std::exchange(_row_size, _row_size + type.size());
        _row_offsets.emplace_back(row_offset);

        if (_pax_offsets.empty())
        {
            _pax_offsets.emplace_back(0U);
        }
        else
        {
            const auto last_offset = _pax_offsets.back();
            const auto last_size = _types[_types.size() - 2U].size() * config::tuples_per_tile();
            const auto offset = last_offset + last_size;
            _pax_offsets.emplace_back(mx::memory::alignment_helper::next_multiple(offset, 64UL));
        }
    }

    /**
     * Adds the given schema to the end of this schema.
     *
     * @param other Reference to the schema that should be added.
     */
    void push_back(const PhysicalSchema &other)
    {
        for (auto i = 0U; i < other.size(); ++i)
        {
            emplace_back(expression::Term{other._terms[i]}, type::Type{other._types[i]}, other._nullables[i],
                         other._primary_keys[i]);
        }
    }

    /**
     * Adds the given schema to the end of this schema.
     *
     * @param other Reference to the schema that should be added.
     */
    void push_back(const LogicalSchema &logical_schema) override
    {
        reserve(logical_schema.size());
        for (auto i = 0U; i < logical_schema.size(); ++i)
        {
            emplace_back(expression::Term{logical_schema.term(i)}, type::Type{logical_schema.type(i)});
        }
    }

    /**
     * Adds the given schema to the end of this schema.
     *
     * @param other Reference to the schema that should be added.
     * @param terms List of terms that should be included or excluded.
     * @param is_include_terms If true, only the given terms will be pushed back, otherwise all terms but the given will
     * be pushed.
     */
    void push_back(const LogicalSchema &logical_schema, const std::vector<expression::Term> &terms,
                   const bool is_include_terms)
    {
        if (is_include_terms)
        {
            reserve(terms.size());
            for (const auto &term : terms)
            {
                const auto index = logical_schema.index(term);
                if (index.has_value())
                {
                    emplace_back(expression::Term{logical_schema.term(index.value())},
                                 type::Type{logical_schema.type(index.value())});
                }
            }
        }
        else
        {
            reserve(logical_schema.size() - terms.size());
            for (auto i = 0U; i < logical_schema.size(); ++i)
            {
                const auto &term = logical_schema.term(i);
                if (std::find(terms.begin(), terms.end(), term) == terms.end())
                {
                    emplace_back(expression::Term{term}, type::Type{logical_schema.type(i)});
                }
            }
        }
    }

    void push_back_missing(const LogicalSchema &logical_schema)
    {
        for (auto i = 0U; i < logical_schema.size(); ++i)
        {
            if (this->index(logical_schema.term(i)) == std::nullopt)
            {
                emplace_back(expression::Term{logical_schema.term(i)}, type::Type{logical_schema.type(i)});
            }
        }
    }

    /**
     * @return The terms of this schema.
     */
    [[nodiscard]] const std::vector<expression::Term> &terms() const noexcept override
    {
        return LogicalSchema::terms();
    }

    /**
     * @return The types of the terms.
     */
    [[nodiscard]] const std::vector<type::Type> &types() const noexcept override { return LogicalSchema::types(); }

    /**
     * Updates the logical order of the terms.
     * @param order New order of the terms.
     */
    void order(std::vector<std::uint16_t> &&order) noexcept { _order = std::move(order); }

    /**
     * @return The order of the terms.
     */
    [[nodiscard]] const std::vector<std::uint16_t> &order() const noexcept { return _order; }

    /**
     * @return Size of all terms (more specifically their physical types).
     */
    [[nodiscard]] std::uint16_t row_size() const noexcept { return _row_size; }

    /**
     * Access to the offset on a specific index in a row storage.
     *
     * @param index Index of the term.
     * @return Offset of the term in a row storage.
     */
    [[nodiscard]] std::uint16_t row_offset(const std::uint16_t index) const noexcept { return _row_offsets[index]; }

    /**
     * Access to the offset on a specific index in a PAX storage.
     *
     * @param index Index of the term.
     * @return Offset of the term in a PAX storage.
     */
    [[nodiscard]] std::uint64_t pax_offset(const std::uint16_t index) const noexcept { return _pax_offsets[index]; }

    /**
     * Access to a is_null specification of a specific term.
     *
     * @param index Index of the term.
     * @return True, if the term is is_null.
     */
    [[nodiscard]] bool is_null(const std::uint16_t index) const noexcept { return _nullables[index]; }

    /**
     * Access to a primary key specification of a specific term.
     *
     * @param index Index of the term.
     * @return True, if the term is a primary key.
     */
    [[nodiscard]] bool is_primary_key(const std::uint16_t index) const noexcept { return _primary_keys[index]; }

    /**
     * @return True if the schema is empty.
     */
    [[nodiscard]] bool empty() const noexcept { return _terms.empty(); }

    /**
     * Reserves space for given number of elements.
     *
     * @param count Number of elements.
     */
    void reserve(const std::size_t count) override
    {
        LogicalSchema::reserve(count);
        _order.reserve(count);
        _row_offsets.reserve(count);
        _pax_offsets.reserve(count);
    }

    /**
     * @return Number of terms in this schema.
     */
    [[nodiscard]] std::uint16_t size() const noexcept override { return LogicalSchema::size(); }

    /***
     * Locates the index of the given term in the schema.
     *
     * @param term Term to locate.
     * @return Optional index, which is std::nullopt when the term was not found.
     */
    [[nodiscard]] std::optional<std::uint16_t> index(const expression::Term &term) const noexcept override
    {
        return LogicalSchema::index(term);
    }

    /***
     * Locates the index of the given term in the schema.
     * When the term has an alias, the schema will locate the alias
     * instead of the attribute.
     *
     * @param term Term to locate.
     * @return Optional index, which is std::nullopt when the term was not found.
     */
    [[nodiscard]] std::optional<std::uint16_t> index_include_alias(const expression::Term &term) const noexcept override
    {
        return LogicalSchema::index_include_alias(term);
    }

    /**
     * Locates the index of the given attribute name in the schema.
     *
     * @param attribute_name Attribute name to locate.
     * @return Optional index, which is std::nullopt when the term was not found.
     */
    [[nodiscard]] std::optional<std::uint16_t> index(const std::string &attribute_name) const noexcept
    {
        return LogicalSchema::index(expression::Term::make_attribute(attribute_name));
    }

    /**
     * Accesses a specific term in this schema.
     *
     * @param index Index of the term.
     * @return Term
     */
    [[nodiscard]] const expression::Term &term(const std::uint16_t index) const noexcept override
    {
        return LogicalSchema::term(index);
    }

    /**
     * Accesses a specific term in this schema.
     *
     * @param index Index of the term.
     * @return Term
     */
    [[nodiscard]] expression::Term &term(const std::uint16_t index) noexcept override
    {
        return LogicalSchema::term(index);
    }

    /**
     * Accesses a specific type in this schema.
     *
     * @param index Index of the term.
     * @return Type
     */
    [[nodiscard]] const type::Type &type(const std::uint16_t index) const noexcept override
    {
        return LogicalSchema::type(index);
    }

    /**
     * Set the type for a specific index.
     *
     * @param index Index of the term.
     * @param type New type.
     */
    void type(const std::uint16_t index, type::Type type) noexcept { _types[index] = type; }

    /**
     * Aligns the schema to have a size multiple to the given base.
     *
     * @param base Base to align this schema to.
     */
    void align_to(const std::uint16_t base)
    {
        const auto aligned_size = mx::memory::alignment_helper::next_multiple(_row_size, base);
        if (aligned_size > _row_size)
        {
            const auto rest = aligned_size - _row_size;
            emplace_back(expression::Term::make_attribute("padding"), type::Type::make_char(rest), true, false);
        }
    }

    bool operator==(const PhysicalSchema &) const noexcept = default;

    [[nodiscard]] std::string to_string() const
    {
        auto terms = std::vector<std::string>{};
        terms.reserve(LogicalSchema::size());
        for (auto i = 0U; i < LogicalSchema::size(); ++i)
        {
            terms.emplace_back(fmt::format("{} ({})", _terms[i].to_string(), _pax_offsets[i]));
        }

        return fmt::format("{}", fmt::join(std::move(terms), ","));
    }

private:
    std::vector<bool> _nullables;
    std::vector<bool> _primary_keys;
    std::vector<std::uint16_t> _order;
    std::vector<std::uint16_t> _row_offsets;
    std::uint16_t _row_size{0U};
    std::vector<std::uint64_t> _pax_offsets;
};
} // namespace db::topology