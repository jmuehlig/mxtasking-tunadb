#pragma once
#include "char.h"
#include "date.h"
#include "decimal.h"
#include <cstdint>
#include <flounder/ir/register.h>
#include <string>
#include <variant>

namespace db::type {
enum Id
{
    INT,
    BIGINT,
    DECIMAL,
    CHAR,
    DATE,
    BOOL,
    UNKNOWN
};

template <Id I> struct underlying
{
    using value = std::int32_t;
};
template <> struct underlying<Id::INT>
{
    using value = std::int32_t;
};
template <> struct underlying<Id::BIGINT>
{
    using value = std::int64_t;
};
template <> struct underlying<Id::DECIMAL>
{
    using value = Decimal::value_t;
};
template <> struct underlying<Id::CHAR>
{
    using value = char;
};
template <> struct underlying<Id::DATE>
{
    using value = Date;
};
template <> struct underlying<Id::BOOL>
{
    using value = bool;
};

template <Id I> struct view
{
    using value = typename underlying<I>::value;
};

template <> struct view<Id::CHAR>
{
    using value = std::string_view;
};

template <Id I> struct store
{
    using value = typename underlying<I>::value;
};

template <> struct store<Id::CHAR>
{
    using value = std::string;
};

class Type
{
public:
    [[nodiscard]] static Type make_int() noexcept { return Type{Id::INT}; }
    [[nodiscard]] static Type make_bigint() noexcept { return Type{Id::BIGINT}; }
    [[nodiscard]] static Type make_date() noexcept { return Type{Id::DATE}; }
    [[nodiscard]] static Type make_bool() noexcept { return Type{Id::BOOL}; }
    [[nodiscard]] static Type make_decimal(const std::uint8_t precision, const std::uint8_t scale) noexcept
    {
        return Type{DecimalDescription{precision, scale}};
    }
    [[nodiscard]] static Type make_decimal(DecimalDescription description) noexcept { return Type{description}; }
    [[nodiscard]] static Type make_char(const std::uint16_t length) noexcept
    {
        return Type{Id::CHAR, CharDescription{length}};
    }

    constexpr Type() noexcept = default;
    Type(Type &&) noexcept = default;
    explicit Type(const Id id) noexcept : _id(id) {}
    Type(const Type &) noexcept = default;
    ~Type() noexcept = default;

    Type &operator=(Type &&) noexcept = default;
    Type &operator=(const Type &) noexcept = default;

    [[nodiscard]] Id id() const noexcept { return _id; }
    [[nodiscard]] DecimalDescription decimal_description() const noexcept
    {
        return std::get<DecimalDescription>(_description);
    }
    [[nodiscard]] CharDescription char_description() const noexcept { return std::get<CharDescription>(_description); }

    bool operator==(const Id id) const { return _id == id; }
    bool operator==(const Type other) const
    {
        if (other != _id)
        {
            return false;
        }

        //        if (_id == Id::DECIMAL)
        //        {
        //            return decimal_description() == other.decimal_description();
        //        }

        return true;
    }

    [[nodiscard]] std::uint16_t size() const noexcept
    {
        switch (_id)
        {
        case Id::INT:
            return sizeof(underlying<Id::INT>::value);
        case Id::BIGINT:
            return sizeof(underlying<Id::BIGINT>::value);
        case Id::DECIMAL:
            return sizeof(underlying<Id::DECIMAL>::value);
        case Id::DATE:
            return sizeof(underlying<Id::DATE>::value);
        case Id::BOOL:
            return sizeof(underlying<Id::BOOL>::value);
        case Id::CHAR:
            return sizeof(underlying<Id::CHAR>::value) * std::get<CharDescription>(_description).length();
        case Id::UNKNOWN:
            return 0U;
        }
    }

    [[nodiscard]] enum flounder::RegisterWidth register_width() const noexcept
    {
        switch (_id)
        {
        case Id::BOOL:
            return flounder::RegisterWidth::r8;
        case Id::INT:
        case Id::DATE:
            return flounder::RegisterWidth::r32;
        case Id::UNKNOWN:
        case Id::BIGINT:
        case Id::DECIMAL:
            return flounder::RegisterWidth::r64;
        case Id::CHAR:
            switch (char_description().length())
            {
            case 1U:
                return flounder::RegisterWidth::r8;
            case 2U:
                return flounder::RegisterWidth::r16;
            case 4U:
                return flounder::RegisterWidth::r32;
            default:
                return flounder::RegisterWidth::r64;
            }
        }
    }

    [[nodiscard]] std::uint64_t min_value() const noexcept
    {
        switch (_id)
        {
        case Id::INT:
            return std::numeric_limits<underlying<Id::INT>::value>::min();
        case Id::BIGINT:
            return std::numeric_limits<underlying<Id::BIGINT>::value>::min();
        case Id::DECIMAL:
            return std::numeric_limits<underlying<Id::DECIMAL>::value>::min();
        case Id::DATE:
        case Id::BOOL:
        case Id::CHAR:
        case Id::UNKNOWN:
            return 0U;
        }
    }

    [[nodiscard]] std::uint64_t max_value() const noexcept
    {
        switch (_id)
        {
        case Id::INT:
            return std::numeric_limits<underlying<Id::INT>::value>::max();
        case Id::BIGINT:
        case Id::UNKNOWN:
            return std::numeric_limits<underlying<Id::BIGINT>::value>::max();
        case Id::DECIMAL:
            return std::numeric_limits<underlying<Id::DECIMAL>::value>::max();
        case Id::DATE:
            return underlying<Id::DATE>::value{9999, 99, 99}.data();
        case Id::BOOL:
            return 1;
        case Id::CHAR:
            return std::numeric_limits<underlying<Id::CHAR>::value>::max();
        }
    }

    bool operator<(const Type &other) const;

    [[nodiscard]] std::string to_string() const;

    Type operator+(Type other) const;
    Type operator-(Type other) const;
    Type operator*(Type other) const;
    Type operator/(Type other) const;

    [[nodiscard]] static std::int64_t decimal_conversion_factor_for_mul(const DecimalDescription left,
                                                                        const DecimalDescription right) noexcept;
    [[nodiscard]] static std::int64_t decimal_conversion_factor_for_div(const DecimalDescription left,
                                                                        const DecimalDescription right) noexcept;

private:
    Type(const Id id, CharDescription char_description) noexcept : _id(id), _description(char_description) {}
    explicit Type(DecimalDescription decimal_description) noexcept : _id(Id::DECIMAL), _description(decimal_description)
    {
    }

    Id _id{Id::UNKNOWN};
    std::variant<DecimalDescription, CharDescription, std::monostate> _description{std::monostate{}};
};
} // namespace db::type