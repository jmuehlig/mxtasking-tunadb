#include "type.h"
#include <db/exception/execution_exception.h>
#include <fmt/core.h>

using namespace db::type;

bool Type::operator<(const Type &other) const
{
    if (*this == other)
    {
        return false;
    }

    if (this->_id == Id::INT)
    {
        if (other == Id::DATE || other == Id::CHAR)
        {
            throw exception::CastException{this->to_string(), other.to_string()};
        }

        if (other == Id::BOOL)
        {
            return true;
        }
    }

    if (this->_id == Id::BIGINT)
    {
        if (other == Id::DATE || other == Id::CHAR)
        {
            throw exception::CastException{this->to_string(), other.to_string()};
        }

        if (other == Id::INT || other == Id::BOOL)
        {
            return true;
        }
    }

    if (this->_id == Id::DECIMAL)
    {
        if (other == Id::DATE || other == Id::CHAR)
        {
            throw exception::CastException{this->to_string(), other.to_string()};
        }

        if (other == Id::INT || other == Id::BIGINT || other == Id::BOOL)
        {
            return true;
        }

        if (other == Id::DECIMAL)
        {
            return decimal_description().scale() > other.decimal_description().scale();
        }
    }

    return false;
}

Type Type::operator+(const Type other) const
{
    if (_id == Id::DECIMAL && other == Id::DECIMAL)
    {
        /// https://www.ibm.com/docs/en/rdfi/9.6.0?topic=operators-decimal-arithmetic-in-sql
        const auto description = this->decimal_description();
        const auto other_description = other.decimal_description();

        /// Calculate scale.
        const auto scale = std::max(description.scale(), other_description.scale());

        /// Calculate precision.
        const auto precision =
            std::min(DecimalDescription::max_precision(description.precision(), other_description.precision()),
                     std::uint8_t(scale +
                                  std::max(description.precision() - description.scale(),
                                           other_description.precision() - other_description.scale()) +
                                  1U));

        return Type::make_decimal(DecimalDescription{precision, scale});
    }

    return std::min(*this, other);
}

Type Type::operator-(const Type other) const
{
    return this->operator+(other);
}

Type Type::operator*(const Type other) const
{
    if (this->_id == Id::DECIMAL && other == Id::DECIMAL)
    {
        /// https://www.ibm.com/docs/en/rdfi/9.6.0?topic=operators-decimal-arithmetic-in-sql
        /// Calculate precision.
        const auto precision =
            std::min(DecimalDescription::max_precision(this->decimal_description().precision(),
                                                       other.decimal_description().precision()),
                     std::uint8_t(this->decimal_description().precision() + other.decimal_description().precision()));

        /// Calculate scale.
        const auto scale = std::min(DecimalDescription::max_scale(), std::uint8_t(this->decimal_description().scale() +
                                                                                  other.decimal_description().scale()));

        return Type::make_decimal(precision, scale);
    }

    return std::min(*this, other);
}

Type Type::operator/(const Type other) const
{
    if (this->_id == Id::DECIMAL && other == Id::DECIMAL)
    {
        /// https://www.ibm.com/docs/en/rdfi/9.6.0?topic=operators-decimal-arithmetic-in-sql
        const auto left_precision = this->decimal_description().precision();
        const auto right_precision = other.decimal_description().precision();
        const auto left_scale = this->decimal_description().scale();
        const auto right_scale = other.decimal_description().scale();

        /// Calculate scale.
        const auto scale =
            std::max(DecimalDescription::minimum_divide_scale(),
                     std::min(DecimalDescription::max_scale(),
                              std::uint8_t(DecimalDescription::max_precision(left_precision, right_precision) -
                                           std::uint8_t(left_precision - left_scale + right_scale))));

        /// Calculate precision.
        const auto precision = std::uint8_t(left_precision - left_scale + scale);

        return Type::make_decimal(DecimalDescription{precision, scale});
    }

    return std::min(*this, other);
}

std::string Type::to_string() const
{
    switch (_id)
    {
    case Id::INT:
        return "INT";
    case Id::BIGINT:
        return "BIGINT";
    case Id::DECIMAL: {
        const auto &description = std::get<DecimalDescription>(_description);
        return fmt::format("DECIMAL({},{})", description.precision(), description.scale());
    }
    case Id::DATE:
        return "DATE";
    case Id::BOOL:
        return "BOOL";
    case Id::CHAR: {
        const auto &description = std::get<CharDescription>(_description);
        return fmt::format("CHAR({})", description.length());
    }
    case Id::UNKNOWN:
        return "UNKNOWN";
    }
}

std::int64_t Type::decimal_conversion_factor_for_mul(const DecimalDescription left,
                                                     const DecimalDescription right) noexcept
{
    /// https://www.ibm.com/docs/en/rdfi/9.6.0?topic=operators-decimal-arithmetic-in-sql

    const auto real_scale = std::uint8_t(left.scale() + right.scale());
    if (real_scale > DecimalDescription::max_scale())
    {
        return std::pow(10U, real_scale - DecimalDescription::max_scale());
    }

    if (DecimalDescription::max_scale() > real_scale) [[unlikely]]
    {
        return -1 * std::pow(10U, DecimalDescription::max_scale() - real_scale);
    }

    return 0;
}

std::int64_t Type::decimal_conversion_factor_for_div(const DecimalDescription left,
                                                     const DecimalDescription right) noexcept
{
    /// https://www.ibm.com/docs/en/rdfi/9.6.0?topic=operators-decimal-arithmetic-in-sql
    const auto left_precision = left.precision();
    const auto right_precision = right.precision();
    const auto left_scale = left.scale();
    const auto right_scale = right.scale();

    const auto real_scale = DecimalDescription::max_precision(left_precision, right_precision) -
                            (left_precision - left_scale + right_scale);
    if (real_scale > DecimalDescription::max_scale())
    {
        return std::pow(10U, real_scale - DecimalDescription::max_scale());
    }

    if (real_scale < DecimalDescription::minimum_divide_scale()) [[unlikely]]
    {
        return -1 * std::pow(10U, DecimalDescription::minimum_divide_scale() - real_scale);
    }

    return 0;
}