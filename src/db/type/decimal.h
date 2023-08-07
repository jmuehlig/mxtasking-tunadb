#pragma once
#include <cmath>
#include <cstdint>
#include <string>

namespace db::type {
class DecimalDescription
{
public:
    [[nodiscard]] constexpr static std::uint8_t max_precision() noexcept { return 31U; }

    [[nodiscard]] static std::uint8_t max_precision(const std::uint8_t precision,
                                                    const std::uint8_t precision_) noexcept
    {
        if (std::max(precision, precision_) > 31U)
        {
            return 63U;
        }

        return max_precision();
    }

    [[nodiscard]] static constexpr std::uint8_t max_scale() noexcept { return 8U; }

    [[nodiscard]] static constexpr std::uint8_t minimum_divide_scale() noexcept
    {
        return std::min(DecimalDescription::max_scale(), std::uint8_t(2U));
    }

    constexpr DecimalDescription(const std::uint8_t precision, const std::uint8_t scale) noexcept
        : _precision(precision), _scale(scale)
    {
    }

    ~DecimalDescription() noexcept = default;

    DecimalDescription &operator=(const DecimalDescription &) noexcept = default;

    [[nodiscard]] std::uint8_t precision() const noexcept { return _precision; }
    [[nodiscard]] std::uint8_t scale() const noexcept { return _scale; }

    [[nodiscard]] bool operator==(const DecimalDescription &other) const noexcept { return _scale == other._scale; }

private:
    /// Number of digits in the decimal, e.g., precision(100.00) = 5.
    std::uint8_t _precision;

    /// Number of digits right to the decimal point, e.g., scale(100.00) = 2.
    std::uint8_t _scale;
};

class Decimal
{
public:
    using value_t = std::int64_t;

    [[nodiscard]] static Decimal from_string(std::string &&decimal) noexcept
    {
        auto scale = std::uint8_t{0U};
        const auto pos = decimal.find('.');
        if (pos != std::string::npos)
        {
            scale = decimal.length() - (pos + 1U);
            decimal.replace(pos, 1U, "");
        }

        return Decimal{DecimalDescription{std::uint8_t(decimal.length()), scale}, std::stoll(decimal)};
    }

    constexpr Decimal(const DecimalDescription description, const value_t decimal) noexcept
        : _decimal_description(description), _value(decimal)
    {
    }

    constexpr Decimal() noexcept = default;

    ~Decimal() noexcept = default;

    Decimal &operator=(const Decimal &) noexcept = default;

    [[nodiscard]] DecimalDescription description() const noexcept { return _decimal_description; }
    [[nodiscard]] value_t data() const noexcept { return _value; }

    [[nodiscard]] Decimal cast(const DecimalDescription &to_description) const noexcept
    {
        if (to_description == _decimal_description)
        {
            return *this;
        }

        return Decimal{to_description, Decimal::cast(this->_value, this->_decimal_description, to_description)};
    }

    static value_t cast(value_t data, const DecimalDescription from_description,
                        const DecimalDescription to_description)
    {
        if (from_description == to_description)
        {
            return data;
        }

        if (to_description.scale() > from_description.scale())
        {
            const auto factor = value_t(std::pow(10U, to_description.scale() - from_description.scale()));
            return data * factor;
        }

        const auto divisor = value_t(std::pow(10U, from_description.scale() - to_description.scale()));
        return data / divisor;
    }

    static value_t multiply(const DecimalDescription description, const value_t left, const value_t right)
    {
        return (left * right) / std::pow(10U, description.scale());
    }

    static value_t divide(const DecimalDescription description, const value_t left, const value_t right)
    {
        return (left / right) * std::pow(10U, description.scale());
    }

    [[nodiscard]] std::string to_string() const noexcept
    {
        auto text = std::string{_value >= 0 ? "" : "-"};
        auto value_as_string = std::to_string(_value >= 0 ? _value : _value * -1);

        if (value_as_string.length() <= _decimal_description.scale())
        {
            text += std::string{"0."} + std::string(_decimal_description.scale() - value_as_string.length(), '0');
        }
        else if (_decimal_description.scale() > 0U)
        {
            value_as_string.insert(value_as_string.length() - _decimal_description.scale(), ".");
        }

        text += value_as_string;
        return text;
    }

    static std::string to_string(value_t data, DecimalDescription description) noexcept
    {
        auto text = std::string{data >= 0 ? "" : "-"};
        auto value_as_string = std::to_string(data >= 0 ? data : data * -1);

        if (value_as_string.length() <= description.scale())
        {
            text += std::string{"0."} + std::string(description.scale() - value_as_string.length(), '0');
        }
        else if (description.scale() > 0U)
        {
            value_as_string.insert(value_as_string.length() - description.scale(), ".");
        }

        text += value_as_string;
        return text;
    }

    bool operator==(const Decimal other) const noexcept
    {
        return _decimal_description == other._decimal_description && _value == other._value;
    }

    bool operator!=(const Decimal /*other*/) const noexcept { return false; }

    bool operator<(const Decimal /*other*/) const noexcept { return false; }

    bool operator<=(const Decimal /*other*/) const noexcept { return false; }

    bool operator>(const Decimal /*other*/) const noexcept { return false; }

    bool operator>=(const Decimal /*other*/) const noexcept { return false; }

private:
    DecimalDescription _decimal_description{0U, 0U};
    value_t _value{0U};
};
} // namespace db::type