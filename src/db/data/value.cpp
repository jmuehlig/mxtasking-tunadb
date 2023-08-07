#include "value.h"
#include <db/exception/execution_exception.h>

using namespace db::data;

Value &Value::as(const type::Type to_type)
{
    if (this->_type == to_type)
    {
        return *this;
    }

    if (this->_type == type::Id::INT)
    {
        if (to_type == type::Id::BIGINT)
        {
            this->_value = Value::as<type::Id::INT, type::Id::BIGINT>(this->_value);
        }
        else if (to_type == type::Id::DECIMAL)
        {
            this->_value =
                Value::as<type::Id::INT, type::Id::DECIMAL>(this->_value) *
                type::underlying<type::Id::DECIMAL>::value(std::pow(10U, to_type.decimal_description().scale()));
        }
        else if (to_type == type::Id::DATE)
        {
            throw exception::CastException(this->_type.to_string(), to_type.to_string());
        }
        else if (to_type == type::Id::BOOL)
        {
            this->_value = Value::as<type::Id::INT, type::Id::BOOL>(this->_value);
        }
        else if (to_type == type::Id::CHAR)
        {
            auto as_string = std::to_string(std::get<type::underlying<type::Id::INT>::value>(this->_value));
            this->_value =
                as_string.substr(0, std::min(std::size_t(to_type.char_description().length()), as_string.length()));
        }
    }

    else if (this->_type == type::Id::BIGINT)
    {
        if (to_type == type::Id::INT)
        {
            this->_value = Value::as<type::Id::BIGINT, type::Id::INT>(this->_value);
        }
        else if (to_type == type::Id::DECIMAL)
        {
            this->_value =
                Value::as<type::Id::BIGINT, type::Id::DECIMAL>(this->_value) *
                type::underlying<type::Id::DECIMAL>::value(std::pow(10U, to_type.decimal_description().scale()));
        }
        else if (to_type == type::Id::DATE)
        {
            throw exception::CastException(this->_type.to_string(), to_type.to_string());
        }
        else if (to_type == type::Id::BOOL)
        {
            this->_value = Value::as<type::Id::BIGINT, type::Id::BOOL>(this->_value);
        }
        else if (to_type == type::Id::CHAR)
        {
            auto as_string = std::to_string(std::get<type::underlying<type::Id::BIGINT>::value>(this->_value));
            this->_value =
                as_string.substr(0, std::min(std::size_t(to_type.char_description().length()), as_string.length()));
        }
    }

    else if (this->_type == type::Id::DECIMAL)
    {
        if (to_type == type::Id::INT)
        {
            this->_value = type::underlying<type::Id::INT>::value(
                type::Decimal::cast(std::get<type::underlying<type::Id::DECIMAL>::value>(this->_value),
                                    this->_type.decimal_description(), type::DecimalDescription{128U, 0U}));
        }
        else if (to_type == type::Id::BIGINT)
        {
            this->_value = type::underlying<type::Id::BIGINT>::value(
                type::Decimal::cast(std::get<type::underlying<type::Id::DECIMAL>::value>(this->_value),
                                    this->_type.decimal_description(), type::DecimalDescription{128U, 0U}));
        }
        else if (to_type == type::Id::DECIMAL)
        {
            this->_value = type::Decimal::cast(std::get<type::underlying<type::Id::DECIMAL>::value>(this->_value),
                                               this->_type.decimal_description(), to_type.decimal_description());
        }
        else if (to_type == type::Id::DATE)
        {
            throw exception::CastException(this->_type.to_string(), to_type.to_string());
        }
        else if (to_type == type::Id::BOOL)
        {
            this->_value = Value::as<type::Id::DECIMAL, type::Id::BOOL>(this->_value);
        }
        else if (to_type == type::Id::CHAR)
        {
            auto as_string = type::Decimal::to_string(
                std::get<type::underlying<type::Id::DECIMAL>::value>(this->_value), this->_type.decimal_description());
            this->_value =
                as_string.substr(0, std::min(std::size_t(to_type.char_description().length()), as_string.length()));
        }
    }

    if (this->_type == type::Id::DATE)
    {
        if (to_type == type::Id::INT)
        {
            throw exception::CastException(this->_type.to_string(), to_type.to_string());
        }

        if (to_type == type::Id::BIGINT)
        {
            throw exception::CastException(this->_type.to_string(), to_type.to_string());
        }

        if (to_type == type::Id::DECIMAL)
        {
            throw exception::CastException(this->_type.to_string(), to_type.to_string());
        }

        if (to_type == type::Id::BOOL)
        {
            throw exception::CastException(this->_type.to_string(), to_type.to_string());
        }

        if (to_type == type::Id::CHAR)
        {
            auto as_string = std::get<type::underlying<type::Id::DATE>::value>(this->_value).to_string();
            this->_value =
                as_string.substr(0, std::min(std::size_t(to_type.char_description().length()), as_string.length()));
        }
    }

    else if (this->_type == type::Id::BOOL)
    {
        if (to_type == type::Id::INT)
        {
            this->_value = Value::as<type::Id::BOOL, type::Id::INT>(this->_value);
        }
        else if (to_type == type::Id::BIGINT)
        {
            this->_value = Value::as<type::Id::BOOL, type::Id::BIGINT>(this->_value);
        }
        else if (to_type == type::Id::DECIMAL)
        {
            this->_value = type::Decimal::cast(Value::as<type::Id::BOOL, type::Id::DECIMAL>(this->_value),
                                               type::DecimalDescription{1U, 0U}, to_type.decimal_description());
        }
        else if (to_type == type::Id::DATE)
        {
            throw exception::CastException(this->_type.to_string(), to_type.to_string());
        }
        else if (to_type == type::Id::CHAR)
        {
            auto as_string = type::Bool::to_string(std::get<type::underlying<type::Id::BOOL>::value>(this->_value));
            this->_value =
                as_string.substr(0, std::min(std::size_t(to_type.char_description().length()), as_string.length()));
        }
    }

    else if (this->_type == type::Id::CHAR)
    {
        if (to_type == type::Id::INT)
        {
            if (std::holds_alternative<std::string>(this->_value))
            {
                this->_value = type::underlying<type::Id::INT>::value(std::stoll(std::get<std::string>(this->_value)));
            }
            else if (std::holds_alternative<std::string_view>(this->_value))
            {
                // TODO!
            }
        }
        else if (to_type == type::Id::BIGINT)
        {
            if (std::holds_alternative<std::string>(this->_value))
            {
                this->_value =
                    type::underlying<type::Id::BIGINT>::value(std::stoll(std::get<std::string>(this->_value)));
            }
            else if (std::holds_alternative<std::string_view>(this->_value))
            {
                // TODO!
            }
        }
        else if (to_type == type::Id::DECIMAL)
        {
            if (std::holds_alternative<std::string>(this->_value))
            {
                this->_value = type::Decimal::from_string(std::move(std::get<std::string>(this->_value)))
                                   .cast(to_type.decimal_description())
                                   .data();
            }
            else if (std::holds_alternative<std::string_view>(this->_value))
            {
                // TODO!
            }
        }
        else if (to_type == type::Id::DATE)
        {
            if (std::holds_alternative<std::string>(this->_value))
            {
                this->_value = type::Date::from_string(std::move(std::get<std::string>(this->_value)));
            }
            else if (std::holds_alternative<std::string_view>(this->_value))
            {
                // TODO
            }
        }
        else if (to_type == type::Id::BOOL)
        {
            if (std::holds_alternative<std::string>(this->_value))
            {
                this->_value = type::Bool::from_string(std::get<std::string>(this->_value));
            }
            else if (std::holds_alternative<std::string_view>(this->_value))
            {
                this->_value = type::Bool::from_string(std::get<std::string_view>(this->_value));
            }
        }
        else if (to_type == type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(this->_value))
            {
                auto &as_string = std::get<std::string>(this->_value);
                if (as_string.length() > to_type.char_description().length())
                {
                    this->_value = as_string.substr(0, to_type.char_description().length());
                }
                else if (as_string.length() < to_type.char_description().length())
                {
                    auto new_value = std::string(to_type.char_description().length(), '\0');
                    std::memcpy(new_value.data(), as_string.data(), as_string.length());
                    this->_value = std::move(new_value);
                    this->_type = to_type;
                }
            }
            else if (std::holds_alternative<std::string_view>(this->_value))
            {
                // TODO
            }
        }
    }

    this->_type = to_type;
    return *this;
}

bool Value::is_lossless_convertible(const type::Type type) const noexcept
{
    if (this->_type == type)
    {
        return true;
    }

    if (this->_type == type::Id::INT)
    {
        if (type == type::Id::BIGINT || type == type::Id::CHAR)
        {
            return true;
        }

        if (type == type::Id::DECIMAL)
        {
            const auto multiplier = std::pow(10U, this->_type.decimal_description().scale());
            const auto decimal_value = this->get<type::Id::INT>();
            return decimal_value ==
                   (type::underlying<type::Id::INT>::value((decimal_value / double(multiplier)) * multiplier));
        }
    }

    if (this->_type == type::Id::BIGINT)
    {
        if (type == type::Id::INT)
        {
            return this->get<type::Id::BIGINT>() <= std::numeric_limits<type::underlying<type::Id::INT>::value>::max();
        }

        if (type == type::Id::CHAR)
        {
            return true;
        }

        if (type == type::Id::DECIMAL)
        {
            const auto multiplier = std::pow(10U, type.decimal_description().scale());
            const auto decimal_value = this->get<type::Id::BIGINT>();
            return decimal_value ==
                   (type::underlying<type::Id::BIGINT>::value((decimal_value / double(multiplier)) * multiplier));
        }
    }

    return false;
}

Value Value::operator+(const Value &other) const
{
    const auto type = this->_type + other._type;
    if (_type == type::Id::INT)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, this->get<type::Id::INT>() + other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, other.get<type::Id::BIGINT>() + this->get<type::Id::INT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            const auto as_decimal = type::underlying<type::Id::DECIMAL>::value(
                this->get<type::Id::INT>() * std::pow(10U, other.type().decimal_description().scale()));
            return Value{type, as_decimal + other.get<type::Id::DECIMAL>()};
        }
        if (other._type == type::Id::BOOL)
        {
            return Value{type, this->get<type::Id::INT>() +
                                   static_cast<type::underlying<type::Id::INT>::value>(other.get<type::Id::BOOL>())};
        }
    }

    if (_type == type::Id::BIGINT)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, this->get<type::Id::BIGINT>() + other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, this->get<type::Id::BIGINT>() + other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            const auto as_decimal = type::underlying<type::Id::DECIMAL>::value(
                this->get<type::Id::BIGINT>() * std::pow(10U, other.type().decimal_description().scale()));
            return Value{type, as_decimal + other.get<type::Id::DECIMAL>()};
        }
        if (other._type == type::Id::BOOL)
        {
            return Value{type, this->get<type::Id::BIGINT>() +
                                   static_cast<type::underlying<type::Id::BIGINT>::value>(other.get<type::Id::BOOL>())};
        }
    }

    if (_type == type::Id::DECIMAL)
    {
        if (other._type == type::Id::INT)
        {
            const auto other_as_decimal = type::underlying<type::Id::DECIMAL>::value(
                other.get<type::Id::INT>() * std::pow(10U, _type.decimal_description().scale()));
            return Value{type, this->get<type::Id::DECIMAL>() + other_as_decimal};
        }
        if (other._type == type::Id::BIGINT)
        {
            const auto other_as_decimal = type::underlying<type::Id::DECIMAL>::value(
                other.get<type::Id::BIGINT>() * std::pow(10U, _type.decimal_description().scale()));
            return Value{type, this->get<type::Id::DECIMAL>() + other_as_decimal};
        }
        if (other._type == type::Id::DECIMAL)
        {
            return Value{type, this->get<type::Id::DECIMAL>() + other.get<type::Id::DECIMAL>()};
        }
        if (other._type == type::Id::BOOL)
        {
            const auto other_as_decimal = type::underlying<type::Id::DECIMAL>::value(
                static_cast<type::underlying<type::Id::DECIMAL>::value>(other.get<type::Id::BOOL>()) *
                std::pow(10U, _type.decimal_description().scale()));
            return Value{type, this->get<type::Id::DECIMAL>() + other_as_decimal};
        }
    }

    if (_type == type::Id::DATE)
    {
        if (other._type == type::Id::DATE)
        {
            return Value{type, this->get<type::Id::DATE>() + other.get<type::Id::DATE>()};
        }
    }

    if (_type == type::Id::BOOL)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, static_cast<type::underlying<type::Id::INT>::value>(this->get<type::Id::BOOL>()) +
                                   other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, static_cast<type::underlying<type::Id::BIGINT>::value>(this->get<type::Id::BOOL>()) +
                                   other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            const auto as_decimal = type::underlying<type::Id::DECIMAL>::value(
                static_cast<type::underlying<type::Id::DECIMAL>::value>(this->get<type::Id::BOOL>()) *
                std::pow(10U, other.type().decimal_description().scale()));
            return Value{type, as_decimal + other.get<type::Id::DECIMAL>()};
        }
    }

    throw exception::OperationNotAllowedException{"+", _type.to_string(), other._type.to_string()};
}

Value &Value::operator+=(const Value &other)
{
    if (_type == type::Id::INT)
    {
        auto &value = std::get<type::underlying<type::Id::INT>::value>(this->_value);
        if (other._type == type::Id::INT)
        {
            value += other.get<type::Id::INT>();
            return *this;
        }
        if (other._type == type::Id::BIGINT)
        {
            value += other.get<type::Id::BIGINT>();
            return *this;
        }
        if (other._type == type::Id::DECIMAL)
        {
            value += other.get<type::Id::DECIMAL>() / std::pow(10U, other.type().decimal_description().scale());
            return *this;
        }
        if (other._type == type::Id::BOOL)
        {
            value += static_cast<type::underlying<type::Id::INT>::value>(other.get<type::Id::BOOL>());
            return *this;
        }
    }

    if (_type == type::Id::BIGINT)
    {
        auto &value = std::get<type::underlying<type::Id::BIGINT>::value>(this->_value);
        if (other._type == type::Id::INT)
        {
            value += other.get<type::Id::INT>();
            return *this;
        }
        if (other._type == type::Id::BIGINT)
        {
            value += other.get<type::Id::BIGINT>();
            return *this;
        }
        if (other._type == type::Id::DECIMAL)
        {
            value += other.get<type::Id::DECIMAL>() / std::pow(10U, other.type().decimal_description().scale());
            return *this;
        }
        if (other._type == type::Id::BOOL)
        {
            value += static_cast<type::underlying<type::Id::BIGINT>::value>(other.get<type::Id::BOOL>());
            return *this;
        }
    }

    if (_type == type::Id::DECIMAL)
    {
        auto &value = std::get<type::underlying<type::Id::DECIMAL>::value>(this->_value);
        if (other._type == type::Id::INT)
        {
            value += other.get<type::Id::INT>() * std::pow(10U, _type.decimal_description().scale());
            return *this;
        }
        if (other._type == type::Id::BIGINT)
        {
            value += other.get<type::Id::BIGINT>() * std::pow(10U, _type.decimal_description().scale());
            return *this;
        }
        if (other._type == type::Id::DECIMAL)
        {
            auto other_decimal = other.get<type::Id::DECIMAL>();
            if (other._type.decimal_description().scale() < this->_type.decimal_description().scale())
            {
                other_decimal *= std::pow(10U, this->_type.decimal_description().scale() -
                                                   other._type.decimal_description().scale());
            }
            else if (other._type.decimal_description().scale() > this->_type.decimal_description().scale())
            {
                other_decimal /= std::pow(10U, other._type.decimal_description().scale() -
                                                   this->_type.decimal_description().scale());
            }

            value += other_decimal;
            return *this;
        }
        if (other._type == type::Id::BOOL)
        {
            value += static_cast<type::underlying<type::Id::DECIMAL>::value>(other.get<type::Id::BOOL>()) *
                     std::pow(10U, _type.decimal_description().scale());
            ;
            return *this;
        }
    }

    if (_type == type::Id::DATE)
    {
        if (other._type == type::Id::DATE)
        {
            std::get<type::underlying<type::Id::DATE>::value>(this->_value) += other.get<type::Id::DATE>();
            return *this;
        }
    }

    if (_type == type::Id::BOOL)
    {
    }

    throw exception::OperationNotAllowedException{"+=", _type.to_string(), other._type.to_string()};
}

Value Value::operator-(const Value &other) const
{
    const auto type = this->_type - other._type;
    if (_type == type::Id::INT)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, this->get<type::Id::INT>() - other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, type::underlying<type::Id::BIGINT>::value(this->get<type::Id::INT>()) -
                                   other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            const auto as_decimal = type::underlying<type::Id::DECIMAL>::value(
                this->get<type::Id::INT>() * std::pow(10U, other.type().decimal_description().scale()));
            return Value{type, as_decimal - other.get<type::Id::DECIMAL>()};
        }
        if (other._type == type::Id::BOOL)
        {
            return Value{type, this->get<type::Id::INT>() -
                                   static_cast<type::underlying<type::Id::INT>::value>(other.get<type::Id::BOOL>())};
        }
    }

    if (_type == type::Id::BIGINT)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, this->get<type::Id::BIGINT>() - other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, this->get<type::Id::BIGINT>() - other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            const auto as_decimal = type::underlying<type::Id::DECIMAL>::value(
                this->get<type::Id::BIGINT>() * std::pow(10U, other.type().decimal_description().scale()));
            return Value{type, as_decimal - other.get<type::Id::DECIMAL>()};
        }
        if (other._type == type::Id::BOOL)
        {
            return Value{type, this->get<type::Id::BIGINT>() -
                                   static_cast<type::underlying<type::Id::BIGINT>::value>(other.get<type::Id::BOOL>())};
        }
    }

    if (_type == type::Id::DECIMAL)
    {
        if (other._type == type::Id::INT)
        {
            const auto other_as_decimal = type::underlying<type::Id::DECIMAL>::value(
                other.get<type::Id::INT>() * std::pow(10U, _type.decimal_description().scale()));
            return Value{type, this->get<type::Id::DECIMAL>() - other_as_decimal};
        }
        if (other._type == type::Id::BIGINT)
        {
            const auto other_as_decimal = type::underlying<type::Id::DECIMAL>::value(
                other.get<type::Id::BIGINT>() * std::pow(10U, _type.decimal_description().scale()));
            return Value{type, this->get<type::Id::DECIMAL>() - other_as_decimal};
        }
        if (other._type == type::Id::DECIMAL)
        {
            return Value{type, this->get<type::Id::DECIMAL>() - other.get<type::Id::DECIMAL>()};
        }
        if (other._type == type::Id::BOOL)
        {
            const auto other_as_decimal = type::underlying<type::Id::DECIMAL>::value(
                static_cast<type::underlying<type::Id::DECIMAL>::value>(other.get<type::Id::BOOL>()) *
                std::pow(10U, _type.decimal_description().scale()));
            return Value{type, this->get<type::Id::DECIMAL>() - other_as_decimal};
        }
    }

    if (_type == type::Id::DATE)
    {
        if (other._type == type::Id::DATE)
        {
            return Value{type, this->get<type::Id::DATE>() - other.get<type::Id::DATE>()};
        }
    }

    if (_type == type::Id::BOOL)
    {
        if (other._type == type::Id::INT)
        {
            const auto as_int = type::underlying<type::Id::INT>::value(this->get<type::Id::BOOL>());
            return Value{type, as_int - other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            const auto as_bigint = type::underlying<type::Id::INT>::value(this->get<type::Id::BOOL>());
            return Value{type, as_bigint - other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            const auto as_decimal = type::underlying<type::Id::DECIMAL>::value(
                static_cast<type::underlying<type::Id::DECIMAL>::value>(this->get<type::Id::BOOL>()) *
                std::pow(10U, other.type().decimal_description().scale()));
            return Value{type, as_decimal - other.get<type::Id::DECIMAL>()};
        }
    }

    throw exception::OperationNotAllowedException{"-", _type.to_string(), other._type.to_string()};
}

Value Value::operator*(const Value &other) const
{
    const auto type = this->_type * other._type;
    if (_type == type::Id::INT)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, this->get<type::Id::INT>() * other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, type::underlying<type::Id::BIGINT>::value(this->get<type::Id::INT>()) *
                                   other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            return Value{type, this->get<type::Id::INT>() * other.get<type::Id::DECIMAL>()};
        }
        if (other._type == type::Id::BOOL)
        {
            return Value{type, this->get<type::Id::INT>() *
                                   static_cast<type::underlying<type::Id::INT>::value>(other.get<type::Id::BOOL>())};
        }
    }

    if (_type == type::Id::BIGINT)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, this->get<type::Id::BIGINT>() * other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, this->get<type::Id::BIGINT>() * other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            return Value{type, this->get<type::Id::BIGINT>() * other.get<type::Id::DECIMAL>()};
        }
        if (other._type == type::Id::BOOL)
        {
            return Value{type, this->get<type::Id::BIGINT>() *
                                   static_cast<type::underlying<type::Id::BIGINT>::value>(other.get<type::Id::BOOL>())};
        }
    }

    if (_type == type::Id::DECIMAL)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, this->get<type::Id::DECIMAL>() * other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, this->get<type::Id::DECIMAL>() * other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            auto result = this->get<type::Id::DECIMAL>() * other.get<type::Id::DECIMAL>();
            const auto scale_factor = type::Type::decimal_conversion_factor_for_mul(this->_type.decimal_description(),
                                                                                    other._type.decimal_description());
            if (scale_factor > 0)
            {
                result /= scale_factor;
            }
            else if (scale_factor < 0) [[unlikely]]
            {
                result *= std::abs(scale_factor);
            }

            return Value{type, result};
        }
        if (other._type == type::Id::BOOL)
        {
            return Value{type, this->get<type::Id::DECIMAL>() * static_cast<type::underlying<type::Id::DECIMAL>::value>(
                                                                    other.get<type::Id::BOOL>())};
        }
    }

    if (_type == type::Id::BOOL)
    {
        if (other._type == type::Id::INT)
        {
            const auto as_int = type::underlying<type::Id::INT>::value(this->get<type::Id::BOOL>());
            return Value{type, as_int * other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            const auto as_bigint = type::underlying<type::Id::INT>::value(this->get<type::Id::BOOL>());
            return Value{type, as_bigint * other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            return Value{type, static_cast<type::underlying<type::Id::DECIMAL>::value>(this->get<type::Id::BOOL>()) *
                                   other.get<type::Id::DECIMAL>()};
        }
    }

    throw exception::OperationNotAllowedException{"*", _type.to_string(), other._type.to_string()};
}

Value Value::operator/(const Value &other) const
{
    const auto type = this->_type / other._type;
    if (_type == type::Id::INT)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, this->get<type::Id::INT>() / other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, type::underlying<type::Id::BIGINT>::value(this->get<type::Id::INT>()) /
                                   other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            const auto scale_factor =
                type::underlying<type::Id::DECIMAL>::value(std::pow(10U, other.type().decimal_description().scale()));
            const auto as_decimal =
                type::underlying<type::Id::DECIMAL>::value(this->get<type::Id::INT>() * scale_factor);
            return Value{type, (as_decimal * scale_factor / other.get<type::Id::DECIMAL>())};
        }
    }

    if (_type == type::Id::BIGINT)
    {
        if (other._type == type::Id::INT)
        {
            return Value{type, this->get<type::Id::BIGINT>() / other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            return Value{type, this->get<type::Id::BIGINT>() / other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            const auto scale_factor =
                type::underlying<type::Id::DECIMAL>::value(std::pow(10U, other.type().decimal_description().scale()));
            const auto as_decimal =
                type::underlying<type::Id::DECIMAL>::value(this->get<type::Id::BIGINT>() * scale_factor);
            return Value{type, (as_decimal * scale_factor / other.get<type::Id::DECIMAL>())};
        }
    }

    if (_type == type::Id::DECIMAL)
    {
        if (other._type == type::Id::INT)
        {
            //            const auto as_decimal = type::underlying<type::DECIMAL>::value(other.get<type::Id::INT>() *
            //            std::pow(10U, this->_type.decimal_description().scale()));
            return Value{type, this->get<type::Id::DECIMAL>() / other.get<type::Id::INT>()};
        }
        if (other._type == type::Id::BIGINT)
        {
            //            const auto as_decimal = type::underlying<type::DECIMAL>::value(other.get<type::Id::BIGINT>() *
            //            std::pow(10U, this->_type.decimal_description().scale()));
            return Value{type, this->get<type::Id::DECIMAL>() / other.get<type::Id::BIGINT>()};
        }
        if (other._type == type::Id::DECIMAL)
        {
            const auto scale_factor = type::Type::decimal_conversion_factor_for_div(this->_type.decimal_description(),
                                                                                    other._type.decimal_description());

            auto result = this->get<type::Id::DECIMAL>() / other.get<type::Id::DECIMAL>();
            if (scale_factor < 0) [[unlikely]]
            {
                result = (this->get<type::Id::DECIMAL>() * std::abs(scale_factor)) / other.get<type::Id::DECIMAL>();
            }
            else if (scale_factor > 0)
            {
                result /= scale_factor;
            }

            return Value{type, type::underlying<type::Id::DECIMAL>::value(result)};
        }
    }

    throw exception::OperationNotAllowedException{"/", _type.to_string(), other._type.to_string()};
}

std::string Value::to_string() const noexcept
{
    if (this->_type == type::Id::INT)
    {
        return std::to_string(std::get<type::underlying<type::Id::INT>::value>(this->_value));
    }

    if (this->_type == type::Id::BIGINT)
    {
        return std::to_string(std::get<type::underlying<type::Id::BIGINT>::value>(this->_value));
    }

    if (this->_type == type::Id::DECIMAL)
    {
        const auto decimal = type::Decimal{this->_type.decimal_description(),
                                           std::get<type::underlying<type::Id::DECIMAL>::value>(this->_value)};
        return decimal.to_string();
    }

    if (this->_type == type::Id::DATE)
    {
        return std::get<type::underlying<type::Id::DATE>::value>(this->_value).to_string();
    }

    if (this->_type == type::Id::BOOL)
    {
        const auto value = std::get<type::underlying<type::Id::BOOL>::value>(this->_value);
        return value ? "True" : "False";
    }

    if (this->_type == type::Id::CHAR)
    {
        if (std::holds_alternative<std::string>(this->_value))
        {
            return std::get<std::string>(this->_value).substr(0, this->_type.char_description().length());
        }

        if (std::holds_alternative<std::string_view>(this->_value))
        {
            return std::string{std::get<std::string_view>(this->_value).data()}.substr(
                0, this->_type.char_description().length());
        }
    }

    return "";
}

// db::data::Value::value_t Value::add(const type::Id type_id, value_t left, const value_t right)
//{
//    switch (type_id)
//    {
//    case type::Id::INT:
//        return std::get<type::underlying<type::Id::INT>::value>(left) +=
//               std::get<type::underlying<type::Id::INT>::value>(right);
//    case type::Id::BIGINT:
//        return std::get<type::underlying<type::Id::BIGINT>::value>(left) +=
//               std::get<type::underlying<type::Id::BIGINT>::value>(right);
//    case type::Id::DECIMAL:
//        return std::get<type::underlying<type::Id::DECIMAL>::value>(left) +=
//               std::get<type::underlying<type::Id::DECIMAL>::value>(right);
//    case type::Id::DATE:
//        return std::get<type::underlying<type::Id::DATE>::value>(left) +=
//               std::get<type::underlying<type::Id::DATE>::value>(right);
//    case type::Id::BOOL:
//    case type::Id::CHAR:
//    case type::Id::UNKNOWN:
//        throw exception::OperationNotAllowedException{"add"};
//    }
//}
//
// db::data::Value::value_t Value::sub(const type::Id type_id, value_t left, const value_t right)
//{
//    switch (type_id)
//    {
//    case type::Id::INT:
//        return std::get<type::underlying<type::Id::INT>::value>(left) -=
//               std::get<type::underlying<type::Id::INT>::value>(right);
//    case type::Id::BIGINT:
//        return std::get<type::underlying<type::Id::BIGINT>::value>(left) -=
//               std::get<type::underlying<type::Id::BIGINT>::value>(right);
//    case type::Id::DECIMAL:
//        return std::get<type::underlying<type::Id::DECIMAL>::value>(left) -=
//               std::get<type::underlying<type::Id::DECIMAL>::value>(right);
//    case type::Id::DATE:
//    case type::Id::BOOL:
//    case type::Id::CHAR:
//    case type::Id::UNKNOWN:
//        throw exception::OperationNotAllowedException{"sub"};
//    }
//}
//
// db::data::Value::value_t Value::mul(const type::Id type_id, value_t left, const value_t right)
//{
//    switch (type_id)
//    {
//    case type::Id::INT:
//        return std::get<type::underlying<type::Id::INT>::value>(left) *=
//               std::get<type::underlying<type::Id::INT>::value>(right);
//    case type::Id::BIGINT:
//        return std::get<type::underlying<type::Id::BIGINT>::value>(left) *=
//               std::get<type::underlying<type::Id::BIGINT>::value>(right);
//    case type::Id::DECIMAL:
//        return std::get<type::underlying<type::Id::DECIMAL>::value>(left) *=
//               std::get<type::underlying<type::Id::DECIMAL>::value>(right);
//    case type::Id::DATE:
//    case type::Id::BOOL:
//    case type::Id::CHAR:
//    case type::Id::UNKNOWN:
//        throw exception::OperationNotAllowedException{"mul"};
//    }
//}
//
// db::data::Value::value_t Value::div(const type::Id type_id, value_t left, const value_t right)
//{
//    switch (type_id)
//    {
//    case type::Id::INT:
//        return std::get<type::underlying<type::Id::INT>::value>(left) /=
//               std::get<type::underlying<type::Id::INT>::value>(right);
//    case type::Id::BIGINT:
//        return std::get<type::underlying<type::Id::BIGINT>::value>(left) /=
//               std::get<type::underlying<type::Id::BIGINT>::value>(right);
//    case type::Id::DECIMAL:
//        // return std::get<type::underlying<type::Id::DECIMAL>::value>(left) /=
//        // std::get<type::underlying<type::Id::DECIMAL>::value>(right);
//    case type::Id::DATE:
//    case type::Id::BOOL:
//    case type::Id::CHAR:
//    case type::Id::UNKNOWN:
//        throw exception::OperationNotAllowedException{"div"};
//    }
//}

db::data::Value::value_t Value::make_zero(const type::Id type_id)
{
    switch (type_id)
    {
    case type::Id::INT:
        return type::underlying<type::Id::INT>::value(0);
    case type::Id::BIGINT:
        return type::underlying<type::Id::BIGINT>::value(0);
    case type::Id::DECIMAL:
        return type::underlying<type::Id::DECIMAL>::value(0);
    case type::Id::DATE:
        return type::underlying<type::Id::DATE>::value{0U, 0U, 0U};
    case type::Id::BOOL:
        return type::underlying<type::Id::BOOL>::value(false);
    case type::Id::CHAR:
        return std::string{"0"};
    default:
        throw exception::OperationNotAllowedException{"zero", "unknown"};
    }
}