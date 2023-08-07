#pragma once

#include <cstdint>
#include <db/type/bool.h>
#include <db/type/type.h>
#include <string>
#include <string_view>
#include <variant>

namespace db::data {
class Value
{
public:
    using value_t = std::variant<type::underlying<type::Id::INT>::value, type::underlying<type::Id::BIGINT>::value,
                                 type::underlying<type::Id::DATE>::value, type::underlying<type::Id::BOOL>::value,
                                 std::string, std::string_view>;

    constexpr Value() noexcept = default;
    Value(const type::Type type, const value_t &value) noexcept : _type(type), _value(value) {}

    Value(const type::Type type, value_t &&value) noexcept : _type(type), _value(std::move(value)) {}

    Value(const Value &) noexcept = default;
    Value(Value &&) noexcept = default;

    Value(const type::Type type) noexcept : _type(type), _value(Value::make_zero(_type.id())) {}

    ~Value() noexcept = default;

    Value &operator=(Value &&) noexcept = default;
    Value &operator=(value_t &&value) noexcept
    {
        _value = std::move(value);
        return *this;
    }
    Value &operator=(const Value &) noexcept = default;

    [[nodiscard]] const value_t &value() const noexcept { return _value; }

    [[nodiscard]] value_t &value() noexcept { return _value; }

    [[nodiscard]] const type::Type &type() const noexcept { return _type; }

    template <type::Id I> [[nodiscard]] typename type::underlying<I>::value get() const noexcept
    {
        return std::get<typename type::underlying<I>::value>(_value);
    }

    [[nodiscard]] std::string to_string() const noexcept;

    [[nodiscard]] Value &as(type::Type to_type);
    [[nodiscard]] bool is_lossless_convertible(type::Type type) const noexcept;

    bool operator==(const Value &other) const noexcept
    {
        if (other._type != _type)
        {
            return false;
        }

        if (_type == type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value))
            {
                if (std::holds_alternative<std::string_view>(other._value))
                {
                    return std::get<std::string>(_value) == std::get<std::string_view>(other._value);
                }
            }

            if (std::holds_alternative<std::string_view>(_value))
            {
                if (std::holds_alternative<std::string>(other._value))
                {
                    return std::get<std::string_view>(_value) == std::get<std::string>(other._value);
                }
            }
        }

        return _value == other._value;
    }

    bool operator!=(const Value &other) const noexcept
    {
        if (other._type != _type)
        {
            return true;
        }

        if (_type == type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value))
            {
                if (std::holds_alternative<std::string_view>(other._value))
                {
                    return std::get<std::string>(_value) != std::get<std::string_view>(other._value);
                }
            }

            if (std::holds_alternative<std::string_view>(_value))
            {
                if (std::holds_alternative<std::string>(other._value))
                {
                    return std::get<std::string_view>(_value) != std::get<std::string>(other._value);
                }
            }
        }

        return _value != other._value;
    }

    bool operator<=(const Value &other) const noexcept
    {
        if (other._type != _type)
        {
            return false;
        }

        if (_type == type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value))
            {
                if (std::holds_alternative<std::string_view>(other._value))
                {
                    return std::get<std::string>(_value) <= std::get<std::string_view>(other._value);
                }
            }

            if (std::holds_alternative<std::string_view>(_value))
            {
                if (std::holds_alternative<std::string>(other._value))
                {
                    return std::get<std::string_view>(_value) <= std::get<std::string>(other._value);
                }
            }
        }

        return _value <= other._value;
    }

    bool operator<(const Value &other) const noexcept
    {
        if (other._type != _type)
        {
            return false;
        }

        if (_type == type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value))
            {
                if (std::holds_alternative<std::string_view>(other._value))
                {
                    return std::get<std::string>(_value) < std::get<std::string_view>(other._value);
                }
            }

            if (std::holds_alternative<std::string_view>(_value))
            {
                if (std::holds_alternative<std::string>(other._value))
                {
                    return std::get<std::string_view>(_value) < std::get<std::string>(other._value);
                }
            }
        }

        return _value < other._value;
    }

    bool operator>=(const Value &other) const noexcept
    {
        if (other._type != _type)
        {
            return false;
        }

        if (_type == type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value))
            {
                if (std::holds_alternative<std::string_view>(other._value))
                {
                    return std::get<std::string>(_value) >= std::get<std::string_view>(other._value);
                }
            }

            if (std::holds_alternative<std::string_view>(_value))
            {
                if (std::holds_alternative<std::string>(other._value))
                {
                    return std::get<std::string_view>(_value) >= std::get<std::string>(other._value);
                }
            }
        }

        return _value >= other._value;
    }

    bool operator>(const Value &other) const noexcept
    {
        if (other._type != _type)
        {
            return false;
        }

        if (_type == type::Id::CHAR)
        {
            if (std::holds_alternative<std::string>(_value))
            {
                if (std::holds_alternative<std::string_view>(other._value))
                {
                    return std::get<std::string>(_value) > std::get<std::string_view>(other._value);
                }
            }

            if (std::holds_alternative<std::string_view>(_value))
            {
                if (std::holds_alternative<std::string>(other._value))
                {
                    return std::get<std::string_view>(_value) > std::get<std::string>(other._value);
                }
            }
        }

        return _value > other._value;
    }

    Value operator+(const Value &other) const;
    Value &operator+=(const Value &other);
    Value operator-(const Value &other) const;
    Value operator*(const Value &other) const;
    Value operator/(const Value &other) const;

    [[nodiscard]] static value_t make_zero(type::Id type_id);
    //    [[nodiscard]] static value_t add(type::Id type_id, value_t left, value_t right);
    //    [[nodiscard]] static value_t sub(type::Id type_id, value_t left, value_t right);
    //    [[nodiscard]] static value_t mul(type::Id type_id, value_t left, value_t right);
    //    [[nodiscard]] static value_t div(type::Id type_id, value_t left, value_t right);

private:
    type::Type _type;
    value_t _value;

    template <type::Id F, type::Id T> [[nodiscard]] static typename type::underlying<T>::value as(const value_t &v)
    {
        return typename type::underlying<T>::value(std::get<typename type::underlying<F>::value>(v));
    }
};
} // namespace db::data

namespace std {
template <> struct hash<db::data::Value>
{
public:
    std::size_t operator()(const db::data::Value &value) const
    {
        std::size_t h;
        std::visit(
            [&h](const auto &v) {
                using T = std::decay_t<decltype(v)>;
                h = std::hash<T>()(v);
            },
            value.value());
        return h;
    }
};
} // namespace std