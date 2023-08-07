#pragma once
#include <chrono>
#include <cstdint>
#include <fmt/core.h>
#include <string>

namespace db::type {
class Date
{
public:
    using data_t = std::uint32_t;

    static Date from_string(std::string &&date) noexcept { return Date::from_string(date); }

    static Date from_string(const std::string &date) noexcept
    {
        const auto year = std::uint16_t(std::stoul(date.substr(0, 4)));
        const auto month = std::uint8_t(std::stoul(date.substr(5, 2)));
        const auto day = std::uint8_t(std::stoul(date.substr(8, 2)));

        return Date{year, month, day};
    }

    static Date make_interval_from_years(const std::uint16_t years)
    {
        return Date{INTERVAL_FLAG | INTERVAL_YEARS_FLAG | data_t(years)};
    }

    static Date make_interval_from_months(const std::uint16_t months)
    {
        return Date{INTERVAL_FLAG | INTERVAL_MONTHS_FLAG | data_t(months)};
    }

    static Date make_interval_from_days(std::uint16_t days)
    {
        return Date{INTERVAL_FLAG | INTERVAL_DAYS_FLAG | data_t(days)};
    }

    constexpr Date() noexcept = default;

    constexpr Date(const data_t data) noexcept : _data(data) {}

    Date(const std::uint16_t year, const std::uint8_t month, const std::uint8_t day) noexcept
        : _data(year * 10000U + month * 100U + day)
    {
    }

    ~Date() noexcept = default;

    Date &operator=(const Date &) noexcept = default;

    [[nodiscard]] std::uint16_t year() const noexcept { return std::uint16_t(_data / 10000U); }
    [[nodiscard]] std::uint8_t month() const noexcept { return std::uint8_t(std::uint32_t(_data / 100U) % 100U); }
    [[nodiscard]] std::uint8_t day() const noexcept { return std::uint8_t(_data % 100U); }

    bool operator==(const Date other) const noexcept { return _data == other._data; }

    bool operator!=(const Date other) const noexcept { return _data != other._data; }

    bool operator<(const Date other) const noexcept { return _data < other._data; }

    bool operator<=(const Date other) const noexcept { return _data <= other._data; }

    bool operator>(const Date other) const noexcept { return _data > other._data; }

    bool operator>=(const Date other) const noexcept { return _data >= other._data; }

    Date operator+(Date other) const noexcept
    {
        if (other.is_interval())
        {
            if (other.is_years_interval())
            {
                return Date{std::uint16_t(year() + other.interval()), month(), day()};
            }

            if (other.is_months_interval())
            {
                auto month_ = std::uint16_t(month() + other.interval());
                auto year_ = year();
                while (month_ > 12U)
                {
                    ++year_;
                    month_ -= 12U;
                }
                return Date{year_, std::uint8_t(month_), day()};
            }

            if (other.is_days_interval())
            {
                auto day_ = std::uint16_t(day() + other.interval());
                auto month_ = month();
                auto year_ = year();

                auto days_of_month = Date::days_of_month(month_, year_);
                while (day_ > days_of_month)
                {
                    day_ -= days_of_month;

                    if (++month_ > 12U)
                    {
                        month_ = 1U;
                        ++year_;
                    }

                    days_of_month = Date::days_of_month(month_, year_);
                }
                return Date{year_, std::uint8_t(month_), std::uint8_t(day_)};
            }
        }

        return *this;
    }

    Date &operator+=(Date other) noexcept
    {
        _data = (*this + other)._data;
        return *this;
    }

    Date operator-(Date other) const noexcept
    {
        if (other.is_interval())
        {
            if (other.is_years_interval())
            {
                return Date{std::uint16_t(std::max(std::int16_t(0), std::int16_t(year() - other.interval()))), month(),
                            day()};
            }

            if (other.is_months_interval())
            {
                auto month_ = std::int16_t(month() - other.interval());
                auto year_ = std::int16_t(year());
                while (month_ < 1)
                {
                    --year_;
                    month_ += 12U;
                }
                return Date{std::uint16_t(std::max(std::int16_t(0), year_)), std::uint8_t(month_), day()};
            }

            if (other.is_days_interval())
            {
                auto interval = other.interval();
                auto day_ = day();
                auto month_ = month();
                auto year_ = year();

                while (interval >= day_)
                {
                    interval -= day_;
                    if (--month_ < 1)
                    {
                        month_ = 12;
                        --year_;
                    }
                    day_ = Date::days_of_month(month_, year_);
                }
                day_ -= interval;
                return Date{year_, std::uint8_t(month_), std::uint8_t(day_)};
            }
        }

        return *this;
    }

    Date &operator-=(Date other) noexcept
    {
        _data = (*this - other)._data;
        return *this;
    }

    [[nodiscard]] std::string to_string() const { return fmt::format("{:04d}-{:02d}-{:02d}", year(), month(), day()); }

    [[nodiscard]] data_t data() const noexcept { return _data; }

private:
    constexpr static auto INTERVAL_FLAG = data_t(1U << 31U);
    constexpr static auto INTERVAL_YEARS_FLAG = data_t(1U << 30U);
    constexpr static auto INTERVAL_MONTHS_FLAG = data_t(1U << 29U);
    constexpr static auto INTERVAL_DAYS_FLAG = data_t(1U << 28U);

    data_t _data{0U};

    [[nodiscard]] bool is_interval() const noexcept { return static_cast<bool>(_data & INTERVAL_FLAG); }
    [[nodiscard]] bool is_years_interval() const noexcept { return static_cast<bool>(_data & INTERVAL_YEARS_FLAG); }
    [[nodiscard]] bool is_months_interval() const noexcept { return static_cast<bool>(_data & INTERVAL_MONTHS_FLAG); }
    [[nodiscard]] bool is_days_interval() const noexcept { return static_cast<bool>(_data & INTERVAL_DAYS_FLAG); }
    [[nodiscard]] std::uint16_t interval() const noexcept { return _data & std::numeric_limits<std::uint16_t>::max(); }

    [[nodiscard]] static std::uint8_t days_of_month(const std::uint8_t month, const std::uint32_t year) noexcept
    {
        if (month == 1U || month == 3U || month == 5U || month == 7U || month == 8U || month == 10U || month == 12U)
        {
            return 31U;
        }

        if (month == 2U)
        {
            return 28U + static_cast<std::uint8_t>(Date::is_leap_year(year));
        }

        return 30U;
    }

    [[nodiscard]] static bool is_leap_year(const std::uint16_t year) noexcept
    {
        return ((year % 4U == 0U) && (year % 100U != 0U)) || (year % 400U == 0U);
    }
};
} // namespace db::type

namespace std {
template <> struct hash<db::type::Date>
{
public:
    std::size_t operator()(const db::type::Date &date) const
    {
        return std::hash<db::type::Date::data_t>{}(date.data());
    }
};
} // namespace std