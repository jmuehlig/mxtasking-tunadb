#pragma once

#include "histogram.h"
#include <algorithm>
#include <cstdint>
#include <db/data/value.h>
#include <map>
#include <vector>

namespace db::statistic {
class SingletonHistogram final : public HistogramInterface
{
public:
    SingletonHistogram(const std::uint64_t count, std::map<data::Value::value_t, std::uint64_t> &&data)
        : _count(count), _data(std::move(data))
    {
    }

    ~SingletonHistogram() override = default;

    [[nodiscard]] Type type() const noexcept override { return HistogramInterface::Type::Singleton; }

    [[nodiscard]] std::uint64_t count() const noexcept { return _count; }
    [[nodiscard]] std::uint64_t width() const noexcept { return _data.size(); }
    [[nodiscard]] const std::map<data::Value::value_t, std::uint64_t> &data() const noexcept { return _data; }

    [[nodiscard]] std::uint64_t approximate_equals(const data::Value &key) const noexcept override
    {
        if (auto iterator = _data.find(key.value()); iterator != _data.end())
        {
            return iterator->second;
        }

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_not_equals(const data::Value &key) const noexcept override
    {
        return _count - approximate_equals(key);
    }

    [[nodiscard]] std::uint64_t approximate_lesser_equals(const data::Value & /*key*/) const noexcept override
    {
        // TODO

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_lesser(const data::Value & /*key*/) const noexcept override
    {
        // TODO

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_greater_equals(const data::Value & /*key*/) const noexcept override
    {
        // TODO

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_greater(const data::Value & /*key*/) const noexcept override
    {
        // TODO

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_between(const data::Value & /*min_key*/,
                                                    const data::Value & /*max_key*/) const noexcept override
    {
        // TODO

        return 0U;
    }

private:
    std::uint64_t _count{0U};
    std::map<data::Value::value_t, std::uint64_t> _data;
};

class SingletonHistogramBuilder
{
public:
    SingletonHistogramBuilder() = default;
    ~SingletonHistogramBuilder() = default;

    void insert(const data::Value &value)
    {
        if (std::holds_alternative<std::string_view>(value.value()))
        {
            const auto view = std::get<std::string_view>(value.value());
            const auto string_size = view.size();
            const auto end_position = view.find('\0');

            auto string =
                std::string{std::get<std::string_view>(value.value()).data(), std::min(string_size, end_position)};
            insert(data::Value{value.type(), data::Value::value_t{std::move(string)}});

            return;
        }

        auto iterator = _data.find(value.value());
        if (iterator != _data.end())
        {
            ++iterator->second;
        }
        else
        {
            _data.insert(std::make_pair(value.value(), 1U));
        }
    }

    [[nodiscard]] std::unique_ptr<SingletonHistogram> build()
    {
        auto data = std::map<data::Value::value_t, std::uint64_t>{};
        auto count = 0UL;
        for (auto &[value, item_count] : _data)
        {
            count += item_count;
            data.insert(std::make_pair(std::move(value), item_count));
        }

        return std::make_unique<SingletonHistogram>(count, std::move(data));
    }

private:
    std::unordered_map<data::Value::value_t, std::uint64_t> _data;

    void insert(data::Value &&value)
    {
        auto iterator = _data.find(value.value());
        if (iterator != _data.end())
        {
            ++iterator->second;
        }
        else
        {
            _data.insert(std::make_pair(std::move(value.value()), 1U));
        }
    }
};
} // namespace db::statistic