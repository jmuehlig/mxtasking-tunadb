#pragma once

#include "histogram.h"
#include <algorithm>
#include <cstdint>
#include <db/data/value.h>
#include <memory>
#include <vector>

namespace db::statistic {
class EquiDepthBin
{
public:
    constexpr EquiDepthBin(const std::int64_t lower, const std::int64_t upper, const std::uint64_t count) noexcept
        : _lower(lower), _upper(upper), _count(count)
    {
    }

    ~EquiDepthBin() noexcept = default;

    [[nodiscard]] std::int64_t lower() const noexcept { return _lower; }
    [[nodiscard]] std::int64_t upper() const noexcept { return _upper; }
    [[nodiscard]] std::uint64_t count() const noexcept { return _count; }
    [[nodiscard]] std::uint64_t width() const noexcept { return _upper - _lower + 1U; }

    bool operator<(const EquiDepthBin &other) const noexcept { return _lower < other._lower; }

    [[nodiscard]] std::uint64_t approximate_equals() const noexcept
    {
        return std::min<std::uint64_t>(1U, _count / width());
    }

    [[nodiscard]] std::uint64_t approximate_lesser_equals(const std::int64_t key) const noexcept
    {
        return _count * ((key - _lower + 1U) / width());
    }

    [[nodiscard]] std::uint64_t approximate_greater_equals(const std::int64_t key) const noexcept
    {
        const auto width = this->width();
        return width > 1U ? _count * ((_upper - key) / width) : _count;
    }

    [[nodiscard]] std::uint64_t approximate_lesser(const std::int64_t key) const noexcept
    {
        const auto width = this->width();
        return width > 1U ? _count * ((key - _lower) / width) : _count;
    }

    [[nodiscard]] std::uint64_t approximate_greater(const std::int64_t key) const noexcept
    {
        return _count * ((_upper - (key + 1U)) / width());
    }

private:
    std::int64_t _lower;
    std::int64_t _upper;
    std::uint64_t _count;
};

class EquiDepthHistogram final : public HistogramInterface
{
public:
    EquiDepthHistogram(const std::uint64_t depth, const std::int64_t lower_key, const std::int64_t upper_key,
                       const std::uint64_t count, std::vector<EquiDepthBin> &&data)
        : _depth(depth), _lower_key(lower_key), _upper_key(upper_key), _count(count), _data(std::move(data))
    {
    }

    ~EquiDepthHistogram() override = default;

    [[nodiscard]] Type type() const noexcept override { return HistogramInterface::Type::EquiDepth; }

    [[nodiscard]] std::uint64_t count() const noexcept { return _count; }
    [[nodiscard]] std::uint64_t depth() const noexcept { return _depth; }
    [[nodiscard]] std::uint64_t lower_key() const noexcept { return _lower_key; }
    [[nodiscard]] std::uint64_t upper_key() const noexcept { return _upper_key; }
    [[nodiscard]] std::uint64_t width() const noexcept
    {
        return std::accumulate(_data.begin(), _data.end(), 0U,
                               [](const auto &sum, const auto &bin) { return sum + bin.width(); });
    }
    [[nodiscard]] const std::vector<EquiDepthBin> &bins() const noexcept { return _data; }

    [[nodiscard]] std::uint64_t approximate_equals(const data::Value &key) const noexcept override
    {
        const auto as_int = EquiDepthHistogram::to_int(key);
        if (as_int.has_value()) [[likely]]
        {
            return this->approximate_equals(as_int.value());
        }

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_not_equals(const data::Value &key) const noexcept override
    {
        const auto as_int = EquiDepthHistogram::to_int(key);
        if (as_int.has_value()) [[likely]]
        {
            return this->approximate_not_equals(as_int.value());
        }

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_lesser_equals(const data::Value &key) const noexcept override
    {
        const auto as_int = EquiDepthHistogram::to_int(key);
        if (as_int.has_value()) [[likely]]
        {
            return this->approximate_lesser_equals(as_int.value());
        }

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_lesser(const data::Value &key) const noexcept override
    {
        const auto as_int = EquiDepthHistogram::to_int(key);
        if (as_int.has_value()) [[likely]]
        {
            return this->approximate_lesser(as_int.value());
        }

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_greater_equals(const data::Value &key) const noexcept override
    {
        const auto as_int = EquiDepthHistogram::to_int(key);
        if (as_int.has_value()) [[likely]]
        {
            return this->approximate_greater_equals(as_int.value());
        }

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_greater(const data::Value &key) const noexcept override
    {
        const auto as_int = EquiDepthHistogram::to_int(key);
        if (as_int.has_value()) [[likely]]
        {
            return this->approximate_greater(as_int.value());
        }

        return 0U;
    }

    [[nodiscard]] std::uint64_t approximate_between(const data::Value &min_key,
                                                    const data::Value &max_key) const noexcept override
    {
        const auto min_as_int = EquiDepthHistogram::to_int(min_key);
        if (min_as_int.has_value()) [[likely]]
        {
            const auto max_as_int = EquiDepthHistogram::to_int(max_key);
            if (max_as_int.has_value()) [[likely]]
            {
                return this->approximate_between(min_as_int.value(), max_as_int.value());
            }
        }

        return 0U;
    }

private:
    std::uint64_t _depth;

    std::int64_t _lower_key;

    std::int64_t _upper_key;

    std::uint64_t _count{0U};

    std::vector<EquiDepthBin> _data;

    [[nodiscard]] std::optional<std::size_t> index(const std::int64_t key) const noexcept
    {
        const auto tmp_bin = EquiDepthBin{key, key, 0U};
        const auto lower_bound =
            std::lower_bound(_data.begin(), _data.end(), tmp_bin,
                             [](const auto &left, const auto &right) { return left.upper() < right.upper(); });
        if (lower_bound == _data.end()) [[unlikely]]
        {
            return std::nullopt;
        }

        return std::distance(_data.begin(), lower_bound);
    }

    [[nodiscard]] static std::optional<std::int64_t> to_int(const data::Value &value) noexcept
    {
        auto value_as_int = std::optional<std::int64_t>{std::nullopt};

        std::visit(
            [&value_as_int](const auto &key) {
                using T = std::decay_t<decltype(key)>;
                if constexpr (std::is_same<T, type::underlying<type::Id::BIGINT>::value>::value ||
                              std::is_same<T, type::underlying<type::Id::INT>::value>::value ||
                              std::is_same<T, type::underlying<type::Id::DECIMAL>::value>::value ||
                              std::is_same<T, type::underlying<type::Id::BOOL>::value>::value)
                {
                    value_as_int = std::int64_t(key);
                }
                else if constexpr (std::is_same<T, type::underlying<type::Id::DATE>::value>::value)
                {
                    value_as_int = std::int64_t(key.data());
                }
                else if constexpr (std::is_same<T, std::string>::value)
                {
                    value_as_int = std::int64_t(std::hash<std::string>{}(key));
                }
                else if constexpr (std::is_same<T, std::string_view>::value)
                {
                    value_as_int = std::int64_t(std::hash<std::string_view>{}(key));
                }
            },
            value.value());

        return value_as_int;
    }

    [[nodiscard]] static std::uint64_t count(std::vector<EquiDepthBin>::const_iterator begin,
                                             std::vector<EquiDepthBin>::const_iterator end) noexcept
    {

        return std::accumulate(begin, end, 0ULL, [](const auto sum, const auto &bin) { return sum + bin.count(); });
    }

    [[nodiscard]] std::uint64_t approximate_not_equals(std::int64_t key) const noexcept
    {
        return _count - approximate_equals(key);
    }

    [[nodiscard]] std::uint64_t approximate_equals(std::int64_t key) const noexcept;
    [[nodiscard]] std::uint64_t approximate_lesser_equals(std::int64_t key) const noexcept;
    [[nodiscard]] std::uint64_t approximate_lesser(std::int64_t key) const noexcept;
    [[nodiscard]] std::uint64_t approximate_greater_equals(std::int64_t key) const noexcept;
    [[nodiscard]] std::uint64_t approximate_greater(std::int64_t key) const noexcept;
    [[nodiscard]] std::uint64_t approximate_between(std::int64_t min_key, std::int64_t max_key) const noexcept;
};

class EquiDepthHistogramBuilder
{
public:
    EquiDepthHistogramBuilder() = default;
    ~EquiDepthHistogramBuilder() = default;

    void insert(const data::Value &value) { insert(value.value()); }

    void insert(const data::Value::value_t &value)
    {
        std::visit(
            [&](const auto &key) {
                using T = std::decay_t<decltype(key)>;
                if constexpr (std::is_same<T, type::underlying<type::Id::BIGINT>::value>::value ||
                              std::is_same<T, type::underlying<type::Id::INT>::value>::value ||
                              std::is_same<T, type::underlying<type::Id::DECIMAL>::value>::value ||
                              std::is_same<T, type::underlying<type::Id::BOOL>::value>::value)
                {
                    insert(std::int64_t(key));
                }
                else if constexpr (std::is_same<T, type::underlying<type::Id::DATE>::value>::value)
                {
                    insert(std::int64_t(key.data()));
                }
                else if constexpr (std::is_same<T, std::string>::value)
                {
                    insert(std::int64_t(std::hash<std::string>()(key)));
                }
                else if constexpr (std::is_same<T, std::string_view>::value)
                {
                    insert(std::int64_t(std::hash<std::string_view>()(key)));
                }
            },
            value);
    }

    void insert(const std::int64_t key)
    {
        if (_data.contains(key))
        {
            ++_data.at(key);
        }
        else
        {
            _data.insert(std::make_pair(key, 1U));
        }
    }

    [[nodiscard]] std::unique_ptr<EquiDepthHistogram> build(const std::uint16_t count_bins)
    {
        const auto count = std::accumulate(_data.begin(), _data.end(), 0ULL,
                                           [](const auto sum, const auto &item) { return sum + std::get<1>(item); });

        const auto depth = std::max(std::uint64_t(1U), std::uint64_t(count / count_bins));
        auto data = std::vector<EquiDepthBin>{};
        data.reserve(count_bins);

        auto lower = _data.begin()->first;
        auto bin_count = 0ULL;

        for (const auto &item : _data)
        {
            if (bin_count + std::get<1>(item) > depth)
            {
                data.emplace_back(lower, std::get<0>(item) - 1, bin_count);
                lower = std::get<0>(item);
                bin_count = 0ULL;
            }

            bin_count += std::get<1>(item);
        }
        data.emplace_back(lower, _data.rbegin()->first, bin_count);

        return std::make_unique<EquiDepthHistogram>(depth, data.front().lower(), data.back().upper(), count,
                                                    std::move(data));
    }

private:
    std::map<std::int64_t, std::uint64_t> _data;
};
} // namespace db::statistic