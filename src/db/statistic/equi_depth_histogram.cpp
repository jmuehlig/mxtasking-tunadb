#include "equi_depth_histogram.h"

using namespace db::statistic;

std::uint64_t EquiDepthHistogram::approximate_equals(const std::int64_t key) const noexcept
{
    if (this->_data.empty())
    {
        return 0U;
    }

    if (key < this->_lower_key)
    {
        return 0U;
    }

    if (key > this->_upper_key)
    {
        return 0U;
    }

    const auto index = this->index(key);
    if (index.has_value() == false) [[unlikely]]
    {
        return 0U;
    }

    const auto &bin = this->_data[index.value()];
    return this->_data[index.value()].approximate_equals();
}

std::uint64_t EquiDepthHistogram::approximate_lesser(std::int64_t key) const noexcept
{
    if (this->_data.empty())
    {
        return this->_count;
    }

    if (key < this->_lower_key)
    {
        return 0U;
    }

    if (key > this->_upper_key)
    {
        return this->_count;
    }

    const auto index = this->index(key);
    if (index.has_value() == false) [[unlikely]]
    {
        return this->_count;
    }

    const auto count = index.value() > 0
                           ? EquiDepthHistogram::count(this->_data.begin(), this->_data.begin() + (index.value() - 1U))
                           : 0U;
    return count + this->_data[index.value()].approximate_lesser(key);
}

std::uint64_t EquiDepthHistogram::approximate_lesser_equals(const std::int64_t key) const noexcept
{
    if (this->_data.empty())
    {
        return this->_count;
    }

    if (key < this->_lower_key)
    {
        return 0U;
    }

    if (key > this->_upper_key)
    {
        return this->_count;
    }

    const auto index = this->index(key);
    if (index.has_value() == false) [[unlikely]]
    {
        return this->_count;
    }

    const auto count = index.value() > 0
                           ? EquiDepthHistogram::count(this->_data.begin(), this->_data.begin() + (index.value() - 1U))
                           : 0U;

    return count + this->_data[index.value()].approximate_lesser_equals(key);
}

std::uint64_t EquiDepthHistogram::approximate_greater(const std::int64_t key) const noexcept
{
    if (this->_data.empty())
    {
        return this->_count;
    }

    if (key < this->_lower_key)
    {
        return this->_count;
    }

    if (key > this->_upper_key)
    {
        return 0U;
    }

    const auto index = this->index(key);
    if (index.has_value() == false) [[unlikely]]
    {
        return this->_count;
    }

    const auto count = EquiDepthHistogram::count(this->_data.begin() + (index.value() + 1U), this->_data.end());

    const auto &bin = this->_data[index.value()];
    if (bin.upper() > key + 1)
    {
        return count + bin.approximate_greater(key);
    }

    return count;
}

std::uint64_t EquiDepthHistogram::approximate_greater_equals(const std::int64_t key) const noexcept
{
    if (this->_data.empty())
    {
        return this->_count;
    }

    if (key < this->_lower_key)
    {
        return this->_count;
    }

    if (key > this->_upper_key)
    {
        return 0U;
    }

    const auto index = this->index(key);
    if (index.has_value() == false) [[unlikely]]
    {
        return this->_count;
    }

    const auto count = EquiDepthHistogram::count(this->_data.begin() + (index.value() + 1U), this->_data.end());
    return count + this->_data[index.value()].approximate_greater_equals(key);
}

std::uint64_t EquiDepthHistogram::approximate_between(std::int64_t min_key, std::int64_t max_key) const noexcept
{
    if (this->_data.empty())
    {
        return this->_count;
    }

    min_key = std::max(min_key, this->_lower_key);
    max_key = std::min(max_key, this->_upper_key);

    const auto min_index = this->index(min_key);
    const auto max_index = this->index(max_key);
    if (min_index.has_value() == false || max_index.has_value() == false) [[unlikely]]
    {
        return this->_count;
    }

    if (min_index.value() == max_index.value())
    {
        return this->_data[min_index.value()].count();
    }

    auto count = 0ULL;
    if (min_index.value() < max_index.value() && max_index.value() - min_index.value() > 1U)
    {
        count = EquiDepthHistogram::count(this->_data.begin() + (min_index.value() + 1U),
                                          this->_data.begin() + max_index.value());
    }

    const auto &min_bin = this->_data[min_index.value()];
    const auto &max_bin = this->_data[max_index.value()];

    return count + min_bin.approximate_greater_equals(min_key) + max_bin.approximate_lesser_equals(max_key);
}