#pragma once

#include "histogram.h"
#include <cstdint>
#include <memory>
#include <vector>

namespace db::statistic {
class Statistics
{
public:
    explicit Statistics(const std::uint32_t count_columns)
        : _histograms(count_columns), _count_distinct(count_columns, 0U)
    {
    }

    Statistics(Statistics &&) noexcept = default;

    ~Statistics() = default;

    void count_rows(const std::uint64_t count_rows) noexcept { _count_rows = count_rows; }
    [[nodiscard]] std::uint64_t count_rows() const noexcept { return _count_rows; }

    [[nodiscard]] const std::vector<std::unique_ptr<statistic::HistogramInterface>> &histograms() const noexcept
    {
        return _histograms;
    }

    [[nodiscard]] const std::unique_ptr<statistic::HistogramInterface> &histogram(
        const std::uint32_t index) const noexcept
    {
        return _histograms[index];
    }

    [[nodiscard]] std::unique_ptr<statistic::HistogramInterface> &histogram(const std::uint32_t index) noexcept
    {
        return _histograms[index];
    }

    [[nodiscard]] const std::vector<std::uint64_t> &count_distinct() const noexcept { return _count_distinct; }
    [[nodiscard]] std::vector<std::uint64_t> &count_distinct() noexcept { return _count_distinct; }
    [[nodiscard]] std::uint64_t count_distinct(const std::uint32_t index) const noexcept
    {
        return _count_distinct[index];
    }

private:
    /// One equi depth histogram per column.
    std::vector<std::unique_ptr<statistic::HistogramInterface>> _histograms;

    /// Distinct values per value.
    std::vector<std::uint64_t> _count_distinct;

    /// Number of rows.
    std::uint64_t _count_rows{0U};
};
} // namespace db::statistic