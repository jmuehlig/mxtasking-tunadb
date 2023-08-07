#pragma once

#include <cstdint>
#include <db/data/value.h>
#include <iostream>
#include <map>
#include <numeric>
#include <utility>
#include <vector>

namespace db::statistic {

class HistogramInterface
{
public:
    enum Type : std::uint8_t
    {
        EquiDepth,
        Singleton
    };

    constexpr HistogramInterface() noexcept = default;
    virtual ~HistogramInterface() noexcept = default;

    [[nodiscard]] virtual Type type() const noexcept = 0;

    [[nodiscard]] virtual std::uint64_t approximate_equals(const data::Value &key) const noexcept = 0;
    [[nodiscard]] virtual std::uint64_t approximate_not_equals(const data::Value &key) const noexcept = 0;
    [[nodiscard]] virtual std::uint64_t approximate_lesser(const data::Value &key) const noexcept = 0;
    [[nodiscard]] virtual std::uint64_t approximate_lesser_equals(const data::Value &key) const noexcept = 0;
    [[nodiscard]] virtual std::uint64_t approximate_greater(const data::Value &key) const noexcept = 0;
    [[nodiscard]] virtual std::uint64_t approximate_greater_equals(const data::Value &key) const noexcept = 0;
    [[nodiscard]] virtual std::uint64_t approximate_between(const data::Value &min_key,
                                                            const data::Value &max_key) const noexcept = 0;
};

} // namespace db::statistic