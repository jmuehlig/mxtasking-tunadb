#pragma once

#include <db/data/value.h>
#include <unordered_set>

namespace db::statistic {
class CountDistinctSketchBuilder
{
public:
    CountDistinctSketchBuilder() = default;
    ~CountDistinctSketchBuilder() = default;

    void insert(const data::Value &value) { _distinct_values.insert(value); }

    [[nodiscard]] std::uint64_t get() const noexcept { return _distinct_values.size(); }

private:
    std::unordered_set<data::Value> _distinct_values;
};
} // namespace db::statistic