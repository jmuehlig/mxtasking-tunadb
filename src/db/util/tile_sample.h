#pragma once

#include <cstdint>
#include <db/topology/physical_schema.h>
#include <mx/system/cache.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace db::util {
class TileSample
{
public:
    class Column
    {
    public:
        Column(std::string &&name, const std::uint32_t id, const std::uint64_t offset, const std::uint64_t size_in_byte)
            : _name(std::move(name)), _id(id), _offset(offset)
        {
            _count_samples.resize(size_in_byte / mx::system::cache::line_size());
        }

        ~Column() = default;

        [[nodiscard]] const std::string &name() const noexcept { return _name; }
        [[nodiscard]] std::uint32_t id() const noexcept { return _id; }
        [[nodiscard]] std::uint64_t offset() const noexcept { return _offset; }
        [[nodiscard]] const std::vector<std::uint64_t> &samples() const noexcept { return _count_samples; }
        [[nodiscard]] bool has_sample() const noexcept
        {
            return std::find_if(_count_samples.begin(), _count_samples.end(),
                                [](const auto sample) { return sample > 0U; }) != _count_samples.end();
        }

        void increment(const std::uint64_t offset_in_byte)
        {
            ++_count_samples[offset_in_byte / mx::system::cache::line_size()];
        }

    private:
        std::string _name;
        std::uint32_t _id;
        std::uint64_t _offset;
        std::vector<std::uint64_t> _count_samples; /// Samples per cache line.
    };

    explicit TileSample(const topology::PhysicalSchema &schema)
    {
        for (auto i = 0U; i < schema.size(); ++i)
        {
            _columns.emplace_back(Column{schema.term(i).to_string(), i, schema.pax_offset(i),
                                         schema.type(i).size() * config::tuples_per_tile()});
        }
    }

    ~TileSample() = default;

    [[nodiscard]] std::uint64_t samples() const noexcept { return _count_samples; }
    [[nodiscard]] const std::vector<Column> &columns() const noexcept { return _columns; }
    [[nodiscard]] std::vector<Column> &columns() noexcept { return _columns; }

    void increment() { ++_count_samples; }

private:
    /// Samples of the tile header.
    std::uint64_t _count_samples{0U};

    /// Samples of the tile columns.
    std::vector<Column> _columns;
};
} // namespace db::util