#pragma once
#include <algorithm>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <db/config.h>

namespace db::execution {
class TileMask
{
public:
    explicit TileMask([[maybe_unused]] const std::size_t size) { set(0U, size); }
    TileMask(TileMask &&) noexcept = default;

    ~TileMask() = default;

    TileMask &operator=(TileMask &&) noexcept = default;

    [[nodiscard]] bool is_set(const std::size_t index) const noexcept { return _mask.test(index); }
    [[nodiscard]] std::size_t count() const noexcept { return _mask.count(); }
    [[nodiscard]] bool is_any_set() const noexcept { return _mask.any(); }
    void set(const std::size_t index) noexcept { _mask.set(index, true); }
    void set(const std::size_t from, const std::size_t count)
    {
        if (count == 0U)
        {
            return;
        }

        auto tmp_set = std::bitset<config::tuples_per_tile()>{0U};
        tmp_set.flip();
        tmp_set >>= (_mask.size() - count);
        tmp_set <<= from;
        _mask |= tmp_set;
    }
    void unset(const std::size_t index) noexcept { _mask.set(index, false); }
    void unset(const std::size_t from, const std::size_t count) noexcept
    {
        for (auto i = 0U; i < count; ++i)
        {
            unset(from + i);
        }
    }
    void unset() noexcept { _mask.reset(); }

    [[nodiscard]] std::bitset<config::tuples_per_tile()> &mask() { return _mask; }

private:
    std::bitset<config::tuples_per_tile()> _mask{0U};
};
} // namespace db::execution