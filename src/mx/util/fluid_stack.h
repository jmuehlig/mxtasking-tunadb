#pragma once

#include <cstdint>
#include <array>
#include <algorithm>

namespace mx::util
{
template<typename T, std::uint16_t S>
class FluidStack
{
public:
    FluidStack()
    {
    }

    ~FluidStack() noexcept = default;

    void push(const T data)
    {
        const auto index = _head++;
        if  constexpr ((S & (S-1U)) == 0)
        {
            _head &= S - 1U;
        }
        else
        {
            _head %= S;
        }
        _data[index] = data;
    }

    [[nodiscard]] bool contains(const T data)
    {
        return std::find(_data.begin(), _data.end(), data) != _data.end();
    }
private:
    std::array<T, S> _data;
    std::uint16_t _head {0U};
};
}