#pragma once

#include <chrono>

namespace db::util {
class Timer
{
public:
    Timer() noexcept : _start(std::chrono::steady_clock::now()) {}
    Timer(const Timer &) noexcept = default;
    Timer(Timer &&) noexcept = default;
    ~Timer() noexcept = default;

    void start() noexcept { _start = std::chrono::steady_clock::now(); }
    void stop() noexcept { _end = std::chrono::steady_clock::now(); }

    template <typename D = std::chrono::milliseconds> D get() const noexcept
    {
        return std::chrono::duration_cast<D>(_end - _start);
    }

private:
    std::chrono::steady_clock::time_point _start;
    std::chrono::steady_clock::time_point _end;
};
} // namespace db::util