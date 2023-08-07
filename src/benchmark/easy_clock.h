#pragma once

#include <chrono>
#include <fmt/core.h>
#include <iostream>
#include <string>

namespace benchmark {
class EasyClock
{
public:
    EasyClock(std::string &&name) noexcept : _name(std::move(name)), _start(std::chrono::steady_clock::now()) {}

    void intermediate(std::string &&name) const
    {
        const auto now = std::chrono::steady_clock::now();
        const auto elapsed = now - _start;

        std::cout << fmt::format("[EasyClock] {} ({}): {:.3f}us", _name, std::move(name),
                                 std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count() / 1000.0)
                  << std::endl;
    }

    ~EasyClock()
    {
        const auto end = std::chrono::steady_clock::now();
        const auto elapsed = end - _start;

        std::cout << fmt::format("[EasyClock] {}: {:.3f}us", std::move(_name),
                                 std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count() / 1000.0)
                  << std::endl;
    }

private:
    std::string _name;
    const std::chrono::steady_clock::time_point _start;
};
} // namespace benchmark