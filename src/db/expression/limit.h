#pragma once
#include <cstdint>
#include <fmt/core.h>
#include <string>

namespace db::expression {
class Limit
{
public:
    explicit constexpr Limit(const std::uint64_t limit) noexcept : _limit(limit) {}
    constexpr Limit(const std::uint64_t limit, const std::uint64_t offset) noexcept : _limit(limit), _offset(offset) {}
    Limit(Limit &&) noexcept = default;
    Limit(const Limit &) noexcept = default;
    ~Limit() noexcept = default;

    Limit &operator=(Limit &&) noexcept = default;

    [[nodiscard]] std::uint64_t limit() const noexcept { return _limit; }
    [[nodiscard]] std::uint64_t offset() const noexcept { return _offset; }

    [[nodiscard]] std::string to_string() const noexcept
    {
        if (_offset > 0U)
        {
            return fmt::format("{} OFFSET {}", _limit, _offset);
        }

        return std::to_string(_limit);
        ;
    }

private:
    std::uint64_t _limit;
    std::uint64_t _offset{0U};
};
} // namespace db::expression