#pragma once

#include <cstdint>

namespace db::type {
class CharDescription
{
public:
    explicit constexpr CharDescription(const std::uint16_t length) noexcept : _length(length) {}

    ~CharDescription() noexcept = default;

    [[nodiscard]] std::uint16_t length() const noexcept { return _length; }

    [[nodiscard]] bool operator==(const CharDescription &other) const noexcept { return _length == other._length; }

private:
    std::uint16_t _length;
};
} // namespace db::type