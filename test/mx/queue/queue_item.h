#pragma once

namespace test::mx::queue {
class Item
{
public:
    constexpr Item() noexcept = default;
    ~Item() noexcept = default;

    void next(Item *next) noexcept { _next = next; }
    [[nodiscard]] Item *next() const noexcept { return _next; }

private:
    Item *_next{nullptr};
};
} // namespace test::mx::queue