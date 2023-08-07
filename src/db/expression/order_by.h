#pragma once

#include "operation.h"
#include <memory>
#include <string>

namespace db::expression {
class OrderBy
{
public:
    enum Direction
    {
        ASC,
        DESC
    };

    OrderBy() noexcept = default;

    explicit OrderBy(std::unique_ptr<Operation> &&expression) noexcept : _expression(std::move(expression)) {}

    OrderBy(std::unique_ptr<Operation> &&expression, const Direction direction) noexcept
        : _expression(std::move(expression)), _direction(direction)
    {
    }

    OrderBy(OrderBy &&) noexcept = default;

    ~OrderBy() noexcept = default;

    OrderBy &operator=(OrderBy &&) noexcept = default;

    [[nodiscard]] std::unique_ptr<Operation> &expression() noexcept { return _expression; }
    [[nodiscard]] const std::unique_ptr<Operation> &expression() const noexcept { return _expression; }
    [[nodiscard]] Direction direction() const noexcept { return _direction; }

    [[nodiscard]] std::string to_string() const noexcept
    {
        return _expression->to_string() + (_direction == Direction::ASC ? " ASC" : " DESC");
    }

private:
    std::unique_ptr<Operation> _expression{nullptr};
    Direction _direction{Direction::ASC};
};
} // namespace db::expression
