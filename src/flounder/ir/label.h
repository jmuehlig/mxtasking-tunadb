#pragma once

#include <string_view>

namespace flounder {
class Label
{
public:
    explicit Label(std::string_view label) noexcept : _label(std::move(label)) {}

    Label(Label &&) noexcept = default;
    Label(const Label &) = default;

    ~Label() = default;

    Label &operator=(Label &&) noexcept = default;
    Label &operator=(const Label &) noexcept = default;

    [[nodiscard]] std::string_view label() const noexcept { return _label; }

    bool operator==(Label other) const noexcept { return _label == other._label; }

private:
    std::string_view _label;
};
} // namespace flounder