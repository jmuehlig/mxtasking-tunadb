#pragma once

#include <cstdint>
#include <optional>

namespace db::expression {
class Annotation
{
public:
    constexpr Annotation() noexcept = default;
    Annotation(Annotation &&) noexcept = default;
    Annotation(const Annotation &) noexcept = default;
    ~Annotation() noexcept = default;

    Annotation &operator=(Annotation &&) noexcept = default;
    Annotation &operator=(const Annotation &) noexcept = default;

    [[nodiscard]] std::optional<float> selectivity() const noexcept { return _selectivity; }

    void selectivity(const float selectivity) noexcept { _selectivity = selectivity; }

private:
    std::optional<float> _selectivity{std::nullopt};
};
} // namespace db::expression