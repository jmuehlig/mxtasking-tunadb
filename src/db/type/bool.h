#pragma once
#include <algorithm>
#include <cctype>
#include <string.h>
#include <string>

namespace db::type {
class Bool
{
public:
    [[nodiscard]] static bool from_string(std::string_view value) noexcept
    {
        return strncasecmp(value.data(), "true", 5U) == 0;
    }

    [[nodiscard]] static bool from_string(const std::string &value) noexcept
    {
        return strncasecmp(value.data(), "true", 5U) == 0;
    }

    [[nodiscard]] static std::string to_string(const bool value)
    {
        return value ? std::string{"true"} : std::string{"false"};
    }
};
} // namespace db::type