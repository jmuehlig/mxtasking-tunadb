#pragma once
#include <fmt/core.h>
#include <string>
#include <utility>
#include <vector>

namespace db::util {
class string
{
public:
    [[nodiscard]] static std::string replace(std::string &&original,
                                             std::vector<std::pair<std::string, std::string>> &&replacers);

    template <typename T> [[nodiscard]] static std::string shorten_number(T number)
    {
        if (number >= 1000000U)
        {
            return fmt::format("{:.3f} M", number / 1000000.0);
        }

        if (number >= 1000U)
        {
            return fmt::format("{:.3f} k", number / 1000.0);
        }

        if constexpr (std::is_same<T, float>::value)
        {
            return fmt::format("{:.3f}", number);
        }
        else
        {
            return fmt::format("{}", number);
        }
    }

    [[nodiscard]] static std::string shorten_data_size(const std::uint64_t count_bytes)
    {
        if (count_bytes >= 1024ULL * 1024ULL * 1024ULL)
        {
            return fmt::format("{:.3f} GB", count_bytes / (1024ULL * 1024ULL * 1024.0));
        }

        if (count_bytes >= 1024ULL * 1024ULL)
        {
            return fmt::format("{:.3f} MB", count_bytes / (1024ULL * 1024.0));
        }

        if (count_bytes >= 1024ULL)
        {
            return fmt::format("{:.3f} kB", count_bytes / 1024.0);
        }

        return fmt::format("{} B", count_bytes);
    }
};
} // namespace db::util