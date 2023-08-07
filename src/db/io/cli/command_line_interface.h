#pragma once
#include <cstdlib>
#include <linenoise.h>
#include <optional>
#include <string>

namespace db::io {
class CommandLineInterface
{
public:
    CommandLineInterface(std::string &&history_file_name, std::string &&prompt_message) noexcept
        : _history_file_name(std::move(history_file_name)), _prompt_message(std::move(prompt_message))
    {
        linenoiseHistoryLoad(_history_file_name.c_str());
        linenoiseSetMultiLine(1);
    }

    ~CommandLineInterface() noexcept = default;

    std::optional<std::string> next()
    {
        char *line;
        if ((line = linenoise(_prompt_message.c_str())) != nullptr)
        {
            auto line_as_string = std::string{line};
            std::free(line);

            linenoiseHistoryAdd(line_as_string.c_str());
            linenoiseHistorySave(_history_file_name.c_str());

            return std::make_optional(std::move(line_as_string));
        }

        return std::nullopt;
    }

private:
    const std::string _history_file_name;
    const std::string _prompt_message;
};
} // namespace db::io