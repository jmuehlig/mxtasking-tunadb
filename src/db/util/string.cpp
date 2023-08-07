#include "string.h"
#include <regex>

using namespace db::util;

std::string string::replace(std::string &&original, std::vector<std::pair<std::string, std::string>> &&replacers)
{
    for (auto &&[find, replace] : replacers)
    {
        original = std::regex_replace(original, std::regex{find}, replace);
    }

    return std::move(original);
}