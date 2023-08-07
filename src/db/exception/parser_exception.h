#pragma once

#include "abstract_exception.h"
#include <fmt/core.h>

namespace db::exception {
class ParserException final : public AbstractException
{
public:
    ParserException(const std::string &message) noexcept : AbstractException(fmt::format("Parser error: {}", message))
    {
    }
    ~ParserException() noexcept override = default;
};
} // namespace db::exception