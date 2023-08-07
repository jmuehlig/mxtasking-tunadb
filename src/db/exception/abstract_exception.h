#pragma once

#include <exception>
#include <string>

namespace db::exception {
class AbstractException : public std::exception
{
public:
    ~AbstractException() noexcept override = default;

    [[nodiscard]] const char *what() const noexcept override { return _message.c_str(); }

protected:
    AbstractException(std::string &&message) noexcept : _message(std::move(message)) {}

private:
    const std::string _message;
};
} // namespace db::exception