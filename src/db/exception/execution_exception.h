#pragma once

#include "abstract_exception.h"
#include <fmt/core.h>

namespace db::exception {
class ExecutionException : public AbstractException
{
public:
    ExecutionException(std::string &&message) noexcept
        : AbstractException(fmt::format("Execution error: {}", std::move(message)))
    {
    }
    ~ExecutionException() noexcept override = default;
};

class NotImplementedException final : public ExecutionException
{
public:
    NotImplementedException(std::string &&what) noexcept
        : ExecutionException(fmt::format("Functionality '{}' not implemented", std::move(what)))
    {
    }
    ~NotImplementedException() noexcept override = default;
};

class CastException final : public ExecutionException
{
public:
    CastException(std::string &&from_type_name, std::string &&to_type_name) noexcept
        : ExecutionException(
              fmt::format("Can not cast from type {} to type {}.", std::move(from_type_name), std::move(to_type_name)))
    {
    }
    ~CastException() noexcept override = default;
};

class OperationNotAllowedException final : public ExecutionException
{
public:
    OperationNotAllowedException(std::string &&operation, std::string &&left_type_name,
                                 std::string &&right_type_name) noexcept
        : ExecutionException(fmt::format("Operation {} not allowed for types {} and {}.", std::move(operation),
                                         std::move(left_type_name), std::move(right_type_name)))
    {
    }
    OperationNotAllowedException(std::string &&operation, std::string &&type_name) noexcept
        : ExecutionException(
              fmt::format("Operation {} not allowed for type {}.", std::move(operation), std::move(type_name)))
    {
    }
    explicit OperationNotAllowedException(std::string &&operation) noexcept
        : ExecutionException(fmt::format("Operation {} not allowed ", std::move(operation)))
    {
    }
    ~OperationNotAllowedException() noexcept override = default;
};

class SymbolNotFoundException final : public ExecutionException
{
public:
    SymbolNotFoundException(std::string &&symbol_name) noexcept
        : ExecutionException(fmt::format("Symbol {} not found.", std::move(symbol_name)))
    {
    }
    ~SymbolNotFoundException() noexcept override = default;
};

class ExpressionNotFoundException final : public ExecutionException
{
public:
    ExpressionNotFoundException(std::string &&expression_name) noexcept
        : ExecutionException(fmt::format("Symbol {} not found.", std::move(expression_name)))
    {
    }
    ~ExpressionNotFoundException() noexcept override = default;
};

class CouldNotCompileException final : public ExecutionException
{
public:
    CouldNotCompileException(std::string &&node_name) noexcept
        : ExecutionException(fmt::format("Could not compile node {}", std::move(node_name)))
    {
    }
    ~CouldNotCompileException() noexcept override = default;
};
} // namespace db::exception