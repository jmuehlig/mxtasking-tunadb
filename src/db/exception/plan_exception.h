#pragma once

#include "abstract_exception.h"
#include <fmt/core.h>

namespace db::exception {
class PlanningException : public AbstractException
{
public:
    PlanningException(std::string &&message) noexcept
        : AbstractException(fmt::format("Planning error: {}", std::move(message)))
    {
    }
    ~PlanningException() noexcept override = default;
};

class TableNotFoundException final : public PlanningException
{
public:
    TableNotFoundException(const std::string &table_name) noexcept
        : PlanningException(fmt::format("Table {} not found.", table_name))
    {
    }
    ~TableNotFoundException() noexcept override = default;
};

class AttributeNotFoundException final : public PlanningException
{
public:
    AttributeNotFoundException(const std::string &attribute_name) noexcept
        : PlanningException(fmt::format("Attribute {} not found.", attribute_name))
    {
    }
    ~AttributeNotFoundException() noexcept override = default;
};

class OptimizingException : public AbstractException
{
public:
    OptimizingException(std::string &&message) noexcept
        : AbstractException(fmt::format("Optimizing error: {}", std::move(message)))
    {
    }
    ~OptimizingException() noexcept override = default;
};

class OptimizingNoChildrenException final : public OptimizingException
{
public:
    OptimizingNoChildrenException(const std::string &node) noexcept
        : OptimizingException(fmt::format("Node {} has no children.", node))
    {
    }
    ~OptimizingNoChildrenException() noexcept override = default;
};

class OptimizingNoRelationException final : public OptimizingException
{
public:
    OptimizingNoRelationException(const std::string &node) noexcept
        : OptimizingException(fmt::format("Node {} has no relation.", node))
    {
    }
    ~OptimizingNoRelationException() noexcept override = default;
};
} // namespace db::exception