#pragma once

#include <flounder/program.h>
#include <vector>

namespace flounder {
class OptimizationInterface
{
public:
    constexpr OptimizationInterface() noexcept = default;
    virtual ~OptimizationInterface() = default;

    /**
     * Optimizes the flounder code.
     *
     * @param program Program to optimize.
     */
    virtual void apply(Program &program) = 0;
};
} // namespace flounder