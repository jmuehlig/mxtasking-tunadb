#pragma once

#include "optimization_interface.h"
#include <flounder/program.h>
#include <memory>
#include <vector>

namespace flounder {
class Optimizer
{
public:
    Optimizer() = default;
    virtual ~Optimizer() = default;

    void add(std::unique_ptr<OptimizationInterface> &&optimization)
    {
        _optimizations.emplace_back(std::move(optimization));
    }

    void optimize(Program &program);

private:
    std::vector<std::unique_ptr<OptimizationInterface>> _optimizations;
};

class PreRegisterAllocationOptimizer final : public Optimizer
{
public:
    PreRegisterAllocationOptimizer();
    ~PreRegisterAllocationOptimizer() override = default;
};

class PostRegisterAllocationOptimizer final : public Optimizer
{
public:
    PostRegisterAllocationOptimizer();
    ~PostRegisterAllocationOptimizer() override = default;
};
} // namespace flounder