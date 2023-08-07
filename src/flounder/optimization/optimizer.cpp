#include "optimizer.h"
#include "analyzer.h"
#include "move_unlikely_branches_optimization.h"

using namespace flounder;

void Optimizer::optimize(Program &program)
{
    for (const auto &optimization : this->_optimizations)
    {
        optimization->apply(program);
    }
}

PreRegisterAllocationOptimizer::PreRegisterAllocationOptimizer() = default;

PostRegisterAllocationOptimizer::PostRegisterAllocationOptimizer()
{
    this->add(std::make_unique<MoveUnlikelyBranchesOptimization>());
}