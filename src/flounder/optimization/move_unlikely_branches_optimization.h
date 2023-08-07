#pragma once
#include "optimization_interface.h"
#include <cstdint>
#include <optional>
//#include <unordered_map>
//#include <vector>
//
namespace flounder {
/**
 * Unrolls loops that access less than ine cache line per tuple
 * in order to access at least one cache line.
 * This enables prefetching.
 */
class MoveUnlikelyBranchesOptimization final : public OptimizationInterface
{
public:
    MoveUnlikelyBranchesOptimization() noexcept = default;
    ~MoveUnlikelyBranchesOptimization() noexcept override = default;

    void apply(Program &program) override;

private:
    [[nodiscard]] static std::optional<std::size_t> find_branch_end_section(const InstructionSet &code,
                                                                            std::size_t begin_line,
                                                                            Label end_label) noexcept;
};
} // namespace flounder