#pragma once

#include <cstdint>
#include <db/plan/optimizer/plan_view.h>

namespace db::plan::optimizer {
class CostModel
{
public:
    [[nodiscard]] static std::uint64_t estimate(const PlanView &plan);
};
} // namespace db::plan::optimizer