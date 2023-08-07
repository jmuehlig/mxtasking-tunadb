#pragma once

#include "plan_view.h"
#include <utility>

namespace db::plan::optimizer {

class PhaseInterface
{
public:
    constexpr PhaseInterface() = default;
    virtual ~PhaseInterface() = default;

    [[nodiscard]] virtual std::pair<bool, PlanView> apply(PlanView &&plan_view) = 0;

    [[nodiscard]] virtual bool is_require_cardinality() const noexcept = 0;
};
} // namespace db::plan::optimizer