#pragma once
#include <db/plan/optimizer/phase_interface.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>
#include <utility>
#include <vector>

namespace db::plan::optimizer {

class RulePhaseInterface : public PhaseInterface
{
public:
    RulePhaseInterface() = default;
    virtual ~RulePhaseInterface() = default;

    [[nodiscard]] std::pair<bool, PlanView> apply(PlanView &&plan_view) override;

protected:
    template <typename T> void add() { _rules.template emplace_back(std::make_unique<T>()); }

private:
    std::vector<std::unique_ptr<RuleInterface>> _rules;
};

class ExpressionSimplificationPhase final : public RulePhaseInterface
{
public:
    ExpressionSimplificationPhase();
    ~ExpressionSimplificationPhase() override = default;

    [[nodiscard]] bool is_require_cardinality() const noexcept override { return false; }
};

class PredicatePushdownPhase final : public RulePhaseInterface
{
public:
    PredicatePushdownPhase();
    ~PredicatePushdownPhase() override = default;

    [[nodiscard]] bool is_require_cardinality() const noexcept override { return false; }
};

class EarlySelectionPhase final : public RulePhaseInterface
{
public:
    EarlySelectionPhase();
    ~EarlySelectionPhase() override = default;

    [[nodiscard]] bool is_require_cardinality() const noexcept override { return false; }
};

class EarlyProjectionPhase final : public RulePhaseInterface
{
public:
    EarlyProjectionPhase();
    ~EarlyProjectionPhase() override = default;

    [[nodiscard]] bool is_require_cardinality() const noexcept override { return false; }
};

class PhysicalOperatorMappingPhase final : public RulePhaseInterface
{
public:
    PhysicalOperatorMappingPhase();
    ~PhysicalOperatorMappingPhase() override = default;

    [[nodiscard]] bool is_require_cardinality() const noexcept override { return true; }
};
} // namespace db::plan::optimizer