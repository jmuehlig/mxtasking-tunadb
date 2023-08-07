#pragma once
#include <db/expression/operation.h>
#include <db/plan/logical/node/arithmetic_node.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace db::plan::optimizer {
/**
 * This optimization splits arithmetics into several nodes when possible.
 */
class SplitArithmeticRule final : public RuleInterface
{
public:
    constexpr SplitArithmeticRule() noexcept = default;
    ~SplitArithmeticRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return true; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return true; }

private:
    [[nodiscard]] static std::unordered_set<expression::Attribute::Source> extract_sources(
        const std::unique_ptr<expression::Operation> &operation);
    static void extract_operation_by_source(
        const std::unique_ptr<expression::Operation> &operation,
        std::unordered_map<db::expression::Attribute::Source, std::vector<std::unique_ptr<db::expression::Operation>>>
            &source_map);

    static void replace_operations_by_terms(std::unique_ptr<expression::Operation> &operation,
                                            const std::unordered_map<expression::Term, bool> &split_off_terms);
};
} // namespace db::plan::optimizer