#pragma once
#include <db/expression/operation.h>
#include <db/plan/logical/node/aggregation_node.h>
#include <db/plan/logical/node/join_node.h>
#include <db/plan/logical/node/order_by_node.h>
#include <db/plan/optimizer/plan_view.h>
#include <db/plan/optimizer/rule_interface.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace db::plan::optimizer {
/**
 * This optimization maps methods for joins and grouped
 * aggregation to physical implementations (find the best
 * join i.e.)
 */
class PhysicalOperatorRule final : public RuleInterface
{
public:
    PhysicalOperatorRule() noexcept = default;
    ~PhysicalOperatorRule() noexcept override = default;

    [[nodiscard]] bool apply(PlanView &plan) override;

    [[nodiscard]] bool is_affect_relation() const noexcept override { return false; }

    [[nodiscard]] bool is_multi_pass() const noexcept override { return false; }

private:
    [[nodiscard]] static logical::JoinNode::Method choose_method(const PlanView &plan, logical::JoinNode *join_node);
    [[nodiscard]] static logical::AggregationNode::Method choose_method(logical::AggregationNode *join_node)
    {
        if (join_node->groups().has_value() == false)
        {
            return logical::AggregationNode::Method::SimpleAggregation;
        }

        return join_node->relation().cardinality() > 100U ? logical::AggregationNode::Method::RadixAggregation
                                                          : logical::AggregationNode::Method::HashAggregation;
    }
    [[nodiscard]] static logical::OrderByNode::Method choose_method(const PlanView &plan,
                                                                    logical::OrderByNode *order_by_node);

    [[nodiscard]] static bool is_selective(const PlanView &plan, logical::NodeInterface *node);
    [[nodiscard]] static bool has_only_equal_comparison(const std::unique_ptr<expression::Operation> &predicate);
};
} // namespace db::plan::optimizer