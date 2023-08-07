#pragma once

#include "adjustment_interface.h"
#include <db/expression/operation.h>
#include <memory>

namespace db::plan::logical {
/**
 * This adjustment unifies resolves the source of predicates, i.e.,
 * SELECT * FROM students WHERE id < 5 -> WHERE students.id < 5.
 * This is required for cardinality estimation.
 */
class ResolvePredicateSourceAdjustment final : public AdjustmentInterface
{
public:
    constexpr ResolvePredicateSourceAdjustment() noexcept = default;
    ~ResolvePredicateSourceAdjustment() noexcept override = default;

    void apply(std::unique_ptr<NodeInterface> &node) override;

private:
    static void apply(std::unique_ptr<expression::Operation> &predicate, const topology::LogicalSchema &schema);
    static void apply(expression::Term &term, const topology::LogicalSchema &schema);
};
} // namespace db::plan::logical