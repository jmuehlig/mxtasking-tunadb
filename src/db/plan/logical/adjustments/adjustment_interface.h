#pragma once

#include <db/plan/logical/node/node_interface.h>
#include <memory>

namespace db::plan::logical {
class AdjustmentInterface
{
public:
    constexpr AdjustmentInterface() noexcept = default;
    virtual ~AdjustmentInterface() = default;

    /**
     * Adjusts the given plan.
     *
     * @param node Node to adjust, typically the root node.
     */
    virtual void apply(std::unique_ptr<NodeInterface> &node) = 0;
};
} // namespace db::plan::logical