#pragma once

#include "adjustments/adjustment_interface.h"
#include "node/node_interface.h"
#include <memory>
#include <vector>

namespace db::plan::logical {
class Adjuster
{
public:
    Adjuster() = default;
    ~Adjuster() = default;

    void adjust(std::unique_ptr<NodeInterface> &root_node) const;

    void add(std::unique_ptr<AdjustmentInterface> &&adjustment) { _adjustments.emplace_back(std::move(adjustment)); }

private:
    std::vector<std::unique_ptr<AdjustmentInterface>> _adjustments;
};
} // namespace db::plan::logical