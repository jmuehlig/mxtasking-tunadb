#include "adjuster.h"

using namespace db::plan::logical;

void Adjuster::adjust(std::unique_ptr<NodeInterface> &root_node) const
{
    for (const auto &adjustment : this->_adjustments)
    {
        adjustment->apply(root_node);
    }
}