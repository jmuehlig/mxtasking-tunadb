#pragma once

namespace db::parser {
class NodeInterface
{
public:
    NodeInterface() noexcept = default;
    virtual ~NodeInterface() noexcept = default;
};
} // namespace db::parser