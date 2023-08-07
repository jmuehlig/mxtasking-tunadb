#pragma once

#include <utility>

namespace db::plan::logical {
class NodeInterface;
class UnaryNode;
class BinaryNode;

class NodeChildIterator
{
public:
    constexpr NodeChildIterator() noexcept = default;
    virtual ~NodeChildIterator() noexcept = default;

    virtual NodeInterface *child(const UnaryNode *node) const = 0;
    virtual std::pair<NodeInterface *, NodeInterface *> children(const BinaryNode *node) const = 0;
};

class TreeNodeChildIterator final : public NodeChildIterator
{
public:
    constexpr TreeNodeChildIterator() noexcept = default;
    ~TreeNodeChildIterator() noexcept override = default;

    NodeInterface *child(const UnaryNode *node) const override;
    std::pair<NodeInterface *, NodeInterface *> children(const BinaryNode *node) const override;
};

} // namespace db::plan::logical