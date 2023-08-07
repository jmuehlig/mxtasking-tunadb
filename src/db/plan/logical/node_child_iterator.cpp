#include "node_child_iterator.h"
#include "node/node_interface.h"

using namespace db::plan::logical;

NodeInterface *TreeNodeChildIterator::child(const UnaryNode *node) const
{
    return node->child().get();
}

std::pair<NodeInterface *, NodeInterface *> TreeNodeChildIterator::children(const BinaryNode *node) const
{
    return std::make_pair(node->left_child().get(), node->right_child().get());
}