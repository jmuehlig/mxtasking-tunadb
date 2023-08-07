#include "join_predicate_left_right_adjustment.h"
#include <db/plan/logical/node/join_node.h>

using namespace db::plan::logical;

void JoinPredicateLeftRightAdjustment::apply(std::unique_ptr<NodeInterface> &node)
{
    if (node->is_unary())
    {
        this->apply(reinterpret_cast<UnaryNode *>(node.get())->child());
    }
    else if (node->is_binary())
    {
        auto *binary_node = reinterpret_cast<BinaryNode *>(node.get());
        if (typeid(*binary_node) == typeid(JoinNode))
        {
            auto *join_node = reinterpret_cast<JoinNode *>(binary_node);
            JoinPredicateLeftRightAdjustment::apply(join_node->left_child(), join_node->right_child(),
                                                    join_node->predicate());
        }
        this->apply(binary_node->left_child());
        this->apply(binary_node->right_child());
    }
}

void JoinPredicateLeftRightAdjustment::apply(const std::unique_ptr<NodeInterface> &left,
                                             const std::unique_ptr<NodeInterface> &right,
                                             std::unique_ptr<expression::Operation> &operation)
{
    if (operation->is_binary())
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());

        /// Operation is a predicate like A = B
        if (binary_operation->is_comparison())
        {
            if (binary_operation->left_child()->result()->is_attribute() &&
                binary_operation->right_child()->result()->is_attribute())
            {
                if (left->relation().schema().contains(binary_operation->left_child()->result().value()) == false &&
                    left->relation().schema().contains(binary_operation->right_child()->result().value()) &&
                    right->relation().schema().contains(binary_operation->right_child()->result().value()) == false &&
                    right->relation().schema().contains(binary_operation->left_child()->result().value()))
                {
                    binary_operation->invert();
                }
            }
        }

        /// Operation is a predicate like (A = B) AND (C = D) -> apply recursive to both predicates.
        else
        {
            JoinPredicateLeftRightAdjustment::apply(left, right, binary_operation->left_child());
            JoinPredicateLeftRightAdjustment::apply(left, right, binary_operation->right_child());
        }
    }
}