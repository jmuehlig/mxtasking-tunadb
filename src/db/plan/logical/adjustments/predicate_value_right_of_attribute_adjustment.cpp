#include "predicate_value_right_of_attribute_adjustment.h"
#include <db/plan/logical/node/selection_node.h>

using namespace db::plan::logical;

void PredicateValueRightOfAttributeAdjustment::apply(std::unique_ptr<NodeInterface> &node)
{
    if (node->is_unary())
    {
        if (typeid(*node) == typeid(SelectionNode))
        {
            PredicateValueRightOfAttributeAdjustment::apply(reinterpret_cast<SelectionNode *>(node.get())->predicate());
        }

        this->apply(reinterpret_cast<UnaryNode *>(node.get())->child());
    }
    else if (node->is_binary())
    {
        this->apply(reinterpret_cast<BinaryNode *>(node.get())->left_child());
        this->apply(reinterpret_cast<BinaryNode *>(node.get())->right_child());
    }
}

void PredicateValueRightOfAttributeAdjustment::apply(std::unique_ptr<expression::Operation> &operation)
{
    if (operation->is_unary())
    {
        PredicateValueRightOfAttributeAdjustment::apply(
            reinterpret_cast<expression::UnaryOperation *>(operation.get())->child());
    }
    if (operation->is_binary())
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());

        PredicateValueRightOfAttributeAdjustment::apply(binary_operation->left_child());
        PredicateValueRightOfAttributeAdjustment::apply(binary_operation->right_child());

        if (binary_operation->is_comparison())
        {
            /// Set attribute left and value right.
            if (binary_operation->left_child()->result()->is_value() &&
                binary_operation->right_child()->result()->is_attribute())
            {
                binary_operation->invert();
            }
        }
    }
}