#include "resolve_predicate_source_adjustment.h"
#include <db/plan/logical/node/aggregation_node.h>
#include <db/plan/logical/node/arithmetic_node.h>
#include <db/plan/logical/node/selection_node.h>

using namespace db::plan::logical;

void ResolvePredicateSourceAdjustment::apply(std::unique_ptr<NodeInterface> &node)
{
    if (node->is_unary())
    {
        if (typeid(*node) == typeid(SelectionNode))
        {
            auto *selection_node = reinterpret_cast<SelectionNode *>(node.get());
            ResolvePredicateSourceAdjustment::apply(selection_node->predicate(),
                                                    selection_node->child()->relation().schema());
        }
        else if (typeid(*node) == typeid(AggregationNode))
        {
            auto *aggregation_node = reinterpret_cast<AggregationNode *>(node.get());
            if (aggregation_node->groups().has_value())
            {
                for (auto &group_by_term : aggregation_node->groups().value())
                {
                    ResolvePredicateSourceAdjustment::apply(group_by_term,
                                                            aggregation_node->child()->relation().schema());
                }
            }
            for (auto &aggregation : aggregation_node->aggregation_operations())
            {
                ResolvePredicateSourceAdjustment::apply(aggregation, aggregation_node->child()->relation().schema());
            }
        }
        else if (typeid(*node) == typeid(ArithmeticNode))
        {
            auto *arithmetic_node = reinterpret_cast<ArithmeticNode *>(node.get());
            for (auto &operation : arithmetic_node->arithmetic_operations())
            {
                ResolvePredicateSourceAdjustment::apply(operation, arithmetic_node->child()->relation().schema());
            }
        }

        this->apply(reinterpret_cast<UnaryNode *>(node.get())->child());
    }
    else if (node->is_binary())
    {
        auto *binary_node = reinterpret_cast<BinaryNode *>(node.get());
        this->apply(binary_node->left_child());
        this->apply(binary_node->right_child());
    }
}

void ResolvePredicateSourceAdjustment::apply(std::unique_ptr<expression::Operation> &predicate,
                                             const topology::LogicalSchema &schema)
{
    if (predicate->is_nullary())
    {
        auto *nullary_operation = reinterpret_cast<expression::NullaryOperation *>(predicate.get());
        if (nullary_operation->result().has_value())
        {
            ResolvePredicateSourceAdjustment::apply(nullary_operation->result().value(), schema);
        }
    }
    else if (predicate->is_unary())
    {
        auto *unary_operation = reinterpret_cast<expression::UnaryOperation *>(predicate.get());
        ResolvePredicateSourceAdjustment::apply(unary_operation->child(), schema);
    }
    else if (predicate->is_binary())
    {
        auto *binary_predicate = reinterpret_cast<expression::BinaryOperation *>(predicate.get());

        ResolvePredicateSourceAdjustment::apply(binary_predicate->left_child(), schema);
        ResolvePredicateSourceAdjustment::apply(binary_predicate->right_child(), schema);
    }
    else if (predicate->is_list())
    {
        auto *list_operation = reinterpret_cast<expression::ListOperation *>(predicate.get());
        for (auto &child : list_operation->children())
        {
            ResolvePredicateSourceAdjustment::apply(child, schema);
        }
    }
}

void ResolvePredicateSourceAdjustment::apply(expression::Term &term, const topology::LogicalSchema &schema)
{
    if (term.is_attribute())
    {
        auto &attribute = term.get<expression::Attribute>();
        if (attribute.source().has_value() == false)
        {
            const auto index = schema.index(term);
            if (index.has_value())
            {
                const auto &schema_term = schema.term(index.value());
                if (schema_term.is_attribute())
                {
                    const auto &schema_attribute = schema_term.get<expression::Attribute>();
                    if (schema_attribute.source().has_value())
                    {
                        attribute.source(schema_attribute.source().value());
                    }
                }
            }
        }
    }
}