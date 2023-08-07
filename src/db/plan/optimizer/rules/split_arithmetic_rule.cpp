#include "split_arithmetic_rule.h"
#include <db/plan/logical/node/arithmetic_node.h>
#include <unordered_set>
#include <vector>

using namespace db::plan::optimizer;

bool SplitArithmeticRule::apply(PlanView &plan)
{
    for (auto [node, _] : plan.nodes_and_parent())
    {
        if (typeid(*node) == typeid(logical::ArithmeticNode))
        {
            auto *arithmetic_node = reinterpret_cast<logical::ArithmeticNode *>(node);
            for (const auto &operation : arithmetic_node->arithmetic_operations())
            {
                auto sources = SplitArithmeticRule::extract_sources(operation);
                if (sources.size() > 1U)
                {
                    /// A map that maps from source to the list of operations.
                    /// These individual arithmetics can be pushed down.
                    auto source_map = std::unordered_map<expression::Attribute::Source,
                                                         std::vector<std::unique_ptr<expression::Operation>>>{};
                    SplitArithmeticRule::extract_operation_by_source(operation, source_map);

                    /// The terms that are now part of another arithmetic.
                    /// We will replace the operations in the current arithmetic node
                    /// by attributes.
                    auto split_off_terms = std::unordered_map<expression::Term, bool>{};

                    /// Create a new arithmetic node for each source.
                    for (auto &[key, operations] : source_map)
                    {
                        for (const auto &split_off_operation : operations)
                        {
                            split_off_terms.insert(std::make_pair(split_off_operation->result().value(),
                                                                  split_off_operation->is_comparison()));
                        }

                        auto *split_off_arithmetic_node =
                            plan.make_node<logical::ArithmeticNode>(std::move(operations));
                        plan.insert_between(arithmetic_node, std::get<0>(plan.children(arithmetic_node)),
                                            split_off_arithmetic_node);
                    }

                    /// Replace the operations in the current arithmetic node by terms.
                    auto replaced_operations = std::vector<std::unique_ptr<expression::Operation>>{};
                    replaced_operations.reserve(arithmetic_node->arithmetic_operations().size());
                    for (const auto &replacing_operation : arithmetic_node->arithmetic_operations())
                    {
                        if (split_off_terms.contains(replacing_operation->result().value()) == false)
                        {
                            auto new_operation = replacing_operation->copy();
                            SplitArithmeticRule::replace_operations_by_terms(new_operation, split_off_terms);
                            if (new_operation->is_nullary() == false)
                            {
                                replaced_operations.emplace_back(std::move(new_operation));
                            }
                        }
                    }
                    arithmetic_node->arithmetic_operations() = std::move(replaced_operations);

                    return true;
                }
            }
        }
    }

    return false;
}

std::unordered_set<db::expression::Attribute::Source> SplitArithmeticRule::extract_sources(
    const std::unique_ptr<expression::Operation> &operation)
{
    auto sources = std::unordered_set<expression::Attribute::Source>{};
    expression::for_each_attribute(operation, [&sources](const expression::Attribute &attr) {
        if (attr.source().has_value())
        {
            sources.insert(attr.source().value());
        }
    });
    return sources;
}

void SplitArithmeticRule::extract_operation_by_source(
    const std::unique_ptr<expression::Operation> &operation,
    std::unordered_map<db::expression::Attribute::Source, std::vector<std::unique_ptr<db::expression::Operation>>>
        &source_map)
{
    auto sources = SplitArithmeticRule::extract_sources(operation);
    if (sources.empty())
    {
        return;
    }

    if (sources.size() == 1U)
    {
        for (const auto &source : sources)
        {
            if (auto iterator = source_map.find(source); iterator != source_map.end())
            {
                iterator->second.emplace_back(operation->copy());
            }
            else
            {
                auto operations = std::vector<std::unique_ptr<expression::Operation>>{};
                operations.emplace_back(operation->copy());
                source_map.insert(std::make_pair(source, std::move(operations)));
            }
        }

        return;
    }

    if (operation->is_binary())
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());
        SplitArithmeticRule::extract_operation_by_source(binary_operation->left_child(), source_map);
        SplitArithmeticRule::extract_operation_by_source(binary_operation->right_child(), source_map);
    }
    else if (operation->is_list())
    {
        auto *list_operation = reinterpret_cast<expression::ListOperation *>(operation.get());
        for (const auto &child : list_operation->children())
        {
            SplitArithmeticRule::extract_operation_by_source(child, source_map);
        }
    }
    else if (operation->is_unary())
    {
        SplitArithmeticRule::extract_operation_by_source(
            reinterpret_cast<expression::UnaryOperation *>(operation.get())->child(), source_map);
    }
}

void SplitArithmeticRule::replace_operations_by_terms(std::unique_ptr<expression::Operation> &operation,
                                                      const std::unordered_map<expression::Term, bool> &split_off_terms)
{
    if (operation->is_nullary())
    {
        return;
    }

    if (const auto split_off_term_iterator = split_off_terms.find(operation->result().value());
        split_off_term_iterator != split_off_terms.end())
    {
        operation = std::make_unique<expression::NullaryOperation>(expression::Term{operation->result().value()});

        const auto was_comparison = split_off_term_iterator->second;
        if (was_comparison)
        {
            operation =
                std::make_unique<expression::UnaryOperation>(expression::Operation::Id::IsTrue, std::move(operation));
        }
    }
    else if (operation->is_unary())
    {
        SplitArithmeticRule::replace_operations_by_terms(
            reinterpret_cast<expression::UnaryOperation *>(operation.get())->child(), split_off_terms);
    }
    else if (operation->is_binary())
    {
        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());
        SplitArithmeticRule::replace_operations_by_terms(binary_operation->left_child(), split_off_terms);
        SplitArithmeticRule::replace_operations_by_terms(binary_operation->right_child(), split_off_terms);
    }
    else if (operation->is_list())
    {
        auto *list_operation = reinterpret_cast<expression::ListOperation *>(operation.get());
        for (auto &child : list_operation->children())
        {
            SplitArithmeticRule::replace_operations_by_terms(child, split_off_terms);
        }
    }
}