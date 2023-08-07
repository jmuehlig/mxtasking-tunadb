#pragma once

#include "symbol_set.h"
#include <db/exception/execution_exception.h>
#include <db/expression/operation.h>
#include <db/expression/term.h>
#include <db/topology/physical_schema.h>
#include <db/util/string.h>
#include <flounder/program.h>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace db::execution::compilation {
/**
 * TODO: Description
 */
class ExpressionSet
{
public:
    explicit ExpressionSet(SymbolSet &symbol_set) : _symbol_set(symbol_set) {}
    ~ExpressionSet() = default;

    void request(const std::unique_ptr<expression::Operation> &operation)
    {
        const auto &identifier = operation->result().value();

        if (operation->is_nullary() && operation->result()->is_attribute())
        {
            _symbol_set.request(identifier);
        }
        else
        {
            if (auto iterator = _requested_expressions.find(identifier); iterator != _requested_expressions.end())
            {
                iterator->second += 1U;
            }
            else
            {
                _requested_expressions.insert(std::make_pair(identifier, 1U));

                if (operation->is_unary())
                {
                    request(reinterpret_cast<expression::UnaryOperation *>(operation.get())->child());
                }
                else if (operation->is_binary())
                {
                    auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());
                    request(binary_operation->left_child());
                    request(binary_operation->right_child());
                }
                else if (operation->is_list())
                {
                    auto *list_operation = reinterpret_cast<expression::ListOperation *>(operation.get());
                    for (const auto &child : list_operation->children())
                    {
                        request(child);
                    }
                }
            }
        }
    }

    void request(const std::vector<std::unique_ptr<expression::Operation>> &operations)
    {
        for (const auto &operation : operations)
        {
            request(operation);
        }
    }

    void release(flounder::Program &program, const std::unique_ptr<expression::Operation> &operation)
    {
        const auto &identifier = operation->result().value();

        if (operation->is_nullary() && operation->result()->is_attribute())
        {
            _symbol_set.release(program, identifier);
        }
        else
        {
            if (auto iterator = _computed_expressions.find(identifier); iterator != _computed_expressions.end())
            {
                auto &reference_counter = std::get<1>(iterator->second);
                if (--reference_counter == 0U)
                {
                    auto operand = std::get<0>(iterator->second);
                    if (operand.is_reg())
                    {
                        program << program.clear(operand.reg());
                    }
                    _computed_expressions.erase(iterator);

                    if (operation->is_unary())
                    {
                        release(program, reinterpret_cast<expression::UnaryOperation *>(operation.get())->child());
                    }
                    else if (operation->is_binary())
                    {
                        auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());
                        release(program, binary_operation->left_child());
                        release(program, binary_operation->right_child());
                    }
                    else if (operation->is_list())
                    {
                        auto *list_operation = reinterpret_cast<expression::ListOperation *>(operation.get());
                        for (const auto &child : list_operation->children())
                        {
                            release(program, child);
                        }
                    }
                }
            }
        }
    }

    void release(flounder::Program &program, const std::vector<std::unique_ptr<expression::Operation>> &operations)
    {
        for (const auto &operation : operations)
        {
            release(program, operation);
        }
    }

    [[nodiscard]] flounder::Operand get(const std::unique_ptr<expression::Operation> &operation) const
    {
        const auto &identifier = operation->result().value();

        if (operation->is_nullary() && operation->result()->is_attribute())
        {
            return flounder::Operand{_symbol_set.get(identifier)};
        }

        if (auto iterator = _computed_expressions.find(identifier); iterator != _computed_expressions.end())
        {
            return std::get<0>(iterator->second);
        }

        throw exception::ExpressionNotFoundException{identifier.to_string()};
    }

    [[nodiscard]] bool is_set(const std::unique_ptr<expression::Operation> &operation) const noexcept
    {
        const auto &identifier = operation->result().value();

        if (operation->is_nullary() && operation->result()->is_attribute())
        {
            return _symbol_set.is_set(identifier);
        }

        return _computed_expressions.find(identifier) != _computed_expressions.end();
    }

    void set(flounder::Program &program, const std::unique_ptr<expression::Operation> &operation,
             flounder::Operand operand)
    {
        const auto &identifier = operation->result().value();

        auto count_requests = 0U;

        if (auto iterator = _requested_expressions.find(identifier); iterator != _requested_expressions.end())
        {
            count_requests += iterator->second;

            /// Set the vreg for the requested expression.
            _requested_expressions.erase(iterator);
        }

        _computed_expressions.insert(std::make_pair(identifier, std::make_pair(operand, count_requests)));

        /// Release all child expressions since we finished computation.
        if (operation->is_unary())
        {
            release(program, reinterpret_cast<expression::UnaryOperation *>(operation.get())->child());
        }
        else if (operation->is_binary())
        {
            auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());
            release(program, binary_operation->left_child());
            release(program, binary_operation->right_child());
        }
        else if (operation->is_list())
        {
            auto *list_operation = reinterpret_cast<expression::ListOperation *>(operation.get());
            for (const auto &child : list_operation->children())
            {
                release(program, child);
            }
        }

        /// If the expression was requested as a symbol (i.e., to materialize), set also.
        if (operand.is_reg() && _symbol_set.is_requested(identifier))
        {
            _symbol_set.set(identifier, operand.reg());
        }
    }

    [[nodiscard]] std::uint32_t count_requests(const std::unique_ptr<expression::Operation> &operation) const noexcept
    {
        if (operation->is_nullary())
        {
            return _symbol_set.count_requests(operation);
        }
        else
        {
            if (auto compute_iterator = _computed_expressions.find(operation->result().value());
                compute_iterator != _computed_expressions.end())
            {
                return compute_iterator->second.second;
            }

            if (auto request_iterator = _requested_expressions.find(operation->result().value());
                request_iterator != _requested_expressions.end())
            {
                return request_iterator->second;
            }
        }

        return 0U;
    }

private:
    SymbolSet &_symbol_set;

    /// All terms of requested expressions and the number of requests.
    std::unordered_map<expression::Term, std::uint32_t, SymbolSet::TermHash> _requested_expressions;

    /// All terms of computed expressions and their virtual registers (and number of requests).
    std::unordered_map<expression::Term, std::pair<flounder::Operand, std::uint32_t>, SymbolSet::TermHash>
        _computed_expressions;
};
} // namespace db::execution::compilation