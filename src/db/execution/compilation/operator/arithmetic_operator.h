#pragma once

#include "operator_interface.h"
#include <db/expression/operation.h>
#include <fmt/core.h>
#include <memory>
#include <vector>

namespace db::execution::compilation {
class ArithmeticOperator final : public UnaryOperator
{
public:
    ArithmeticOperator(topology::PhysicalSchema &&schema,
                       std::vector<std::unique_ptr<expression::Operation>> &&arithmetic_operations) noexcept
        : _schema(std::move(schema)), _arithmetics(std::move(arithmetic_operations))
    {
    }

    ~ArithmeticOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        auto child_finalization = this->child()->finalization_data();
        _is_child_has_finalization_pass = child_finalization.has_value();

        return child_finalization;
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(const GenerationPhase phase) override
    {
        return this->child()->output_provider(phase);
    }

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return child()->dependencies();
    }

    [[nodiscard]] std::string to_string() const override { return child()->to_string(); }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    topology::PhysicalSchema _schema;

    std::vector<std::unique_ptr<expression::Operation>> _arithmetics;

    bool _is_child_has_finalization_pass{false};

    /**
     * Ascertains if the operator should emit code for the arithmetic operator
     * depending on the current phase and the needs of the child.
     *
     * @param phase Current phase.
     * @return True, if we are in execution phase and have no finalization pass OR
     *               if we are in finalization phase and have a finalization pass.
     */
    [[nodiscard]] bool is_emit_arithmetic(const GenerationPhase phase) const noexcept
    {
        return (phase == GenerationPhase::execution && _is_child_has_finalization_pass == false) ||
               (phase == GenerationPhase::finalization && _is_child_has_finalization_pass);
    }
};

class ArithmeticComparator
{
public:
    ArithmeticComparator() noexcept = default;
    ~ArithmeticComparator() noexcept = default;

    bool operator()(const std::unique_ptr<expression::Operation> &left,
                    const std::unique_ptr<expression::Operation> & /*right*/) const
    {
        if (has_branch(left) == false)
        {
            return true;
        }

        return false;
    }

private:
    [[nodiscard]] static bool has_branch(const std::unique_ptr<expression::Operation> &operation)
    {
        if (operation->is_nullary())
        {
            return false;
        }

        if (operation->is_case())
        {
            return true;
        }

        if (operation->is_unary())
        {
            return has_branch(reinterpret_cast<expression::UnaryOperation *>(operation.get())->child());
        }

        if (operation->is_binary())
        {
            auto *binary_operation = reinterpret_cast<expression::BinaryOperation *>(operation.get());
            return has_branch(binary_operation->left_child()) || has_branch(binary_operation->right_child());
        }

        if (operation->is_list())
        {
            auto *list_operation = reinterpret_cast<expression::ListOperation *>(operation.get());
            for (const auto &child : list_operation->children())
            {
                if (has_branch(child))
                {
                    return true;
                }
            }
        }

        return false;
    }
};
} // namespace db::execution::compilation