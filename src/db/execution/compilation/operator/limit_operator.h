#pragma once

#include "operator_interface.h"
#include <db/expression/limit.h>
#include <fmt/core.h>

namespace db::execution::compilation {
class LimitOperator final : public UnaryOperator
{
public:
    LimitOperator(topology::PhysicalSchema &&schema, expression::Limit limit) noexcept
        : _schema(std::move(schema)), _limit(limit)
    {
    }

    ~LimitOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;

    void request_symbols(const GenerationPhase phase, SymbolSet &symbols) override
    {
        this->child()->request_symbols(phase, symbols);
    }

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        auto child_data = this->child()->finalization_data();
        _is_child_has_finalization_pass = child_data.has_value();

        return child_data;
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
    expression::Limit _limit;
    std::optional<flounder::Register> _offset_address_vreg{std::nullopt};
    std::optional<flounder::Register> _limit_address_vreg{std::nullopt};

    bool _is_child_has_finalization_pass{false};

    /**
     * Ascertains if the operator should emit code for the limit operator
     * depending on the current phase and the needs of the child.
     *
     * @param phase Current phase.
     * @return True, if we are in execution phase and have no finalization pass OR
     *               if we are in finalization phase and have a finalization pass.
     */
    [[nodiscard]] bool is_use_limit(const GenerationPhase phase) const noexcept
    {
        return (phase == GenerationPhase::execution && _is_child_has_finalization_pass == false) ||
               (phase == GenerationPhase::finalization && _is_child_has_finalization_pass);
    }
};
} // namespace db::execution::compilation