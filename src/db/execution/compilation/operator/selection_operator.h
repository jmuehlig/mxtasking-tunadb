#pragma once

#include "operator_interface.h"
#include <db/expression/operation.h>
#include <fmt/core.h>

namespace db::execution::compilation {
class SelectionOperator final : public UnaryOperator
{
public:
    SelectionOperator(topology::PhysicalSchema &&schema, std::unique_ptr<expression::Operation> &&predicate) noexcept
        : _schema(std::move(schema)), _predicate(std::move(predicate))
    {
    }

    ~SelectionOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return this->child()->dependencies();
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(const GenerationPhase /*phase*/) override
    {
        return nullptr;
    }

    [[nodiscard]] std::string to_string() const override { return child()->to_string(); }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    topology::PhysicalSchema _schema;
    std::unique_ptr<expression::Operation> _predicate;
    std::vector<expression::Term> _required_terms;
};
} // namespace db::execution::compilation