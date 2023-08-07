#pragma once

#include "operator_interface.h"
#include <db/execution/compilation/record_token.h>
#include <db/expression/term.h>
#include <fmt/core.h>

namespace db::execution::compilation {

class UserDefinedOperator final : public UnaryOperator
{
public:
    UserDefinedOperator(
        topology::PhysicalSchema &&schema,
        std::vector<std::unique_ptr<expression::UserDefinedFunctionOperation>> &&user_defined_functions) noexcept
        : _schema(std::move(schema)), _user_defined_functions(std::move(user_defined_functions))
    {
    }

    ~UserDefinedOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase /*phase*/, flounder::Program & /*program*/, CompilationContext & /*context*/) override
    {
        /// Since this operator consumes tuples from emitted tiles, it will call the parents consume(),
        /// but its own consume will never get called (like scan).
    }

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        return std::nullopt;
    }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const override
    {
        /// Since the probe will consume data from the graph,
        /// we do not need to commit any data.
        return nullptr;
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(const GenerationPhase /*phase*/) override
    {
        return nullptr;
    }

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return std::make_optional(OperatorProgramContext{this->child().get()});
    }

    [[nodiscard]] std::string to_string() const override { return "User Defined"; }

    [[nodiscard]] std::string pipeline_identifier() const override { return this->to_string(); }

    void emit_information(std::unordered_map<std::string, std::string> & /*container*/) override {}

    [[nodiscard]] enum mx::tasking::annotation::resource_boundness resource_boundness() const noexcept override
    {
        const auto has_compute_bound_udf =
            std::find_if(_user_defined_functions.begin(), _user_defined_functions.end(), [](const auto &udf) {
                return udf->descriptor()->get().is_compute_bound();
            }) != _user_defined_functions.end();

        return has_compute_bound_udf ? mx::tasking::annotation::resource_boundness::compute
                                     : mx::tasking::annotation::resource_boundness::mixed;
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    /// Schema produced by the probe,
    topology::PhysicalSchema _schema;

    /// User defined functions
    const std::vector<std::unique_ptr<expression::UserDefinedFunctionOperation>> _user_defined_functions;
};
} // namespace db::execution::compilation