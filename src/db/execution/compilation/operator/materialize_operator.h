#pragma once

#include "operator_interface.h"
#include <db/execution/compilation/flounder_record_set_emitter.h>
#include <fmt/core.h>
#include <vector>

namespace db::execution::compilation {
class TileOutputProvider final : public OutputProviderInterface
{
public:
    TileOutputProvider(const std::uint16_t count_workers, topology::PhysicalSchema &&schema)
        : _schema(std::move(schema))
    {
        _emitter.resize(count_workers);
    }

    ~TileOutputProvider() override = default;

    std::uintptr_t get(const std::uint16_t worker_id,
                       std::optional<std::reference_wrapper<const RecordToken>> /*token*/,
                       mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &graph,
                       mx::tasking::dataflow::NodeInterface<execution::RecordSet> *node) override
    {
        if (_emitter[worker_id] == nullptr)
        {
            _emitter[worker_id] = std::make_unique<MaterializeEmitter>(worker_id, _schema, graph, node);
        }

        return std::uintptr_t(_emitter[worker_id].get());
    }

private:
    const topology::PhysicalSchema _schema;
    std::vector<std::unique_ptr<MaterializeEmitter>> _emitter;
};

class MaterializeOperator : public UnaryOperator
{
public:
    MaterializeOperator(topology::PhysicalSchema &&schema) : _schema(std::move(schema)) {}
    ~MaterializeOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        auto child_data = this->child()->finalization_data();
        _is_child_has_finalization_pass = child_data.has_value();

        return child_data;
    }

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return child()->dependencies();
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) override;

    [[nodiscard]] std::string to_string() const override { return child()->to_string(); }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    const topology::PhysicalSchema _schema;

    std::optional<flounder::Register> _emitter_vreg{std::nullopt};
    std::optional<flounder::Register> _tile_out_vreg{std::nullopt};
    std::optional<flounder::Register> _tile_out_size_vreg{std::nullopt};

    bool _is_child_has_finalization_pass{false};

    /**
     * Ascertains if the operator should emit materialization
     * depending on the current phase and the needs of the child.
     *
     * @param phase Current phase.
     * @return True, if we are in execution phase and have no finalization pass OR
     *               if we are in finalization phase and have a finalization pass.
     */
    [[nodiscard]] bool is_materialize(const GenerationPhase phase) const noexcept
    {
        return (phase == GenerationPhase::execution && _is_child_has_finalization_pass == false) ||
               (phase == GenerationPhase::finalization && _is_child_has_finalization_pass);
    }
};
} // namespace db::execution::compilation