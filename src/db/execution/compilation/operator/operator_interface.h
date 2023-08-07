#pragma once

#include <db/execution/compilation/context.h>
#include <db/execution/operator_interface.h>
#include <ecpp/static_vector.hpp>
#include <flounder/program.h>
#include <functional>
#include <memory>
#include <mx/tasking/annotation.h>
#include <mx/tasking/dataflow/token_generator.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace db::execution::compilation {

class OperatorInterface;
class OperatorProgramContext
{
public:
    OperatorProgramContext() = default;
    explicit OperatorProgramContext(OperatorInterface *subsequent_operator) : _subsequent_operator(subsequent_operator)
    {
    }
    OperatorProgramContext(OperatorInterface *subsequent_operator, OperatorInterface *depending_operator)
        : _subsequent_operator(subsequent_operator)
    {
        _dependent_operators.emplace_back(depending_operator);
    }
    OperatorProgramContext(OperatorProgramContext &&) noexcept = default;
    ~OperatorProgramContext() = default;

    OperatorProgramContext &operator=(OperatorProgramContext &&) noexcept = default;

    [[nodiscard]] OperatorInterface *subsequent_operator() const noexcept { return _subsequent_operator; }
    [[nodiscard]] const ecpp::static_vector<OperatorInterface *, 4U> &dependent_operators() const noexcept
    {
        return _dependent_operators;
    }

    void subsequent_operator(OperatorInterface *compilation_operator) noexcept
    {
        _subsequent_operator = compilation_operator;
    }
    void add_dependent_operator(OperatorInterface *compilation_operator)
    {
        _dependent_operators.emplace_back(compilation_operator);
    }

private:
    /// Pointer to a stand-alone compiled program that is executed
    /// as a new operator in the execution graph.
    OperatorInterface *_subsequent_operator{nullptr};

    /// List of stand-alone compiled operators that have to finish
    /// before the operator with the given context starts executing.
    ecpp::static_vector<OperatorInterface *, 4U> _dependent_operators;
};

class OperatorInterface : public execution::OperatorInterface
{
public:
    enum Finalization
    {
        single,
        parallel,
        reduce
    };

    /**
     * Every operate may generate code for two separate phases:
     * The execution (scanning tuples from relation, filtering, aggregate into local results, ...)
     * and finalization phase (merge aggregations, ...).
     */
    enum GenerationPhase : std::uint8_t
    {
        execution,
        finalization,
        prefetching
    };

    constexpr OperatorInterface() noexcept = default;
    ~OperatorInterface() noexcept override = default;

    /**
     * Produces flounder code for producing or consuming records during execution.
     *
     * @param phase Phase of generation.
     * @param program Program to emit flounder code.
     * @param context Context of the flounder code, holds tuple iterator etc..
     */
    virtual void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) = 0;

    /**
     * Consumes flounder code during code generation.
     *
     * @param phase Phase of genneration.
     * @param program Program to emit flounder code.
     * @param context Context of the flounder code, holds tuple iterator etc..
     */
    virtual void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) = 0;

    /**
     * Insert requests for the required symbols accessed by this operator.
     *
     * @param phase Phase of genneration.
     * @param symbols Symbol set to place requests.
     */
    virtual void request_symbols(GenerationPhase phase, SymbolSet &symbols) = 0;

    /**
     * Generates data and finalization type that is used for finalization.
     * Operators, that do not need finalization may return nullopt.
     *
     * @return Type and data used for finalization.
     */
    [[nodiscard]] virtual std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept = 0;

    /**
     * @return The completion callback for a compiled node.
     */
    [[nodiscard]] virtual std::unique_ptr<mx::tasking::dataflow::annotation<RecordSet>::CompletionCallbackInterface>
    completion_callback() = 0;

    /**
     * Creates the data that is consumed by this operator
     * and has to be emitted into the graph.
     *
     * @return Data that will be consumed by this operator initially.
     */
    [[nodiscard]] virtual std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const = 0;

    /**
     * @return The resource boundness of this operator.
     */
    [[nodiscard]] virtual enum mx::tasking::annotation::resource_boundness resource_boundness() const noexcept = 0;

    /**
     * @return True, if this node finalizes the pipeline preamture.
     */
    [[nodiscard]] virtual bool is_finalize_pipeline_premature() const noexcept = 0;

    /**
     * Creates an output provider that handles the data
     * generated by the operator.
     *
     * @param phase Phase of the generation pass.
     * @return Output provider that handles the generated data.
     */
    [[nodiscard]] virtual std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) = 0;

    /**
     * @return Dependencies and pipelinebreakers.
     */
    [[nodiscard]] virtual std::optional<OperatorProgramContext> dependencies() const = 0;

    /**
     * @return The number of prefetching cache lines.
     */
    [[nodiscard]] virtual std::uint8_t count_prefeches() const = 0;

    /**
     * @return Label of the produced operator, shown for debugging or explanation reasons.
     */
    [[nodiscard]] virtual std::string to_string() const = 0;

    /**
     * @return Parent of this operator.
     */
    [[nodiscard]] OperatorInterface *parent() const noexcept { return _parent; }

    /**
     * Updates the parent operator.
     * @param parent New parent operator.
     */
    void parent(OperatorInterface *parent) noexcept { _parent = parent; }

    /**
     * Since multiple operators may named the same,
     * every pipeline brings it's own identifier
     * (i.e., the source table name).
     *
     * @return Identifier of the pipeline.
     */
    [[nodiscard]] virtual std::string pipeline_identifier() const = 0;

    /**
     * Emit information for the dataflow graph for a specific operator.
     * Information may be the size of a hash table, the count of radix bits/partitions etc.
     *
     * @param container Container to emit information.
     */
    virtual void emit_information(std::unordered_map<std::string, std::string> &container) = 0;

    /**
     * Emit memory tags for memory tracing.
     *
     * @param container
     */
    virtual void emit_memory_tags(
        std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>> &container) const = 0;

private:
    OperatorInterface *_parent{nullptr};
};

class UnaryOperator : public OperatorInterface
{
public:
    constexpr UnaryOperator() noexcept = default;
    ~UnaryOperator() noexcept override = default;

    /**
     * @return Child of this operator.
     */
    [[nodiscard]] const std::unique_ptr<OperatorInterface> &child() const noexcept { return _child; }

    /**
     * Updates the child of this operator and the parent of the given child.
     *
     * @param child Child to update.
     */
    void child(std::unique_ptr<OperatorInterface> &&child) noexcept
    {
        _child = std::move(child);
        _child->parent(this);
    }

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override
    {
        return _child->finalization_data();
    }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::annotation<RecordSet>::CompletionCallbackInterface>
    completion_callback() override
    {
        return _child->completion_callback();
    }

    [[nodiscard]] enum mx::tasking::annotation::resource_boundness resource_boundness() const noexcept override
    {
        return _child->resource_boundness();
    }

    [[nodiscard]] bool is_finalize_pipeline_premature() const noexcept override
    {
        return _child->is_finalize_pipeline_premature();
    }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const override
    {
        if (_child != nullptr) [[likely]]
        {
            return _child->input_data_generator();
        }

        return nullptr;
    }

    [[nodiscard]] std::uint8_t count_prefeches() const override
    {
        if (_child != nullptr) [[likely]]
        {
            return _child->count_prefeches();
        }

        return 0U;
    }

    [[nodiscard]] std::string pipeline_identifier() const override { return _child->pipeline_identifier(); }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        if (_child != nullptr)
        {
            _child->emit_information(container);
        }
    }

    void emit_memory_tags(std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>
                              &container) const override
    {
        if (_child != nullptr)
        {
            _child->emit_memory_tags(container);
        }
    }

private:
    std::unique_ptr<OperatorInterface> _child;
};

class BinaryOperator : public OperatorInterface
{
public:
    constexpr BinaryOperator() noexcept = default;
    ~BinaryOperator() noexcept override = default;

    [[nodiscard]] const std::unique_ptr<OperatorInterface> &left_child() const noexcept { return _left_child; }
    [[nodiscard]] const std::unique_ptr<OperatorInterface> &right_child() const noexcept { return _right_child; }
    void left_child(std::unique_ptr<OperatorInterface> &&child) noexcept
    {
        _left_child = std::move(child);
        _left_child->parent(this);
    }
    void right_child(std::unique_ptr<OperatorInterface> &&child) noexcept
    {
        _right_child = std::move(child);
        _right_child->parent(this);
    }

    [[nodiscard]] enum mx::tasking::annotation::resource_boundness resource_boundness() const noexcept override
    {
        return _left_child->resource_boundness();
    }

    [[nodiscard]] bool is_finalize_pipeline_premature() const noexcept override
    {
        return _left_child->is_finalize_pipeline_premature();
    }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::annotation<RecordSet>::CompletionCallbackInterface>
    completion_callback() override
    {
        return _left_child->completion_callback();
    }

    void emit_memory_tags(std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>
                              &container) const override
    {
        if (_left_child != nullptr)
        {
            _left_child->emit_memory_tags(container);
        }

        if (_right_child != nullptr)
        {
            _right_child->emit_memory_tags(container);
        }
    }

private:
    std::unique_ptr<OperatorInterface> _left_child;
    std::unique_ptr<OperatorInterface> _right_child;
};
} // namespace db::execution::compilation