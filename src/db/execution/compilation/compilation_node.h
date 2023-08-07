#pragma once

#include "profile_guided_optimizer.h"
#include "program.h"
#include <db/execution/operator_interface.h>
#include <db/execution/record_token.h>
#include <db/execution/scan_generator.h>
#include <db/util/chronometer.h>
#include <flounder/compilation/compiler.h>
#include <memory>
#include <mx/tasking/dataflow/node.h>
#include <mx/tasking/dataflow/task_node.h>
#include <perf/counter.h>
#include <perf/sample.h>
#include <utility>
#include <vector>

namespace db::execution::compilation {
class CompilationNode : public execution::OperatorInterface
{
public:
    CompilationNode(std::string &&name, topology::PhysicalSchema &&schema, flounder::Program &&consume_program,
                    std::unique_ptr<OutputProviderInterface> &&consume_output_provider,
                    std::optional<flounder::Program> &&finalize_program,
                    std::unique_ptr<OutputProviderInterface> &&finalize_output_provider,
                    std::optional<flounder::Program> &&prefetching_program, std::uint8_t count_prefetches,
                    std::shared_ptr<util::Chronometer> &&chronometer,
                    std::unordered_map<std::string, std::string> &&information) noexcept;

    ~CompilationNode() noexcept override = default;

    [[nodiscard]] const topology::PhysicalSchema &schema() const noexcept override { return _schema; }

    /**
     * @return Pair of strings for (1) the consuming flounder code and
     *  (2, if given) the finalization flounder code.
     */
    [[nodiscard]] std::tuple<std::vector<std::string>, std::optional<std::vector<std::string>>,
                             std::optional<std::vector<std::string>>>
    flounder_code() const noexcept
    {
        return std::make_tuple(
            _consume_program.flounder().code(),
            _finalize_program.has_value() ? std::make_optional(_finalize_program->flounder().code()) : std::nullopt,
            _prefetch_program.has_value() ? std::make_optional(_prefetch_program->flounder().code()) : std::nullopt);
    }

    /**
     * @return Pair of strings for (1) the consuming assembly code and
     *  (2, if given) the finalization assemnly code.
     */
    [[nodiscard]] std::tuple<std::optional<std::vector<std::string>>, std::optional<std::vector<std::string>>,
                             std::optional<std::vector<std::string>>>
    assembly_code() const noexcept
    {
        auto consume_code = std::optional<std::vector<std::string>>{std::nullopt};
        auto finalize_code = std::optional<std::vector<std::string>>{std::nullopt};
        auto prefetching_code = std::optional<std::vector<std::string>>{std::nullopt};

        const auto &consume_compilate = _consume_program.executable().compilate();
        if (consume_compilate.has_code())
        {
            consume_code = consume_compilate.code();
        }

        if (_finalize_program.has_value())
        {
            const auto &finalize_compilate = _finalize_program->executable().compilate();
            if (finalize_compilate.has_code())
            {
                finalize_code = finalize_compilate.code();
            }
        }

        if (_prefetch_program.has_value())
        {
            const auto &prefetching_compilate = _prefetch_program->executable().compilate();
            if (prefetching_compilate.has_code())
            {
                prefetching_code = prefetching_compilate.code();
            }
        }

        return std::make_tuple(std::move(consume_code), std::move(finalize_code), std::move(prefetching_code));
    }

    [[nodiscard]] std::tuple<std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>,
                             std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>,
                             std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>>
    assembly_code(const perf::AggregatedSamples &samples) const noexcept
    {
        auto consume_code = std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>{std::nullopt};
        auto finalize_code = std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>{std::nullopt};
        auto prefetching_code = std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>{std::nullopt};

        const auto &consume_compilate = _consume_program.executable().compilate();
        if (consume_compilate.has_code())
        {
            consume_code = consume_compilate.code(samples);
        }

        if (_finalize_program.has_value())
        {
            const auto &finalize_compilate = _finalize_program->executable().compilate();
            if (finalize_compilate.has_code())
            {
                finalize_code = finalize_compilate.code(samples);
            }
        }

        if (_prefetch_program.has_value())
        {
            const auto &prefetching_compilate = _prefetch_program->executable().compilate();
            if (prefetching_compilate.has_code())
            {
                prefetching_code = prefetching_compilate.code(samples);
            }
        }

        return std::make_tuple(std::move(consume_code), std::move(finalize_code), std::move(prefetching_code));
    }

    [[nodiscard]] std::tuple<std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>,
                             std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>,
                             std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>>
    contexts(const perf::AggregatedSamples &samples) const noexcept
    {
        auto consume_contexts = std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>{std::nullopt};
        auto finalize_contexts =
            std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>{std::nullopt};
        auto prefetching_contexts =
            std::optional<std::vector<std::tuple<std::uint64_t, float, std::string>>>{std::nullopt};

        const auto &consume_compilate = _consume_program.executable().compilate();
        if (consume_compilate.has_contexts())
        {
            consume_contexts = consume_compilate.contexts(samples);
        }

        if (_finalize_program.has_value())
        {
            const auto &finalize_compilate = _finalize_program->executable().compilate();
            if (finalize_compilate.has_contexts())
            {
                finalize_contexts = finalize_compilate.contexts(samples);
            }
        }

        if (_prefetch_program.has_value())
        {
            const auto &prefetching_compilate = _prefetch_program->executable().compilate();
            if (prefetching_compilate.has_contexts())
            {
                prefetching_contexts = prefetching_compilate.contexts(samples);
            }
        }

        return std::make_tuple(std::move(consume_contexts), std::move(finalize_contexts),
                               std::move(prefetching_contexts));
    }

    /**
     * Compiles the given programs for consuming records and finalizing the node into assembly.
     *
     * @param compiler Compiler to compile the consume and apply_best_version programs.
     * @return True, if the code does compile successfully.
     */
    [[nodiscard]] bool compile(flounder::Compiler &compiler)
    {
        if (_consume_program.compile(compiler) == false)
        {
            return false;
        }

        if (_finalize_program.has_value() && _finalize_program->compile(compiler) == false)
        {
            return false;
        }

        if (_prefetch_program.has_value())
        {
            if (_prefetch_program->compile(compiler) == false)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * @return Name of this node as a function (i.e., using underscore instead of space).
     */
    [[nodiscard]] std::string name() const;

    [[nodiscard]] Program &consume_program() noexcept { return _consume_program; }
    [[nodiscard]] std::optional<Program> &finalize_program() noexcept { return _finalize_program; }
    [[nodiscard]] std::optional<Program> &prefetch_program() noexcept { return _prefetch_program; }

    void consume(std::uint16_t worker_id, mx::tasking::dataflow::NodeInterface<RecordSet> *node,
                 mx::tasking::dataflow::EmitterInterface<RecordSet> &emitter,
                 mx::tasking::dataflow::Token<RecordSet> &data);

    void finalize(std::uint16_t worker_id, mx::tasking::dataflow::NodeInterface<RecordSet> *node,
                  mx::tasking::dataflow::EmitterInterface<RecordSet> &emitter, bool is_last, mx::resource::ptr data,
                  mx::resource::ptr reduced_data);

    [[nodiscard]] std::uint8_t count_prefetches() const noexcept { return _count_prefetches; }

    [[nodiscard]] std::optional<std::uintptr_t> prefetch_callback() const noexcept
    {
        if (_prefetch_program.has_value() == false)
        {
            return std::nullopt;
        }

        return _prefetch_program->callback();
    }

    [[nodiscard]] const std::unordered_map<std::string, std::string> &information() const noexcept
    {
        return _information;
    }

protected:
    /// Name of this node, compound by names of nested, compiled operators.
    /// Will be used when visualizing the dataflow graph and showing code.
    const std::string _name;

    /// Outgoing schema of this operator.
    const topology::PhysicalSchema _schema;

    /// Code consuming (and emitting) records.
    Program _consume_program;

    /// Code called when the node finished its work, will be operator-depending.
    /// Some operators do not need the finalization step.
    std::optional<Program> _finalize_program{std::nullopt};

    /// Program that initiates prefetches.
    std::optional<Program> _prefetch_program{std::nullopt};

    std::uint8_t _count_prefetches;

    /// Optimizer that measures and recompiles the executable.
    /// TODO: Use for adaptive recompilation.
    //    ProfileGuidedOptimizer _optimizer;

    /// Chronometer to log finalization of nodes. May be nullptr if
    /// logging is not desired.
    std::shared_ptr<util::Chronometer> _chronometer;

    /// Information emitted by fusioned operators; used for debugging
    /// purposes in data flow graph.
    std::unordered_map<std::string, std::string> _information;
};

class ProducingNode final : public mx::tasking::dataflow::ProducingNodeInterface<RecordSet>, public CompilationNode
{
public:
    ProducingNode(std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> &&data_generator,
                  topology::PhysicalSchema &&schema, std::string &&name, flounder::Program &&produce_program,
                  std::unique_ptr<OutputProviderInterface> &&execution_output_provider,
                  std::optional<flounder::Program> &&finalize_program,
                  std::unique_ptr<OutputProviderInterface> &&finalization_output_provider,
                  std::optional<flounder::Program> &&prefetching_program, const std::uint8_t count_prefetches,
                  std::shared_ptr<util::Chronometer> chronometer,
                  std::unordered_map<std::string, std::string> &&information) noexcept
        : CompilationNode(std::move(name), std::move(schema), std::move(produce_program),
                          std::move(execution_output_provider), std::move(finalize_program),
                          std::move(finalization_output_provider), std::move(prefetching_program), count_prefetches,
                          std::move(chronometer), std::move(information))
    {
        mx::tasking::dataflow::NodeInterface<RecordSet>::annotation().produces(std::move(data_generator));
        mx::tasking::dataflow::NodeInterface<RecordSet>::annotation().is_parallel(true);
    }

    ~ProducingNode() noexcept override = default;

    [[nodiscard]] std::string to_string() const noexcept override { return _name; }

    void consume(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                 RecordToken &&data) override
    {
        this->CompilationNode::consume(worker_id, this, graph, data);
    }

    void finalize(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                  const bool is_last, const mx::resource::ptr data, const mx::resource::ptr reduced_data) override
    {
        this->CompilationNode::finalize(worker_id, this, graph, is_last, data, reduced_data);
    }

    [[nodiscard]] std::uint64_t trace_id() const noexcept override { return std::uint64_t(this); }

    [[nodiscard]] ScanGenerator *scan_generator() noexcept
    {
        return reinterpret_cast<ScanGenerator *>(
            mx::tasking::dataflow::NodeInterface<RecordSet>::annotation().token_generator().get());
    }
};

class ConsumingTask final : public mx::tasking::dataflow::DataTaskInterface<RecordSet>
{
public:
    void execute(std::uint16_t worker_id, mx::tasking::dataflow::NodeInterface<RecordSet> *node,
                 mx::tasking::dataflow::EmitterInterface<RecordSet> &emitter,
                 mx::tasking::dataflow::Token<RecordSet> &&data) override;
};

class ConsumingNode final : public mx::tasking::dataflow::TaskNode<ConsumingTask>, public CompilationNode
{
public:
    ConsumingNode(topology::PhysicalSchema &&schema, std::string &&name, flounder::Program &&consume_program,
                  std::unique_ptr<OutputProviderInterface> &&execution_output_provider,
                  std::optional<flounder::Program> &&finalize_program,
                  std::unique_ptr<OutputProviderInterface> &&finalization_output_provider,
                  std::optional<flounder::Program> &&prefetching_program, const std::uint8_t count_prefetches,
                  std::shared_ptr<util::Chronometer> chronometer,
                  std::unordered_map<std::string, std::string> &&information) noexcept
        : CompilationNode(std::move(name), std::move(schema), std::move(consume_program),
                          std::move(execution_output_provider), std::move(finalize_program),
                          std::move(finalization_output_provider), std::move(prefetching_program), count_prefetches,
                          std::move(chronometer), std::move(information))
    {
    }

    ~ConsumingNode() noexcept override = default;

    [[nodiscard]] std::string to_string() const noexcept override { return _name; }

    void finalize(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                  const bool is_last, const mx::resource::ptr data, const mx::resource::ptr reduced_data) override
    {
        this->CompilationNode::finalize(worker_id, this, graph, is_last, data, reduced_data);
    }

    [[nodiscard]] std::uint64_t trace_id() const noexcept override { return std::uint64_t(this); }
};

} // namespace db::execution::compilation