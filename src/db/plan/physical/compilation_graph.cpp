#include "compilation_graph.h"
#include <db/exception/execution_exception.h>
#include <db/execution/gather_result_node.h>
#include <db/execution/memory_tracing_node.h>
#include <flounder/jit_profiling_api.h>
#include <flounder/optimization/optimizer.h>
#include <mx/tasking/runtime.h>

using namespace db::plan::physical;

CompilationGraph *CompilationGraph::build(
    const topology::Database &database, std::shared_ptr<util::Chronometer> chronometer,
    CompilationPlan &&compilation_plan, const std::uint32_t client_id, const bool is_record_performance,
    const bool is_record_task_load, const bool is_record_task_traces, bool is_explain_flounder,
    const bool is_explain_assembly, const bool is_explain_dram_bandwidth, const bool is_explain_task_graph,
    const bool is_explain_data_flow_graph, const bool is_explain_times,
    const std::optional<
        std::tuple<logical::SampleNode::Level, logical::SampleNode::CounterType, std::optional<std::uint64_t>>>
        sample_type,
    const perf::Counter &profiling_counter)
{
    auto *graph =
        new CompilationGraph{sample_type.has_value(), is_explain_assembly || sample_type.has_value(), is_explain_times};
    graph->add(std::move(compilation_plan.preparatory_tasks()));

    /// Build operators/nodes according to the logical plan.
    const auto is_memory_tracing =
        sample_type.has_value() && std::get<0>(sample_type.value()) == logical::SampleNode::Level::HistoricalMemory;
    auto *last_operator_node = graph->build(compilation_plan.root_operator().get(), profiling_counter,
                                            is_explain_dram_bandwidth ? chronometer : nullptr,
                                            is_explain_task_graph || is_explain_data_flow_graph, is_memory_tracing);

    if (is_explain_flounder || is_explain_assembly) [[unlikely]]
    {
        return graph;
    }

    /// Internal requests (e.g., when starting the system) do not require the result.
    if (client_id == std::numeric_limits<std::uint32_t>::max()) [[unlikely]]
    {
        auto *drying_up_node = new mx::tasking::dataflow::EmptyNode<execution::RecordSet>();
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         drying_up_node);
        return graph;
    }

    /// The user requested performance counter only. Collect performance
    /// results and send them to the user.
    if (is_record_performance) [[unlikely]]
    {
        auto *gather_performance_node = new execution::GatherPerformanceCounterNode{client_id, std::move(chronometer)};
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         gather_performance_node);
        return graph;
    }

    /// The user requested to profile the query execution.
    if (sample_type.has_value()) [[unlikely]]
    {
        const auto counter_type = CompilationGraph::to_perf_counter(std::get<1>(sample_type.value()));

        if (std::get<0>(sample_type.value()) == logical::SampleNode::Level::Memory)
        {
            auto *gather_sample_operators_node = new execution::GatherSampleMemoryNode{
                database, client_id, std::move(chronometer), counter_type, std::get<2>(sample_type.value())};
            graph->make_edge(
                dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                gather_sample_operators_node);
        }
        else if (std::get<0>(sample_type.value()) == logical::SampleNode::Level::Operators)
        {
            auto *gather_sample_operators_node = new execution::GatherSampleOperatorsNode{
                client_id, std::move(chronometer), counter_type, std::get<2>(sample_type.value())};
            graph->make_edge(
                dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                gather_sample_operators_node);
        }
        else if (std::get<0>(sample_type.value()) == logical::SampleNode::Level::HistoricalMemory)
        {
            auto *memory_tracing_node = new execution::MemoryTracingNode{
                last_operator_node->name(), data::PaxTile::size(last_operator_node->schema())};
            graph->make_edge(
                dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                memory_tracing_node);

            auto *gather_sample_memory_history_node = new execution::GatherSampleMemoryHistoryNode{
                client_id, std::move(chronometer), counter_type, std::get<2>(sample_type.value())};
            graph->make_edge(memory_tracing_node, gather_sample_memory_history_node);
        }
        else
        {
            auto *gather_sample_assembly_node = new execution::GatherSampleAssemblyNode{
                client_id, std::move(chronometer), counter_type, std::get<2>(sample_type.value())};
            graph->make_edge(
                dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                gather_sample_assembly_node);
        }

        return graph;
    }

    /// The user requested task load only. Collect task load results
    /// from the runtime and send them to the user.
    if (is_record_task_load) [[unlikely]]
    {
        auto *gather_task_load_node = new execution::GatherTaskLoadNode{client_id, std::move(chronometer)};
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         gather_task_load_node);
        return graph;
    }

    /// The user requested to trace tasks. Collect tasks times from
    /// the runtime and send them to the user.
    if (is_record_task_traces) [[unlikely]]
    {
        auto *gather_task_traces_node = new execution::GatherTaskTraceNode{client_id, std::move(chronometer)};
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         gather_task_traces_node);
        return graph;
    }

    /// The user requested to record DRAM bandwith.
    if (is_explain_dram_bandwidth) [[unlikely]]
    {
        auto *gather_bandwidth_node = new execution::GatherMemoryBandwidthNode{client_id, std::move(chronometer)};
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         gather_bandwidth_node);
        return graph;
    }

    /// The user requested to explain the data flow.
    if (is_explain_data_flow_graph) [[unlikely]]
    {
        auto *gather_data_flow_node = new execution::GatherDataFlowGraphNode{client_id, std::move(chronometer)};
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         gather_data_flow_node);
        return graph;
    }

    if (is_explain_times) [[unlikely]]
    {
        auto *gather_times_node = new execution::GatherTimesNode{client_id, std::move(chronometer)};
        graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                         gather_times_node);
        return graph;
    }

    /// (Normal) user requests will be answered by the gather result node,
    /// which collects the results and send them to the user.
    auto *gather_results_node =
        new execution::GatherQueryResultNode{client_id, std::move(chronometer), last_operator_node->schema()};
    graph->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(last_operator_node),
                     gather_results_node);
    return graph;
}

db::execution::compilation::CompilationNode *CompilationGraph::build(
    execution::compilation::OperatorInterface *compilation_operator, const perf::Counter &profiling_counter,
    const std::shared_ptr<util::Chronometer> &chronometer, const bool is_collect_operator_information,
    const bool is_collect_memory_traces)
{
    execution::compilation::CompilationNode *compilation_node = nullptr;

    /// Execution context for consume/produce records and close.
    /// Per default, the close context has no need to materialize anything,
    /// except one operator explicitly changes that need.

    /// At first, acquire information if the operator needs a finalization phase.
    /// Depending on the result, the operators may emit code (or not).
    auto finalization_data = compilation_operator->finalization_data();

    /// Program, that consumes (or produces) records.
    /// This program is executed by Node::consume() or Node::produce(), respectively.
    auto execution_program = flounder::Program{};

    /// Produce code for operator.
    {
        auto context = execution::compilation::CompilationContext{};
        compilation_operator->request_symbols(execution::compilation::OperatorInterface::GenerationPhase::execution,
                                              context.symbols());
        compilation_operator->produce(execution::compilation::OperatorInterface::GenerationPhase::execution,
                                      execution_program, context);

        /// Optimize programs.
        auto optimizer = flounder::PreRegisterAllocationOptimizer{};
        optimizer.optimize(execution_program);
    }

    /// Let the compiled operator generate the data it will access.
    auto input_data_generator = compilation_operator->input_data_generator();

    /// Output provider for execution.
    auto execution_output_provider =
        compilation_operator->output_provider(execution::compilation::OperatorInterface::GenerationPhase::execution);

    /// Program, that is called on Node::close() when all records are processed.
    auto finalization_program = std::optional<flounder::Program>{std::nullopt};
    auto finalization_output_provider = std::unique_ptr<execution::compilation::OutputProviderInterface>{nullptr};
    if (finalization_data.has_value())
    {
        finalization_program = std::make_optional<flounder::Program>();
        /// Produce code.
        auto context = execution::compilation::CompilationContext{};
        compilation_operator->request_symbols(execution::compilation::OperatorInterface::GenerationPhase::finalization,
                                              context.symbols());
        compilation_operator->produce(execution::compilation::OperatorInterface::GenerationPhase::finalization,
                                      finalization_program.value(), context);

        /// Output provider for finalization.
        finalization_output_provider = compilation_operator->output_provider(
            execution::compilation::OperatorInterface::GenerationPhase::finalization);
    }

    /// Program for prefetching.
    auto prefechting_program = std::optional<flounder::Program>{std::nullopt};
    auto count_prefetches = std::uint8_t{0U};
    if (mx::tasking::runtime::prefetch_distance().is_enabled())
    {
        prefechting_program = std::make_optional<flounder::Program>();
        auto context = execution::compilation::CompilationContext{};

        compilation_operator->produce(execution::compilation::OperatorInterface::GenerationPhase::prefetching,
                                      prefechting_program.value(), context);
        count_prefetches = compilation_operator->count_prefeches();
    }

    /// Completion callback.
    auto completion_callback = compilation_operator->completion_callback();

    auto dependency_context = compilation_operator->dependencies();

    /// Name of the node compound by multiple operators.
    auto node_name = compilation_operator->to_string();

    /// Collect information.
    auto operator_information = std::unordered_map<std::string, std::string>{};
    if (is_collect_operator_information)
    {
        if (input_data_generator != nullptr)
        {
            operator_information.insert(
                std::make_pair("#Produced Tiles", util::string::shorten_number(input_data_generator->count())));
        }
        compilation_operator->emit_information(operator_information);
    }

    /// Some operators may finalize their nodes pipeline premature.
    /// The materialize partition operator can release the pipeline before spawning
    /// the radix join build operator, for instance.
    const auto is_finalize_premature = compilation_operator->is_finalize_pipeline_premature();

    /// Make program a node in the program graph.rsp-16
    if (input_data_generator != nullptr)
    {
        auto *producing_node =
            new execution::compilation::ProducingNode{std::move(input_data_generator),
                                                      topology::PhysicalSchema{compilation_operator->schema()},
                                                      std::string{node_name},
                                                      std::move(execution_program),
                                                      std::move(execution_output_provider),
                                                      std::move(finalization_program),
                                                      std::move(finalization_output_provider),
                                                      std::move(prefechting_program),
                                                      count_prefetches,
                                                      chronometer,
                                                      std::move(operator_information)};
        this->add(producing_node);

        /// Whenever a operator can finalize in parallel, annotate the node accordingly.
        if (finalization_data.has_value())
        {
            producing_node->annotation().finalization_type(std::get<0>(finalization_data.value()));
            producing_node->annotation().finalizes(std::move(std::get<1>(finalization_data.value())));
        }
        producing_node->annotation().is_finalizes_pipeline(is_finalize_premature);

        producing_node->annotation().completion_callback(std::move(completion_callback));

        compilation_node = producing_node;

        if constexpr (mx::tasking::config::is_collect_task_traces() ||
                      mx::tasking::config::is_monitor_task_cycles_for_prefetching())
        {
            mx::tasking::runtime::register_task_for_trace(producing_node->trace_id(), producing_node->name());
        }
    }
    else
    {
        auto *consuming_node =
            new execution::compilation::ConsumingNode{topology::PhysicalSchema{compilation_operator->schema()},
                                                      std::string{node_name},
                                                      std::move(execution_program),
                                                      std::move(execution_output_provider),
                                                      std::move(finalization_program),
                                                      std::move(finalization_output_provider),
                                                      std::move(prefechting_program),
                                                      count_prefetches,
                                                      chronometer,
                                                      std::move(operator_information)};

        /// Whenever a operator can finalize in parallel, annotate the node accordingly.
        if (finalization_data.has_value())
        {
            consuming_node->annotation().finalization_type(std::get<0>(finalization_data.value()));
            consuming_node->annotation().finalizes(std::move(std::get<1>(finalization_data.value())));
        }
        consuming_node->annotation().is_finalizes_pipeline(is_finalize_premature);

        consuming_node->annotation().completion_callback(std::move(completion_callback));

        compilation_node = consuming_node;

        if constexpr (mx::tasking::config::is_collect_task_traces() ||
                      mx::tasking::config::is_monitor_task_cycles_for_prefetching())
        {
            mx::tasking::runtime::register_task_for_trace(consuming_node->trace_id(), std::move(node_name));
        }

        if (dependency_context.has_value() && dependency_context->subsequent_operator() != nullptr)
        {
            auto *child = this->build(dependency_context->subsequent_operator(), profiling_counter, chronometer,
                                      is_collect_operator_information, is_collect_memory_traces);

            if (is_collect_memory_traces) [[unlikely]]
            {
                auto *memory_tracing_node =
                    new execution::MemoryTracingNode{child->name(), data::PaxTile::size(child->schema())};
                this->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(child),
                                memory_tracing_node);
                this->make_edge(memory_tracing_node, consuming_node);
            }
            else
            {
                this->make_edge(dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(child),
                                consuming_node);
            }
        }
    }

    /// Annotate the resource boundness.
    const auto resource_boundness = compilation_operator->resource_boundness();
    dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(compilation_node)
        ->annotation()
        .resource_boundness(resource_boundness);

    /// Produce code for build pipeline.
    if (dependency_context.has_value())
    {
        const auto &depending_operators = dependency_context->dependent_operators();
        for (auto *depending_operator : depending_operators)
        {
            /// This operator is a stand-alone program; no parent needed anymore.
            depending_operator->parent(nullptr);

            auto *depending_node = this->build(depending_operator, profiling_counter, chronometer,
                                               is_collect_operator_information, is_collect_memory_traces);
            this->make_dependency(
                dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(compilation_node),
                dynamic_cast<mx::tasking::dataflow::NodeInterface<execution::RecordSet> *>(depending_node));
        }
    }

    return compilation_node;
}

void CompilationGraph::compile(const bool make_visible_to_perf, const bool make_visible_to_vtune)
{
    auto perf_jit_map = flounder::PerfJitMap{};

    this->for_each_node(
        [&compiler = this->_compiler, make_visible_to_perf, make_visible_to_vtune, &perf_jit_map](auto *node) {
            const auto is_producing_node = typeid(*node) == typeid(execution::compilation::ProducingNode);
            if (is_producing_node || typeid(*node) == typeid(execution::compilation::ConsumingNode))
            {
                auto *compilation_node = dynamic_cast<execution::compilation::CompilationNode *>(node);

                /// Compile the node.
                const auto is_compiled = compilation_node->compile(compiler);
                if (is_compiled == false) [[unlikely]]
                {
                    throw exception::CouldNotCompileException{compilation_node->name()};
                }

                /// Annotate the prefetch descriptor.
                if (is_producing_node)
                {
                    auto *producing_node = dynamic_cast<execution::compilation::ProducingNode *>(node);
                    const auto prefetch_callback = producing_node->prefetch_callback();
                    const auto prefetch_size = producing_node->count_prefetches();
                    if (prefetch_callback.has_value() && prefetch_size)
                    {
                        auto &token_generator = producing_node->annotation().token_generator();
                        if (token_generator)
                        {
                            const auto prefetch_descriptor =
                                mx::tasking::PrefetchCallback::make(prefetch_size, prefetch_callback.value());

                            auto *scan_generator = reinterpret_cast<execution::ScanGenerator *>(token_generator.get());
                            scan_generator->prefetch(prefetch_descriptor);
                        }
                    }
                }

                /// Add to perf jit map, if requested.
                if (make_visible_to_perf) [[unlikely]]
                {
                    auto name = compilation_node->name();

                    if (compilation_node->finalize_program().has_value())
                    {
                        perf_jit_map.make_visible(compilation_node->finalize_program()->executable(),
                                                  fmt::format("{}::finalize", name));
                    }

                    if (compilation_node->prefetch_program().has_value())
                    {
                        perf_jit_map.make_visible(compilation_node->prefetch_program()->executable(),
                                                  fmt::format("{}::prefetch", name));
                    }

                    perf_jit_map.make_visible(compilation_node->consume_program().executable(),
                                              fmt::format("{}::consume", std::move(name)));
                }

                /// Make the jitted code visible to VTune.
                if (make_visible_to_vtune) [[unlikely]]
                {
                    auto name = compilation_node->name();

                    if (compilation_node->finalize_program().has_value())
                    {
                        flounder::VTuneJitAPI::make_visible(compilation_node->finalize_program()->executable(),
                                                            fmt::format("{}::finalize", name));
                    }

                    if (compilation_node->prefetch_program().has_value())
                    {
                        flounder::VTuneJitAPI::make_visible(compilation_node->prefetch_program()->executable(),
                                                            fmt::format("{}::prefetch", name));
                    }

                    flounder::VTuneJitAPI::make_visible(compilation_node->consume_program().executable(),
                                                        fmt::format("{}::consume", std::move(name)));
                }
            }
        });
}

nlohmann::json CompilationGraph::to_code(
    const bool compiled_code, std::optional<std::reference_wrapper<const perf::AggregatedSamples>> samples) const
{
    auto json = nlohmann::json{};

    auto program_id = 0U;
    for (auto *pipeline : this->pipelines())
    {
        for (auto *node : pipeline->nodes())
        {
            if (auto *compiled_node = dynamic_cast<execution::compilation::CompilationNode *>(node))
            {
                auto json_program = nlohmann::json{};
                json_program["id"] = std::to_string(program_id++);
                json_program["name"] = compiled_node->name();

                if (compiled_code)
                {
                    if (samples.has_value() == false)
                    {
                        const auto [consume_code, finalize_code, prefetching_code] = compiled_node->assembly_code();
                        if (consume_code.has_value())
                        {
                            std::move(consume_code->begin(), consume_code->end(),
                                      std::back_inserter(json_program["code"]["consume"]));
                        }

                        if (finalize_code.has_value())
                        {
                            std::move(finalize_code->begin(), finalize_code->end(),
                                      std::back_inserter(json_program["code"]["finalize"]));
                        }

                        if (prefetching_code.has_value())
                        {
                            std::move(prefetching_code->begin(), prefetching_code->end(),
                                      std::back_inserter(json_program["code"]["prefetching"]));
                        }
                    }
                    else
                    {
                        auto [consume_code, finalize_code, prefetching_code] =
                            compiled_node->assembly_code(samples->get());
                        if (consume_code.has_value())
                        {
                            const auto consume_percentage = std::accumulate(
                                consume_code->begin(), consume_code->end(), 0.0,
                                [](const auto sum, const auto &line) { return sum + std::get<1>(line); });
                            const auto consume_count = std::accumulate(
                                consume_code->begin(), consume_code->end(), std::uint64_t(0U),
                                [](const auto sum, const auto &line) { return sum + std::get<0>(line); });
                            for (auto &line : consume_code.value())
                            {
                                json_program["code"]["consume"]["lines"].emplace_back(
                                    nlohmann::json{{"count", std::get<0>(line)},
                                                   {"percentage", std::get<1>(line)},
                                                   {"line", std::move(std::get<2>(line))}});
                            }

                            json_program["code"]["consume"]["count"] = consume_count;
                            json_program["code"]["consume"]["percentage"] = consume_percentage;
                        }

                        if (finalize_code.has_value())
                        {
                            const auto finalize_percentage = std::accumulate(
                                finalize_code->begin(), finalize_code->end(), 0.0,
                                [](const auto sum, const auto &line) { return sum + std::get<1>(line); });
                            const auto finalize_count = std::accumulate(
                                finalize_code->begin(), finalize_code->end(), std::uint64_t(0U),
                                [](const auto sum, const auto &line) { return sum + std::get<0>(line); });
                            for (auto &line : finalize_code.value())
                            {
                                json_program["code"]["finalize"]["lines"].emplace_back(
                                    nlohmann::json{{"count", std::get<0>(line)},
                                                   {"percentage", std::get<1>(line)},
                                                   {"line", std::move(std::get<2>(line))}});
                            }

                            json_program["code"]["finalize"]["count"] = finalize_count;
                            json_program["code"]["finalize"]["percentage"] = finalize_percentage;
                        }

                        if (prefetching_code.has_value())
                        {
                            const auto prefetching_percentage = std::accumulate(
                                prefetching_code->begin(), prefetching_code->end(), 0.0,
                                [](const auto sum, const auto &line) { return sum + std::get<1>(line); });
                            const auto prefetching_count = std::accumulate(
                                prefetching_code->begin(), prefetching_code->end(), std::uint64_t(0U),
                                [](const auto sum, const auto &line) { return sum + std::get<0>(line); });
                            for (auto &line : prefetching_code.value())
                            {
                                json_program["code"]["prefetching"]["lines"].emplace_back(
                                    nlohmann::json{{"count", std::get<0>(line)},
                                                   {"percentage", std::get<1>(line)},
                                                   {"line", std::move(std::get<2>(line))}});
                            }

                            json_program["code"]["prefetching"]["count"] = prefetching_count;
                            json_program["code"]["prefetching"]["percentage"] = prefetching_percentage;
                        }
                    }
                }
                else
                {
                    const auto [consume_code, finalize_code, prefetching_code] = compiled_node->flounder_code();
                    std::move(consume_code.begin(), consume_code.end(),
                              std::back_inserter(json_program["code"]["consume"]));

                    if (finalize_code.has_value())
                    {
                        std::move(finalize_code.value().begin(), finalize_code.value().end(),
                                  std::back_inserter(json_program["code"]["finalize"]));
                    }

                    if (prefetching_code.has_value())
                    {
                        std::move(prefetching_code.value().begin(), prefetching_code.value().end(),
                                  std::back_inserter(json_program["code"]["prefetching"]));
                    }
                }
                json.emplace_back(std::move(json_program));
            }
        }
    }

    return json;
}

nlohmann::json CompilationGraph::to_contexts(const perf::AggregatedSamples &samples) const
{
    auto json = nlohmann::json{};

    auto program_id = 0U;
    for (auto *pipeline : this->pipelines())
    {
        for (auto *node : pipeline->nodes())
        {
            if (auto *compiled_node = dynamic_cast<execution::compilation::CompilationNode *>(node))
            {
                auto json_program = nlohmann::json{};
                json_program["id"] = std::to_string(program_id++);
                json_program["name"] = compiled_node->name();

                auto [consume_context, finalize_context, prefetching_context] = compiled_node->contexts(samples);

                if (consume_context.has_value())
                {
                    const auto consume_count =
                        std::accumulate(consume_context->begin(), consume_context->end(), std::uint64_t(0U),
                                        [](const auto sum, const auto &line) { return sum + std::get<0>(line); });
                    const auto consume_percentage =
                        std::accumulate(consume_context->begin(), consume_context->end(), 0.0,
                                        [](const auto sum, const auto &line) { return sum + std::get<1>(line); });

                    for (auto &operation : consume_context.value())
                    {
                        json_program["contexts"]["consume"]["operators"].emplace_back(
                            nlohmann::json{{"count", std::get<0>(operation)},
                                           {"percentage", std::get<1>(operation)},
                                           {"operator", std::move(std::get<2>(operation))}});
                    }

                    json_program["contexts"]["consume"]["count"] = consume_count;
                    json_program["contexts"]["consume"]["percentage"] = consume_percentage;
                }

                if (finalize_context.has_value())
                {
                    const auto finalize_count =
                        std::accumulate(finalize_context->begin(), finalize_context->end(), std::uint64_t(0),
                                        [](const auto sum, const auto &line) { return sum + std::get<0>(line); });
                    const auto finalize_percentage =
                        std::accumulate(finalize_context->begin(), finalize_context->end(), 0.0,
                                        [](const auto sum, const auto &line) { return sum + std::get<1>(line); });
                    for (auto &operation : finalize_context.value())
                    {
                        json_program["contexts"]["finalize"]["operators"].emplace_back(
                            nlohmann::json{{"count", std::get<0>(operation)},
                                           {"percentage", std::get<1>(operation)},
                                           {"operator", std::move(std::get<2>(operation))}});
                    }

                    json_program["contexts"]["finalize"]["count"] = finalize_count;
                    json_program["contexts"]["finalize"]["percentage"] = finalize_percentage;
                }

                if (prefetching_context.has_value())
                {
                    const auto prefetching_count =
                        std::accumulate(prefetching_context->begin(), prefetching_context->end(), std::uint64_t(0),
                                        [](const auto sum, const auto &line) { return sum + std::get<0>(line); });
                    const auto prefetching_percentage =
                        std::accumulate(prefetching_context->begin(), prefetching_context->end(), 0.0,
                                        [](const auto sum, const auto &line) { return sum + std::get<1>(line); });
                    for (auto &operation : finalize_context.value())
                    {
                        json_program["contexts"]["prefetching"]["operators"].emplace_back(
                            nlohmann::json{{"count", std::get<0>(operation)},
                                           {"percentage", std::get<1>(operation)},
                                           {"operator", std::move(std::get<2>(operation))}});
                    }

                    json_program["contexts"]["prefetching"]["count"] = prefetching_count;
                    json_program["contexts"]["prefetching"]["percentage"] = prefetching_percentage;
                }

                json.emplace_back(std::move(json_program));
            }
        }
    }

    return json;
}

perf::CounterDescription CompilationGraph::to_perf_counter(
    const logical::SampleNode::CounterType logical_counter) noexcept
{
    switch (logical_counter)
    {
    case logical::SampleNode::CounterType::Branches:
        return perf::CounterDescription::BRANCHES;
    case logical::SampleNode::CounterType::BranchMisses:
        return perf::CounterDescription::BRANCH_MISSES;
    case logical::SampleNode::CounterType::Cycles:
        return perf::CounterDescription::CYCLES;
    case logical::SampleNode::CounterType::Instructions:
        return perf::CounterDescription::INSTRUCTIONS;
    case logical::SampleNode::CounterType::CacheMisses:
        return perf::CounterDescription::CACHE_MISSES;
    case logical::SampleNode::CounterType::CacheReferences:
        return perf::CounterDescription::CACHE_REFERENCES;
    case logical::SampleNode::CounterType::StallsMemAny:
        return perf::CounterDescription::CYCLE_ACTIVITY_STALLS_MEM_ANY;
    case logical::SampleNode::CounterType::StallsL3Miss:
        return perf::CounterDescription::CYCLE_ACTIVITY_STALLS_L3_MISS;
    case logical::SampleNode::CounterType::StallsL2Miss:
        return perf::CounterDescription::CYCLE_ACTIVITY_STALLS_L2_MISS;
    case logical::SampleNode::CounterType::StallsL1DMiss:
        return perf::CounterDescription::CYCLE_ACTIVITY_STALLS_L1D_MISS;
    case logical::SampleNode::CounterType::CyclesL3Miss:
        return perf::CounterDescription::CYCLE_ACTIVITY_CYCLES_L3_MISS;
    case logical::SampleNode::CounterType::DTLBMiss:
        return perf::CounterDescription::DTLB_LOAD_MISSES;
    case logical::SampleNode::CounterType::L3MissRemote:
        return perf::CounterDescription::MEM_LOAD_L3_MISS_RETIRED_REMOTE_DRAM;
    case logical::SampleNode::CounterType::FillBufferFull:
        return perf::CounterDescription::L1D_PEND_MISS_FB_FULL;
    case logical::SampleNode::CounterType::LoadHitL1DFillBuffer:
        return perf::CounterDescription::LOAD_HIT_PRE_SW_PF;
    case logical::SampleNode::CounterType::MemRetiredLoads:
        return perf::CounterDescription::MEM_INST_RETIRED_ALL_LOADS;
    case logical::SampleNode::CounterType::MemRetiredStores:
        return perf::CounterDescription::MEM_INST_RETIRED_ALL_STORES;
    case logical::SampleNode::CounterType::MemRetiredLoadL1Miss:
        return perf::CounterDescription::MEM_LOAD_RETIRED_L1_MISS;
    case logical::SampleNode::CounterType::MemRetiredLoadL2Miss:
        return perf::CounterDescription::MEM_LOAD_RETIRED_L2_MISS;
    case logical::SampleNode::CounterType::MemRetiredLoadL3Miss:
        return perf::CounterDescription::MEM_LOAD_RETIRED_L3_MISS;
    case logical::SampleNode::CounterType::BAClearsAny:
        return perf::CounterDescription::BACLEARS_ANY;
    }
}