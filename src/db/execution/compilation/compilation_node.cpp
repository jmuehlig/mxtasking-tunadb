#include "compilation_node.h"
#include <db/util/string.h>

using namespace db::execution::compilation;

CompilationNode::CompilationNode(std::string &&name, topology::PhysicalSchema &&schema,
                                 flounder::Program &&consume_program,
                                 std::unique_ptr<OutputProviderInterface> &&consume_output_provider,
                                 std::optional<flounder::Program> &&finalize_program,
                                 std::unique_ptr<OutputProviderInterface> &&finalize_output_provider,
                                 std::optional<flounder::Program> &&prefetching_program,
                                 const std::uint8_t count_prefetches, std::shared_ptr<util::Chronometer> &&chronometer,
                                 std::unordered_map<std::string, std::string> &&information) noexcept
    : _name(std::move(name)), _schema(std::move(schema)),
      _consume_program(std::move(consume_program), std::move(consume_output_provider)),
      _chronometer(std::move(chronometer)), _information(std::move(information))
{
    if (finalize_program.has_value())
    {
        this->_finalize_program.emplace(std::move(finalize_program.value()), std::move(finalize_output_provider));
    }

    if (prefetching_program.has_value())
    {
        this->_prefetch_program.emplace(std::move(prefetching_program.value()));
        this->_count_prefetches = count_prefetches;
    }
}

std::string CompilationNode::name() const
{
    return db::util::string::replace(
        std::string{this->_name},
        {std::make_pair(R"(\s\{\s)", "_"), std::make_pair("\\s\\}", ""), std::make_pair("\\s", "")});
}

void CompilationNode::consume(std::uint16_t worker_id, mx::tasking::dataflow::NodeInterface<RecordSet> *node,
                              mx::tasking::dataflow::EmitterInterface<RecordSet> &emitter,
                              mx::tasking::dataflow::Token<RecordSet> &data)
{
    auto *tile = data.data().tile().get<data::PaxTile>();

    /// Input data for the task.
    const auto begin = std::uintptr_t(tile->begin());
    const auto size = tile->size();
    const auto secondary_input = std::uintptr_t(data.data().secondary_input().get());

    /// Figure out, which kind of output is passed to the compiled program.
    const auto output = this->_consume_program.output_provider() != nullptr
                            ? this->_consume_program.output_provider()->get(
                                  worker_id, std::make_optional(std::ref(data)), emitter, node)
                            : 0U;
    this->_consume_program.execute<void, std::uintptr_t, std::uintptr_t, std::uintptr_t, std::uintptr_t>(
        begin, size, output, secondary_input);
}

void CompilationNode::finalize(std::uint16_t worker_id, mx::tasking::dataflow::NodeInterface<RecordSet> *node,
                               mx::tasking::dataflow::EmitterInterface<RecordSet> &emitter, const bool /*is_last*/,
                               const mx::resource::ptr data, const mx::resource::ptr reduced_data)
{
    auto &finalizer = this->_finalize_program;
    if (finalizer.has_value())
    {
        /// Figure out, which kind of output is passed to the compiled program.
        const auto &output_provider = finalizer->output_provider();
        const auto output =
            output_provider != nullptr ? output_provider->get(worker_id, std::nullopt, emitter, node) : 0U;

        /// Execute the compiled operator.
        finalizer->execute<void, std::uintptr_t, std::uint64_t, std::uintptr_t, std::uintptr_t>(
            output, std::uint64_t(worker_id), std::uintptr_t(data.get()), std::uintptr_t(reduced_data.get()));
    }

    if (this->_chronometer != nullptr) [[unlikely]]
    {
        this->_chronometer->timed_events().emplace_back(this->name());
    }
}

void ConsumingTask::execute(std::uint16_t worker_id, mx::tasking::dataflow::NodeInterface<RecordSet> *node,
                            mx::tasking::dataflow::EmitterInterface<RecordSet> &emitter,
                            mx::tasking::dataflow::Token<RecordSet> &&data)
{
    auto *consuming_node = dynamic_cast<ConsumingNode *>(node);
    assert(consuming_node != nullptr);
    consuming_node->CompilationNode::consume(worker_id, node, emitter, data);
}
