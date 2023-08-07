#pragma once

#include <cstdint>
#include <db/execution/record_token.h>
#include <mx/tasking/dataflow/node.h>
#include <mx/tasking/runtime.h>
#include <mx/util/aligned_t.h>
#include <string>

namespace db::execution {
class MemoryTracingNode final : public mx::tasking::dataflow::NodeInterface<RecordSet>
{
public:
    explicit MemoryTracingNode(std::string &&node_in_name, const std::uint64_t data_size)
        : _data_name(std::move(node_in_name)), _data_size(data_size)
    {
        this->annotation().finalization_type(mx::tasking::dataflow::annotation<RecordSet>::FinalizationType::none);

        _data.resize(mx::tasking::runtime::workers());

        for (auto &vector : _data)
        {
            vector.value().reserve(1 << 12U);
        }
    }

    ~MemoryTracingNode() override = default;

    void consume(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &emitter,
                 RecordToken &&data) override
    {
        /// Save the data and the size
        const auto begin = std::uintptr_t(data.data().tile().get());
        const auto end = begin + _data_size;
        _data[worker_id].value().emplace_back(begin, end);

        emitter.emit(worker_id, this, std::move(data));
    }

    void in_completed(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &emitter,
                      mx::tasking::dataflow::NodeInterface<RecordSet> & /*in_node*/) override
    {
        emitter.finalize(worker_id, this);
    }

    [[nodiscard]] std::string to_string() const noexcept override { return "Memory Tracing Node"; }

    [[nodiscard]] const std::string &data_name() const noexcept { return _data_name; }

    [[nodiscard]] std::vector<std::pair<std::uintptr_t, std::uintptr_t>> ranges()
    {
        auto size = std::accumulate(_data.begin(), _data.end(), 0U,
                                    [](auto sum, const auto &data) { return sum + data.value().size(); });

        auto ranges = std::vector<std::pair<std::uintptr_t, std::uintptr_t>>{};
        ranges.reserve(size);

        for (auto &worker_local_ranges : _data)
        {
            auto &data = worker_local_ranges.value();
            std::move(data.begin(), data.end(), std::back_inserter(ranges));
        }

        return ranges;
    }

private:
    std::string _data_name;
    const std::uint64_t _data_size;

    std::vector<mx::util::aligned_t<std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>> _data;
};
} // namespace db::execution