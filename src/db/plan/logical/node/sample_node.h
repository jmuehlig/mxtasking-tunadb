#pragma once
#include "node_interface.h"

namespace db::plan::logical {
class SampleNode final : public UnaryNode
{
public:
    enum Level
    {
        Assembly,
        Operators,
        Memory,
        HistoricalMemory
    };

    enum CounterType
    {
        Branches,
        BranchMisses,
        Cycles,
        Instructions,
        CacheMisses,
        CacheReferences,
        StallsMemAny,
        StallsL3Miss,
        StallsL2Miss,
        StallsL1DMiss,
        CyclesL3Miss,
        DTLBMiss,
        L3MissRemote,
        FillBufferFull,
        LoadHitL1DFillBuffer,
        BAClearsAny,
        MemRetiredLoads,
        MemRetiredStores,
        MemRetiredLoadL1Miss,
        MemRetiredLoadL2Miss,
        MemRetiredLoadL3Miss,
    };

    SampleNode(const Level level, const CounterType counter_type, const std::optional<std::uint64_t> frequency)
        : UnaryNode("Sample"), _level(level), _counter_type(counter_type), _frequency(frequency)
    {
    }

    ~SampleNode() noexcept override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SAMPLE; }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        return child()->to_json(database);
    }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database & /*database*/,
                                            const NodeChildIterator &child_iterator) const override
    {
        return child_iterator.child(this)->relation().cardinality();
    }

    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const override
    {
        return child_iterator.child(this)->relation().schema();
    }

    [[nodiscard]] CounterType counter_type() const noexcept { return _counter_type; }
    [[nodiscard]] Level level() const noexcept { return _level; }
    [[nodiscard]] std::optional<std::uint64_t> frequency() const noexcept { return _frequency; }

private:
    const Level _level;
    const CounterType _counter_type;
    const std::optional<std::uint64_t> _frequency;
};
} // namespace db::plan::logical