#pragma once
#include "node_interface.h"

namespace db::plan::logical {
class ExplainNode final : public UnaryNode
{
public:
    enum Level
    {
        Plan,
        TaskGraph,
        DataFlowGraph,
        Performance,
        TaskLoad,
        TaskTraces,
        Flounder,
        Assembly,
        DRAMBandwidth,
        Times,
    };

    explicit ExplainNode(const Level level) : UnaryNode("Explain"), _level(level) {}

    ~ExplainNode() noexcept override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::EXPLAIN; }

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

    [[nodiscard]] Level level() const noexcept { return _level; }

private:
    const Level _level;
};
} // namespace db::plan::logical