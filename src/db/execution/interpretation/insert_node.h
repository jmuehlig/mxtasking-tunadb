#pragma once
#include <db/execution/operator_interface.h>
#include <db/execution/record_token.h>
#include <db/topology/table.h>
#include <mx/tasking/dataflow/task_node.h>

namespace db::execution::interpretation {
class InsertTask final : public mx::tasking::dataflow::DataTaskInterface<RecordSet>
{
public:
    constexpr InsertTask() noexcept = default;
    ~InsertTask() noexcept override = default;

    void execute(std::uint16_t worker_id, mx::tasking::dataflow::NodeInterface<RecordSet> *node,
                 mx::tasking::dataflow::EmitterInterface<RecordSet> &graph, RecordToken &&records) override;
};

class InsertNode final : public mx::tasking::dataflow::TaskNode<InsertTask>, public OperatorInterface
{
public:
    InsertNode(topology::Table &table) noexcept : _table(table) {}
    ~InsertNode() noexcept override = default;

    [[nodiscard]] topology::Table &table() noexcept { return _table; }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

    [[nodiscard]] std::string to_string() const noexcept override { return "Insert (Task)"; }

private:
    /// Schema of this operator, is always empty since this operator has no output.
    const topology::PhysicalSchema _schema;

    /// Table to insert data into.
    topology::Table &_table;
};

void InsertTask::execute(const std::uint16_t /*worker_id*/, mx::tasking::dataflow::NodeInterface<RecordSet> *node,
                         mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/, RecordToken &&records)
{
    auto &table = reinterpret_cast<InsertNode *>(node)->table();

    /// Split the tile to fixed-size tiles and emplace them in the table.
    table.emplace_back(records.data().tile().get<data::PaxTile>());
}
} // namespace db::execution::interpretation