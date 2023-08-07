#pragma once
#include <cstdint>
#include <db/data/value.h>
#include <db/execution/operator_interface.h>
#include <db/execution/record_token.h>
#include <db/execution/scan_generator.h>
#include <db/topology/table.h>
#include <mx/tasking/dataflow/node.h>
#include <optional>
#include <string>
#include <vector>

namespace db::execution::interpretation {
class DeliverNode final : public mx::tasking::dataflow::ProducingNodeInterface<RecordSet>, public OperatorInterface
{
public:
    DeliverNode(topology::PhysicalSchema &&schema, std::vector<std::uint16_t> &&column_indices,
                std::vector<std::vector<data::Value>> &&data_lists) noexcept
        : _schema(std::move(schema)), _column_indices(std::move(column_indices)), _data_lists(std::move(data_lists))
    {
        NodeInterface<RecordSet>::annotation().produces(std::make_unique<DisponsableGenerator>());
    }

    ~DeliverNode() noexcept override = default;

    void consume(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                 RecordToken &&data) override;

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

    [[nodiscard]] static std::vector<std::uint16_t> build_column_indices(const topology::Table &table,
                                                                         const std::vector<std::string> &column_names);

    [[nodiscard]] std::string to_string() const noexcept override { return "Deliver"; }

private:
    /// Schema of this operator, basically the schema of the table these records are delivered for.
    const topology::PhysicalSchema _schema;

    /// List of column indices that are given by the data lists.
    const std::vector<std::uint16_t> _column_indices;

    /// List of data that is delivered.
    std::vector<std::vector<data::Value>> _data_lists;
};
} // namespace db::execution::interpretation