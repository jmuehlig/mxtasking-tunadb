#pragma once
#include <cstdint>
#include <db/execution/operator_interface.h>
#include <db/execution/record_token.h>
#include <db/execution/scan_generator.h>
#include <db/topology/table.h>
#include <mx/tasking/dataflow/node.h>
#include <optional>
#include <string>
#include <vector>

namespace db::execution::interpretation {
class CopyNode final : public mx::tasking::dataflow::ProducingNodeInterface<RecordSet>, public OperatorInterface
{
public:
    CopyNode(topology::PhysicalSchema &&schema, std::string &&file_name, const char separator) noexcept
        : _schema(std::move(schema)), _file_name(std::move(file_name)), _separator(separator)
    {
        NodeInterface<RecordSet>::annotation().produces(std::make_unique<DisponsableGenerator>());
    }

    ~CopyNode() noexcept override = default;

    void consume(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                 RecordToken &&data) override;

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

    [[nodiscard]] std::string to_string() const noexcept override { return "COPY"; }

private:
    /// Schema of this operator, basically the schema
    /// of the table that will the CSV imported into.
    const topology::PhysicalSchema _schema;

    /// Name of the file that is imported.
    const std::string _file_name;

    /// Separator that separates values.
    const char _separator;
};
} // namespace db::execution::interpretation