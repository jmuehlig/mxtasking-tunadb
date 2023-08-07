#pragma once
#include <algorithm>
#include <cstdint>
#include <db/execution/operator_interface.h>
#include <db/execution/record_token.h>
#include <db/execution/scan_generator.h>
#include <db/topology/database.h>
#include <db/topology/physical_schema.h>
#include <db/type/type.h>
#include <mx/tasking/dataflow/node.h>
#include <optional>
#include <string>
#include <vector>

namespace db::execution::interpretation {
class UpdateStatisticsNode final : public mx::tasking::dataflow::ProducingNodeInterface<RecordSet>,
                                   public OperatorInterface
{
public:
    UpdateStatisticsNode(topology::Table &table) noexcept : _table(table)
    {
        NodeInterface<RecordSet>::annotation().produces(std::make_unique<DisponsableGenerator>());
    }

    ~UpdateStatisticsNode() noexcept override = default;

    void consume(std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                 RecordToken &&data) override;

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

    [[nodiscard]] std::string to_string() const noexcept override { return "Update Statistics"; }

private:
    /// Table to read the schema from.
    topology::Table &_table;

    /// Schema of this operator, attributes to describe the schema.
    topology::PhysicalSchema _schema;

    [[nodiscard]] static bool is_use_equi_depth(const type::Type type) noexcept
    {
        return type == type::Id::INT || type == type::Id::BIGINT || type == type::Id::DECIMAL || type == type::Id::DATE;
    }

    [[nodiscard]] static bool is_use_singleton(const type::Type type, const std::uint64_t count_distinct) noexcept
    {
        return (type == type::Id::CHAR && count_distinct <= 64U) || type == type::Id::BOOL;
    }
};
} // namespace db::execution::interpretation