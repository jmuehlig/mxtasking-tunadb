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
class DescribeTableNode final : public mx::tasking::dataflow::ProducingNodeInterface<RecordSet>,
                                public OperatorInterface
{
public:
    DescribeTableNode(const topology::Table &table) noexcept : _table(table)
    {
        _schema.emplace_back(expression::Term::make_attribute("Attribute"), type::Type::make_char(64U));
        _schema.emplace_back(expression::Term::make_attribute("Type"), type::Type::make_char(64U));
        _schema.emplace_back(expression::Term::make_attribute("Is Null"), type::Type::make_bool());
        _schema.emplace_back(expression::Term::make_attribute("Primary Key"), type::Type::make_bool());
        _schema.emplace_back(expression::Term::make_attribute("Length (Byte)"), type::Type::make_int());

        NodeInterface<RecordSet>::annotation().produces(std::make_unique<DisponsableGenerator>());
    }

    ~DescribeTableNode() noexcept override = default;

    void consume(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                 RecordToken && /*data*/) override
    {
        /// Build a record set with size equal to number of attributes.
        // TODO: Bug when number of columns > config::tuples_per_tile().
        auto records = RecordSet::make_record_set(_schema, worker_id);

        /// Build a record for each attribute in the schema.
        for (auto index = 0U; index < _table.schema().size(); ++index)
        {
            auto record = records.tile().get<data::PaxTile>()->allocate();
            record->set(0U, _table.schema().term(index).to_string());
            record->set(1U, _table.schema().type(index).to_string());
            record->set(2U, _table.schema().is_null(index));
            record->set(3U, _table.schema().is_primary_key(index));
            record->set(4U, type::underlying<type::Id::INT>::value(_table.schema().type(index).size()));
        }

        graph.emit(worker_id, this, RecordToken{std::move(records)});
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

    [[nodiscard]] std::string to_string() const noexcept override { return "Describe Table"; }

private:
    /// Table to read the schema from.
    const topology::Table &_table;

    /// Schema of this operator, attributes to describe the schema.
    topology::PhysicalSchema _schema;
};
} // namespace db::execution::interpretation