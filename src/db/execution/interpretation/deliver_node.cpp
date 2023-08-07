#include "deliver_node.h"
#include <numeric>

using namespace db::execution::interpretation;

void DeliverNode::consume(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                          RecordToken && /*data*/)
{
    /// Record sets that are emitted to the graph.
    auto record_sets = std::vector<RecordSet>{};
    record_sets.reserve(1U << 6U);

    /// Make a first set.
    record_sets.emplace_back(RecordSet::make_record_set(this->_schema, worker_id));

    /// Build a record of each line.
    for (auto &values : this->_data_lists)
    {
        if (record_sets.back().tile().get<data::PaxTile>()->full())
        {
            record_sets.emplace_back(RecordSet::make_record_set(this->_schema, worker_id));
        }

        auto record = record_sets.back().tile().get<data::PaxTile>()->allocate();
        for (auto i = 0U; i < this->_column_indices.size(); ++i)
        {
            const auto index = this->_column_indices[i];
            record->set(index, std::move(values[i].as(this->_schema.type(index))));
        }
    }

    /// Remove the last record set, if it is empty.
    if (record_sets.back().tile().get<data::RowTile>()->empty())
    {
        record_sets.pop_back();
    }

    /// Transform sets to tokens accepted by the graph.
    auto tokens = std::vector<RecordToken>{};
    for (auto &record_set : record_sets)
    {
        graph.emit(worker_id, this, RecordToken{std::move(record_set), mx::tasking::annotation{worker_id}});
    }
}

std::vector<std::uint16_t> DeliverNode::build_column_indices(const topology::Table &table,
                                                             const std::vector<std::string> &column_names)
{
    auto column_indices = std::vector<std::uint16_t>{};

    if (column_indices.empty() == false)
    {
        column_indices.reserve(column_names.size());
        for (const auto &column_name : column_names)
        {
            const auto index = table.schema().index(column_name);
            if (index.has_value())
            {
                column_indices.push_back(index.value());
            }
        }
    }
    else
    {
        column_indices.resize(table.schema().size());
        std::iota(column_indices.begin(), column_indices.end(), 0U);
    }

    return column_indices;
}