#include "copy_node.h"
#include <algorithm>
#include <db/data/pax_tile.h>
#include <db/data/value.h>
#include <db/exception/execution_exception.h>

using namespace db::execution::interpretation;

void CopyNode::consume(const std::uint16_t worker_id, mx::tasking::dataflow::EmitterInterface<RecordSet> &graph,
                       RecordToken && /*data*/)
{
    /// Open the file.
    auto file = std::ifstream{this->_file_name};
    if (file.is_open() == false)
    {
        throw exception::ExecutionException{"Can not open csv file '" + this->_file_name + "'."};
    }

    /// Record sets that are emitted to the graph.
    auto record_sets = std::vector<RecordSet>{};
    record_sets.reserve(1U << 6U);

    /// Make a first set.
    record_sets.emplace_back(RecordSet::make_record_set(this->_schema, worker_id));

    /// Scan the file line by line.
    std::string line;
    while (std::getline(file, line))
    {
        auto line_stream = std::istringstream{line};
        auto row = std::vector<std::string>{};
        row.reserve(this->_schema.size());

        /// Split the line into multiple values by the separator.
        std::string cell;
        while (std::getline(line_stream, cell, this->_separator))
        {
            row.emplace_back(std::move(cell));
        }

        /// Allocate a record within the tile and set the value of each cell.
        if (row.size() == this->_schema.size())
        {
            if (record_sets.back().tile().get<data::PaxTile>()->full())
            {
                record_sets.emplace_back(RecordSet::make_record_set(this->_schema, worker_id));
            }

            auto pax_record_view = record_sets.back().tile().get<data::PaxTile>()->allocate();
            for (auto column_id = 0U; column_id < row.size(); ++column_id)
            {
                auto value = data::Value{type::Type::make_char(row[column_id].length()), std::move(row[column_id])};
                pax_record_view->set(column_id, std::move(value.as(this->_schema.type(column_id))));
            }
        }
    }

    /// Remove the last record set, if it is empty.
    if (record_sets.back().tile().get<data::PaxTile>()->empty())
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