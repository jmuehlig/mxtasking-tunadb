#include "update_statistics_node.h"
#include <db/statistic/count_distinct_hyperloglog.h>
#include <db/statistic/equi_depth_histogram.h>
#include <db/statistic/singleton_histogram.h>
using namespace db::execution::interpretation;

void UpdateStatisticsNode::consume(const std::uint16_t /*worker_id*/,
                                   mx::tasking::dataflow::EmitterInterface<RecordSet> & /*graph*/,
                                   RecordToken && /*data*/)
{
    /// COUNT DISTINCT / COUNT
    {
        /// Distinct count and count statistics for each column/table.
        auto count_distinct_builders =
            std::vector<statistic::CountDistinctHyperLogLogBuilder>{this->_table.schema().size()};
        auto count_rows = 0ULL;
        for (const auto tile_ptr : this->_table.tiles())
        {
            auto *tile = tile_ptr.get<data::PaxTile>();

            for (auto row_id = 0U; row_id < tile->size(); ++row_id)
            {
                ++count_rows;

                const auto record_view = tile->view(row_id);
                for (auto term_id = 0U; term_id < this->_table.schema().size(); ++term_id)
                {
                    count_distinct_builders[term_id].insert(record_view.get(term_id));
                }
            }
        }

        /// Update table statistics for count.
        this->_table.statistics().count_rows(count_rows);
        for (auto term_id = 0U; term_id < this->_table.schema().size(); ++term_id)
        {
            this->_table.statistics().count_distinct()[term_id] = count_distinct_builders[term_id].get();
        }
    }

    /// HISTOGRAMS
    {
        /// Based on the distinct count statistics, choose the histogram.
        auto equi_depth_histogram_builders =
            std::vector<std::optional<statistic::EquiDepthHistogramBuilder>>{this->_table.schema().size()};
        auto singleton_histogram_builders =
            std::vector<std::optional<statistic::SingletonHistogramBuilder>>{this->_table.schema().size()};
        for (auto term_id = 0U; term_id < this->_table.schema().size(); ++term_id)
        {
            const auto type = this->_table.schema().type(term_id);

            if (UpdateStatisticsNode::is_use_equi_depth(type))
            {
                equi_depth_histogram_builders[term_id] = statistic::EquiDepthHistogramBuilder{};
            }
            else if (UpdateStatisticsNode::is_use_singleton(type, this->_table.statistics().count_distinct(term_id)))
            {
                singleton_histogram_builders[term_id] = statistic::SingletonHistogramBuilder{};
            }
        }

        /// Histograms statistics for each column/table.
        for (const auto tile_ptr : this->_table.tiles())
        {
            auto *tile = tile_ptr.get<data::PaxTile>();

            for (auto row_id = 0U; row_id < tile->size(); ++row_id)
            {
                const auto record = tile->view(row_id);
                for (auto term_id = 0U; term_id < this->_table.schema().size(); ++term_id)
                {
                    const auto value = record.get(term_id);

                    auto &equi_depth_histogram_builder = equi_depth_histogram_builders[term_id];
                    if (equi_depth_histogram_builder.has_value())
                    {
                        equi_depth_histogram_builder->insert(value);
                    }
                    else
                    {
                        auto &singleton_histogram_builder = singleton_histogram_builders[term_id];
                        if (singleton_histogram_builder.has_value())
                        {
                            singleton_histogram_builder->insert(value);
                        }
                    }
                }
            }
        }

        /// Update table statistics for count.
        for (auto term_id = 0U; term_id < this->_table.schema().size(); ++term_id)
        {
            auto &equi_depth_histogram_builder = equi_depth_histogram_builders[term_id];
            if (equi_depth_histogram_builder.has_value())
            {
                this->_table.statistics().histogram(term_id) = equi_depth_histogram_builder->build(256U);
            }
            else
            {
                auto &singleton_histogram_builder = singleton_histogram_builders[term_id];
                if (singleton_histogram_builder.has_value())
                {
                    this->_table.statistics().histogram(term_id) = singleton_histogram_builder->build();
                }
            }
        }
    }
}