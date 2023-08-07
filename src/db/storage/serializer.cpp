#include "serializer.h"
#include <db/data/row_tile.h>
#include <db/exception/execution_exception.h>
#include <db/statistic/equi_depth_histogram.h>
#include <db/statistic/singleton_histogram.h>
#include <fmt/core.h>
#include <fstream>

using namespace db::storage;

void Serializer::serialize(const topology::Database &database, const std::string &file_name)
{
    auto out_stream = std::ofstream{file_name, std::ios::trunc | std::ios::binary};

    /// Number of the tables.
    const auto count_tables = std::uint64_t(database.tables().size());
    out_stream.write(reinterpret_cast<const char *>(&count_tables), sizeof(std::uint64_t));

    for (const auto &[name, table] : database.tables())
    {
        /// Name of the table.
        const auto name_size = std::uint64_t(name.size());
        out_stream.write(reinterpret_cast<const char *>(&name_size), sizeof(std::uint64_t));
        out_stream.write(name.data(), name_size);

        const auto &schema = table.schema();

        /// Number of terms.
        const auto count_terms = std::uint64_t(schema.size());
        out_stream.write(reinterpret_cast<const char *>(&count_terms), sizeof(std::uint64_t));

        for (auto i = 0U; i < schema.size(); ++i)
        {
            const auto &term = schema.term(i);

            if (term.is_attribute())
            {
                /// Column name.
                const auto attribute = term.get<expression::Attribute>().column_name();
                const auto attribute_size = std::uint64_t(attribute.size());
                out_stream.write(reinterpret_cast<const char *>(&attribute_size), sizeof(std::uint64_t));
                out_stream.write(attribute.data(), attribute_size);

                /// Type.
                const auto type = schema.type(i);
                const auto type_id = std::uint32_t(type.id());
                out_stream.write(reinterpret_cast<const char *>(&type_id), sizeof(std::uint32_t));
                if (type.id() == type::Id::DECIMAL)
                {
                    const auto precision = type.decimal_description().precision();
                    out_stream.write(reinterpret_cast<const char *>(&precision), sizeof(std::uint8_t));

                    const auto scale = type.decimal_description().scale();
                    out_stream.write(reinterpret_cast<const char *>(&scale), sizeof(std::uint8_t));
                }
                else if (type.id() == type::Id::CHAR)
                {
                    const auto length = type.char_description().length();
                    out_stream.write(reinterpret_cast<const char *>(&length), sizeof(std::uint16_t));
                }

                /// Nullable.
                const auto nullable = std::uint8_t(schema.is_null(i));
                out_stream.write(reinterpret_cast<const char *>(&nullable), sizeof(std::uint8_t));

                /// Primary Key.
                const auto primary_key = std::uint8_t(schema.is_primary_key(i));
                out_stream.write(reinterpret_cast<const char *>(&primary_key), sizeof(std::uint8_t));

                /// Statistics
                /// - Histogram
                const auto &histogram = table.statistics().histogram(i);
                const auto has_histogram = static_cast<std::uint8_t>(histogram != nullptr);
                out_stream.write(reinterpret_cast<const char *>(&has_histogram), sizeof(std::uint8_t));
                if (has_histogram > 0U)
                {
                    const auto histogram_type = histogram->type();
                    out_stream.write(reinterpret_cast<const char *>(&histogram_type),
                                     sizeof(statistic::HistogramInterface::Type));

                    if (histogram_type == statistic::HistogramInterface::Type::EquiDepth)
                    {
                        auto *equi_depth_histogram = reinterpret_cast<statistic::EquiDepthHistogram *>(histogram.get());
                        const auto count = equi_depth_histogram->count();
                        out_stream.write(reinterpret_cast<const char *>(&count), sizeof(std::uint64_t));
                        const auto depth = equi_depth_histogram->depth();
                        out_stream.write(reinterpret_cast<const char *>(&depth), sizeof(std::uint64_t));
                        const auto lower = equi_depth_histogram->lower_key();
                        out_stream.write(reinterpret_cast<const char *>(&lower), sizeof(std::uint64_t));
                        const auto upper = equi_depth_histogram->upper_key();
                        out_stream.write(reinterpret_cast<const char *>(&upper), sizeof(std::uint64_t));

                        const auto count_bins = std::uint64_t(equi_depth_histogram->bins().size());
                        out_stream.write(reinterpret_cast<const char *>(&count_bins), sizeof(std::uint64_t));
                        for (const auto &bin : equi_depth_histogram->bins())
                        {
                            const auto bin_lower = bin.lower();
                            out_stream.write(reinterpret_cast<const char *>(&bin_lower), sizeof(std::int64_t));
                            const auto bin_upper = bin.upper();
                            out_stream.write(reinterpret_cast<const char *>(&bin_upper), sizeof(std::int64_t));
                            const auto bin_count = bin.count();
                            out_stream.write(reinterpret_cast<const char *>(&bin_count), sizeof(std::uint64_t));
                        }
                    }
                    else if (histogram_type == statistic::HistogramInterface::Type::Singleton)
                    {
                        auto *singleton_histogram = reinterpret_cast<statistic::SingletonHistogram *>(histogram.get());
                        const auto count = singleton_histogram->count();
                        out_stream.write(reinterpret_cast<const char *>(&count), sizeof(std::uint64_t));

                        const auto bin_count = std::uint64_t(singleton_histogram->data().size());
                        out_stream.write(reinterpret_cast<const char *>(&bin_count), sizeof(std::uint64_t));

                        for (const auto &[value, value_count] : singleton_histogram->data())
                        {
                            if (std::holds_alternative<type::underlying<type::Id::INT>::value>(value))
                            {
                                const auto value_type_id = type::Id::INT;
                                out_stream.write(reinterpret_cast<const char *>(&value_type_id), sizeof(type::Id));
                                const auto underlying_value = std::get<type::underlying<type::Id::INT>::value>(value);
                                out_stream.write(reinterpret_cast<const char *>(&underlying_value),
                                                 sizeof(type::underlying<type::Id::INT>::value));
                            }
                            else if (std::holds_alternative<type::underlying<type::Id::BIGINT>::value>(value))
                            {
                                const auto value_type_id = type::Id::BIGINT;
                                out_stream.write(reinterpret_cast<const char *>(&value_type_id), sizeof(type::Id));
                                const auto underlying_value =
                                    std::get<type::underlying<type::Id::BIGINT>::value>(value);
                                out_stream.write(reinterpret_cast<const char *>(&underlying_value),
                                                 sizeof(type::underlying<type::Id::BIGINT>::value));
                            }
                            else if (std::holds_alternative<type::underlying<type::Id::BOOL>::value>(value))
                            {
                                const auto value_type_id = type::Id::BOOL;
                                out_stream.write(reinterpret_cast<const char *>(&value_type_id), sizeof(type::Id));
                                const auto underlying_value = std::get<type::underlying<type::Id::BOOL>::value>(value);
                                out_stream.write(reinterpret_cast<const char *>(&underlying_value),
                                                 sizeof(type::underlying<type::Id::BOOL>::value));
                            }
                            else if (std::holds_alternative<type::underlying<type::Id::DATE>::value>(value))
                            {
                                const auto value_type_id = type::Id::DATE;
                                out_stream.write(reinterpret_cast<const char *>(&value_type_id), sizeof(type::Id));
                                const auto underlying_value = std::get<type::underlying<type::Id::DATE>::value>(value);
                                out_stream.write(reinterpret_cast<const char *>(&underlying_value),
                                                 sizeof(type::underlying<type::Id::DATE>::value));
                            }
                            else if (std::holds_alternative<std::string>(value))
                            {
                                const auto value_type_id = type::Id::CHAR;
                                out_stream.write(reinterpret_cast<const char *>(&value_type_id), sizeof(type::Id));
                                const auto &underlying_value = std::get<std::string>(value);
                                const auto underlying_value_size = std::uint64_t(underlying_value.size());
                                out_stream.write(reinterpret_cast<const char *>(&underlying_value_size),
                                                 sizeof(std::uint64_t));
                                out_stream.write(underlying_value.data(), underlying_value_size);
                            }

                            out_stream.write(reinterpret_cast<const char *>(&value_count), sizeof(std::uint64_t));
                        }
                    }
                }

                /// - Count Distinct
                const auto count_distinct = std::uint64_t(table.statistics().count_distinct(i));
                out_stream.write(reinterpret_cast<const char *>(&count_distinct), sizeof(std::uint64_t));
            }
            else
            {
                throw exception::ExecutionException{
                    fmt::format("Could not serialize schema: {} of table {}.", term.to_string(), table.name())};
            }
        }

        /// Statistics
        ///  - Count Rows
        const auto count_rows = std::uint64_t(table.statistics().count_rows());
        out_stream.write(reinterpret_cast<const char *>(&count_rows), sizeof(std::uint64_t));

        /// Data.
        const auto count_tiles = std::uint64_t(table.tiles().size());
        out_stream.write(reinterpret_cast<const char *>(&count_tiles), sizeof(std::uint64_t));

        const auto pax_tile_size = data::PaxTile::size(table.schema());
        for (auto tile_ptr : table.tiles())
        {
            auto *tile = tile_ptr.get<data::PaxTile>();

            const auto count_records = std::uint32_t(tile->size());
            out_stream.write(reinterpret_cast<const char *>(&count_records), sizeof(std::uint32_t));
            out_stream.write(reinterpret_cast<const char *>(tile->begin()), pax_tile_size);
        }

        out_stream << std::flush;
    }
}

void Serializer::deserialize(topology::Database &database, const std::string &file_name)
{
    auto in_stream = std::ifstream{file_name, std::ios::binary};
    if (in_stream.is_open())
    {
        in_stream.seekg(0U, std::ios::beg);

        auto count_tables = std::uint64_t{0U};
        in_stream.read(reinterpret_cast<char *>(&count_tables), sizeof(std::uint64_t));

        for (auto tid = 0U; tid < count_tables; ++tid)
        {
            /// Size and string of table name.
            auto table_name_size = std::uint64_t{0U};
            in_stream.read(reinterpret_cast<char *>(&table_name_size), sizeof(std::uint64_t));

            auto table_name = std::string(table_name_size, '\0');
            in_stream.read(table_name.data(), table_name_size);

            /// Terms.
            auto schema = topology::PhysicalSchema{};
            auto statistic_histograms = std::vector<std::unique_ptr<statistic::HistogramInterface>>{};
            auto statistic_count_distinct = std::vector<std::uint64_t>{};
            auto count_terms = std::uint64_t{0U};
            in_stream.read(reinterpret_cast<char *>(&count_terms), sizeof(std::uint64_t));

            for (auto term_id = 0U; term_id < count_terms; ++term_id)
            {
                /// Name of term.
                auto term_name_size = std::uint64_t{0U};
                in_stream.read(reinterpret_cast<char *>(&term_name_size), sizeof(std::uint64_t));

                auto term_name = std::string(term_name_size, '\0');
                in_stream.read(term_name.data(), term_name_size);

                auto type_id = std::uint32_t{0U};
                in_stream.read(reinterpret_cast<char *>(&type_id), sizeof(std::uint32_t));
                auto type = type::Type{static_cast<type::Id>(type_id)};
                if (static_cast<type::Id>(type_id) == type::Id::DECIMAL)
                {
                    auto precision = std::uint8_t{0U};
                    auto scale = std::uint8_t{0U};
                    in_stream.read(reinterpret_cast<char *>(&precision), sizeof(std::uint8_t));
                    in_stream.read(reinterpret_cast<char *>(&scale), sizeof(std::uint8_t));
                    type = type::Type::make_decimal(precision, scale);
                }
                else if (static_cast<type::Id>(type_id) == type::Id::CHAR)
                {
                    auto length = std::uint16_t{0U};
                    in_stream.read(reinterpret_cast<char *>(&length), sizeof(std::uint16_t));
                    type = type::Type::make_char(length);
                }

                auto is_null = std::uint8_t{0U};
                in_stream.read(reinterpret_cast<char *>(&is_null), sizeof(std::uint8_t));

                auto is_primary_key = std::uint8_t{0U};
                in_stream.read(reinterpret_cast<char *>(&is_primary_key), sizeof(std::uint8_t));

                schema.emplace_back(expression::Term::make_attribute(std::move(term_name)), type,
                                    static_cast<bool>(is_null), static_cast<bool>(is_primary_key));

                /// Read statistics.
                auto has_histogram = std::uint8_t{0U};
                in_stream.read(reinterpret_cast<char *>(&has_histogram), sizeof(std::uint8_t));
                if (has_histogram > 0U)
                {
                    auto histogram_type = statistic::HistogramInterface::Type::EquiDepth;
                    in_stream.read(reinterpret_cast<char *>(&histogram_type),
                                   sizeof(statistic::HistogramInterface::Type));

                    if (histogram_type == statistic::HistogramInterface::Type::EquiDepth)
                    {
                        auto count = std::uint64_t{0U};
                        in_stream.read(reinterpret_cast<char *>(&count), sizeof(std::uint64_t));
                        auto depth = std::uint64_t{0U};
                        in_stream.read(reinterpret_cast<char *>(&depth), sizeof(std::uint64_t));
                        auto lower_key = std::uint64_t{0U};
                        in_stream.read(reinterpret_cast<char *>(&lower_key), sizeof(std::uint64_t));
                        auto upper_key = std::uint64_t{0U};
                        in_stream.read(reinterpret_cast<char *>(&upper_key), sizeof(std::uint64_t));

                        auto count_bins = std::uint64_t{0U};
                        in_stream.read(reinterpret_cast<char *>(&count_bins), sizeof(std::uint64_t));

                        auto bins = std::vector<statistic::EquiDepthBin>{};
                        bins.reserve(count_bins);
                        for (auto bin_id = 0U; bin_id < count_bins; ++bin_id)
                        {
                            auto bin_lower = std::int64_t{0U};
                            in_stream.read(reinterpret_cast<char *>(&bin_lower), sizeof(std::int64_t));
                            auto bin_upper = std::int64_t{0U};
                            in_stream.read(reinterpret_cast<char *>(&bin_upper), sizeof(std::int64_t));
                            auto bin_count = std::uint64_t{0U};
                            in_stream.read(reinterpret_cast<char *>(&bin_count), sizeof(std::uint64_t));

                            bins.emplace_back(statistic::EquiDepthBin{bin_lower, bin_upper, bin_count});
                        }

                        statistic_histograms.emplace_back(std::make_unique<statistic::EquiDepthHistogram>(
                            depth, lower_key, upper_key, count, std::move(bins)));
                    }
                    else if (histogram_type == statistic::HistogramInterface::Type::Singleton)
                    {
                        auto bins = std::map<data::Value::value_t, std::uint64_t>{};
                        auto count = std::uint64_t{0U};
                        in_stream.read(reinterpret_cast<char *>(&count), sizeof(std::uint64_t));

                        auto bin_count = std::uint64_t{0U};
                        in_stream.read(reinterpret_cast<char *>(&bin_count), sizeof(std::uint64_t));

                        for (auto bin_id = 0U; bin_id < bin_count; ++bin_id)
                        {
                            auto value = data::Value::value_t{0};

                            auto value_type_id = type::Id::UNKNOWN;
                            in_stream.read(reinterpret_cast<char *>(&value_type_id), sizeof(type::Id));

                            if (value_type_id == type::Id::INT)
                            {
                                auto int_value = type::underlying<type::Id::INT>::value{0U};
                                in_stream.read(reinterpret_cast<char *>(&int_value),
                                               sizeof(type::underlying<type::Id::INT>::value));
                                value = data::Value::value_t{int_value};
                            }
                            else if (value_type_id == type::Id::BIGINT)
                            {
                                auto bigint_value = type::underlying<type::Id::BIGINT>::value{0U};
                                in_stream.read(reinterpret_cast<char *>(&bigint_value),
                                               sizeof(type::underlying<type::Id::BIGINT>::value));
                                value = data::Value::value_t{bigint_value};
                            }
                            else if (value_type_id == type::Id::BOOL)
                            {
                                auto bool_value = type::underlying<type::Id::BOOL>::value{false};
                                in_stream.read(reinterpret_cast<char *>(&bool_value),
                                               sizeof(type::underlying<type::Id::BOOL>::value));
                                value = data::Value::value_t{bool_value};
                            }
                            else if (value_type_id == type::Id::DATE)
                            {
                                auto date_value = type::underlying<type::Id::DATE>::value::data_t{0U};
                                in_stream.read(reinterpret_cast<char *>(&date_value),
                                               sizeof(type::underlying<type::Id::DATE>::value::data_t));
                                value = data::Value::value_t{type::underlying<type::Id::DATE>::value{date_value}};
                            }
                            else if (value_type_id == type::Id::CHAR)
                            {
                                auto string_size = std::uint64_t(0U);
                                in_stream.read(reinterpret_cast<char *>(&string_size), sizeof(std::uint64_t));
                                auto string = std::string(string_size, '\0');
                                in_stream.read(string.data(), string_size);
                                value = data::Value::value_t{std::move(string)};
                            }

                            auto value_count = std::uint64_t{0U};
                            in_stream.read(reinterpret_cast<char *>(&value_count), sizeof(std::uint64_t));

                            bins.insert(std::make_pair(std::move(value), value_count));
                        }

                        statistic_histograms.emplace_back(
                            std::make_unique<statistic::SingletonHistogram>(count, std::move(bins)));
                    }
                }
                else
                {
                    statistic_histograms.emplace_back(nullptr);
                }

                auto count_distinct = std::uint64_t{0U};
                in_stream.read(reinterpret_cast<char *>(&count_distinct), sizeof(std::uint64_t));
                statistic_count_distinct.emplace_back(count_distinct);
            }

            auto count_rows = std::uint64_t{0U};
            in_stream.read(reinterpret_cast<char *>(&count_rows), sizeof(std::uint64_t));

            /// Create the table.
            auto &table = database.insert(std::move(table_name), std::move(schema));
            const auto record_size = table.schema().row_size();

            /// Update table statistics.
            for (auto i = 0U; i < table.schema().size(); ++i)
            {
                table.statistics().histogram(i) = std::move(statistic_histograms[i]);
                table.statistics().count_distinct()[i] = statistic_count_distinct[i];
            }
            table.statistics().count_rows(count_rows);

            /// Deserialize the data.
            auto count_tiles = std::uint64_t{0U};
            in_stream.read(reinterpret_cast<char *>(&count_tiles), sizeof(std::uint64_t));

            const auto pax_tile_size = data::PaxTile::size(table.schema());
            auto *tmp_tile = new (std::malloc(sizeof(data::PaxTile) + pax_tile_size))
                data::PaxTile{data::AllocationType::TemporaryResource, table.schema()};

            for (auto tile_id = 0U; tile_id < count_tiles; ++tile_id)
            {
                auto count_records = std::uint32_t{0U};
                in_stream.read(reinterpret_cast<char *>(&count_records), sizeof(std::uint32_t));
                tmp_tile->size(count_records);
                in_stream.read(reinterpret_cast<char *>(tmp_tile->begin()), pax_tile_size);

                table.emplace_back(tmp_tile);
            }

            std::free(tmp_tile);
        }
    }
}