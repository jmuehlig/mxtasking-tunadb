#include "query_result.h"
#include <db/config.h>
#include <db/util/text_table.h>
#include <sstream>

using namespace db::io;

void QueryResult::serialize(const std::size_t needed_size, std::byte *data)
{
    auto data_ptr = std::uintptr_t(data);

    // Size of serialized bytes.
    *reinterpret_cast<std::size_t *>(data_ptr) = needed_size;
    data_ptr += sizeof(std::size_t);

    // Number of attributes in schema.
    *reinterpret_cast<std::uint16_t *>(data_ptr) = this->_schema.size();
    data_ptr += sizeof(std::uint16_t);

    // Attributes.
    for (auto i = 0U; i < this->_schema.size(); ++i)
    {
        const auto &term = this->_schema.term(i);
        const auto &type = this->_schema.type(i);
        const auto is_nullable = this->_schema.is_null(i);
        const auto is_primary_key = this->_schema.is_primary_key(i);

        // Length of name and name itself.
        const auto attribute = term.to_string();
        *reinterpret_cast<std::uint16_t *>(data_ptr) = attribute.size();
        data_ptr += sizeof(std::uint16_t);
        std::memcpy(reinterpret_cast<void *>(data_ptr), attribute.c_str(), attribute.size());
        data_ptr += attribute.size();

        // Type.
        *reinterpret_cast<type::Type *>(data_ptr) = type;
        data_ptr += sizeof(type::Type);

        // Nullable.
        *reinterpret_cast<bool *>(data_ptr) = is_nullable;
        data_ptr += sizeof(bool);

        // Primary key.
        *reinterpret_cast<bool *>(data_ptr) = is_primary_key;
        data_ptr += sizeof(bool);
    }

    // Order vector
    *reinterpret_cast<std::uint16_t *>(data_ptr) = this->_schema.order().size();
    data_ptr += sizeof(std::uint16_t);
    for (const auto order : this->_schema.order())
    {
        *reinterpret_cast<std::uint16_t *>(data_ptr) = order;
        data_ptr += sizeof(std::uint16_t);
    }

    // Number of records.
    *reinterpret_cast<std::uint64_t *>(data_ptr) = this->_count_records;
    data_ptr += sizeof(std::uint64_t);

    // Records.
    for (auto column_id = 0U; column_id < this->_schema.size(); ++column_id)
    {
        const auto pax_offset = this->_schema.pax_offset(column_id);
        const auto type_size = this->_schema.type(column_id).size();
        for (const auto &record_set : this->_records)
        {
            auto *tile = record_set.tile().get<data::PaxTile>();

            const auto start = std::uintptr_t(tile->begin()) + pax_offset;
            /// All records are visible, copy the whole column.

            const auto data_size = type_size * tile->size();
            std::memcpy(reinterpret_cast<void *>(data_ptr), reinterpret_cast<void *>(start), data_size);
            data_ptr += data_size;
        }
    }
}

std::size_t QueryResult::serialized_size() const noexcept
{
    auto needed_size =
        sizeof(std::size_t)     // Space for the size in bytes of the serialized data (needed for deserialization).
        + sizeof(std::uint16_t) // Space for number of attributes in schema.
        + sizeof(type::Type) * this->_schema.size()            // Space for types (types + name = attribute).
        + sizeof(bool) * this->_schema.size()                  // Space for NULL flags.
        + sizeof(bool) * this->_schema.size()                  // Space for PK flags.
        + sizeof(std::uint64_t)                                // Space for number of records.
        + sizeof(std::uint16_t)                                // Space for number of elements in order vector
        + sizeof(std::uint16_t) * this->_schema.order().size() // Space for order vector.
        + this->_count_records * this->_schema.row_size()      // Space for records.
        ;

    // Space for attribute names.
    for (const auto &term : this->_schema.terms())
    {
        needed_size += sizeof(std::uint16_t) + term.to_string().size();
    }

    return needed_size;
}

QueryResult QueryResult::deserialize(const std::byte *data)
{
    auto data_ptr = std::uintptr_t(data);

    // Skip the size of serialized data.
    // const auto needed_size = *reinterpret_cast<std::size_t *>(data_ptr);
    data_ptr += sizeof(std::size_t);

    // Number of attributes.
    const auto count_attributes = *reinterpret_cast<std::uint16_t *>(data_ptr);
    data_ptr += sizeof(std::uint16_t);

    // Build attributes.
    topology::PhysicalSchema schema;
    for (auto i = 0U; i < count_attributes; ++i)
    {
        const auto name_length = *reinterpret_cast<std::uint16_t *>(data_ptr);
        data_ptr += sizeof(std::uint16_t);
        auto name = std::string{reinterpret_cast<char *>(data_ptr), name_length};
        data_ptr += name_length;

        auto type = *reinterpret_cast<type::Type *>(data_ptr);
        data_ptr += sizeof(type::Type);

        const auto is_null = *reinterpret_cast<bool *>(data_ptr);
        data_ptr += sizeof(bool);

        const auto is_primary_key = *reinterpret_cast<bool *>(data_ptr);
        data_ptr += sizeof(bool);

        schema.emplace_back(expression::Term::make_attribute(std::move(name)), type, is_null, is_primary_key);
    }

    // Order.
    std::vector<std::uint16_t> order;
    const auto order_length = *reinterpret_cast<std::uint16_t *>(data_ptr);
    data_ptr += sizeof(std::uint16_t);
    order.reserve(order_length);
    for (auto i = 0U; i < order_length; ++i)
    {
        order.emplace_back(*reinterpret_cast<std::uint16_t *>(data_ptr));
        data_ptr += sizeof(std::uint16_t);
    }
    schema.order(std::move(order));

    auto result = QueryResult{std::move(schema)};

    // Number of records.
    const auto count_records = *reinterpret_cast<std::uint64_t *>(data_ptr);
    data_ptr += sizeof(std::uint64_t);

    /// Calculate the offsets for every column.
    auto column_offsets = std::vector<std::uint64_t>{};
    column_offsets.reserve(result._schema.size());
    for (auto column_id = 0U; column_id < result._schema.size(); ++column_id)
    {
        if (column_offsets.empty())
        {
            column_offsets.emplace_back(0U);
        }
        else
        {
            const auto last_offset = column_offsets.back();
            const auto last_size = result._schema.type(column_id - 1U).size() * count_records;
            column_offsets.emplace_back(last_offset + last_size);
        }
    }

    auto records = std::vector<execution::RecordSet>{};
    records.reserve(count_records / config::tuples_per_tile() + 1U);
    records.emplace_back(execution::RecordSet::make_client_record_set(result.schema()));

    auto deserialized_records = 0ULL;
    while (deserialized_records < count_records)
    {
        const auto count_tile_records =
            std::min<std::uint64_t>(count_records - deserialized_records, config::tuples_per_tile());

        auto *tile = records.back().tile().get<data::PaxTile>();
        const auto [tile_index, allocated_records] = tile->allocate(count_tile_records);
        if (allocated_records == 0U)
        {
            records.emplace_back(execution::RecordSet::make_client_record_set(result.schema()));
            continue;
        }

        for (auto column_id = 0U; column_id < result._schema.size(); ++column_id)
        {
            const auto type_size = result._schema.type(column_id).size();

            const auto destination =
                std::uintptr_t(tile->begin()) + result._schema.pax_offset(column_id) + type_size * tile_index;
            const auto source = data_ptr + column_offsets[column_id] + type_size * deserialized_records;
            const auto size = type_size * allocated_records;
            std::memcpy(reinterpret_cast<void *>(destination), reinterpret_cast<void *>(source), size);
        }

        deserialized_records += allocated_records;
    }

    result.add(std::move(records));

    return result;
}

std::string QueryResult::to_string() const noexcept
{
    auto text_table = util::TextTable{};
    text_table.reserve(this->_records.size() * config::tuples_per_tile() + 2U);

    /// Schema name as header.
    auto column_names = std::vector<std::string>{};
    column_names.reserve(this->_schema.order().size());
    for (const auto index : this->_schema.order())
    {
        column_names.emplace_back(this->_schema.term(index).to_string());
    }
    text_table.header(std::move(column_names));

    /// Schema type as header.
    auto column_types = std::vector<std::string>{};
    column_types.reserve(this->_schema.order().size());
    for (const auto index : this->_schema.order())
    {
        column_types.emplace_back(this->_schema.type(index).to_string());
    }
    text_table.header(std::move(column_types));

    /// Records as rows.
    for (const auto &record_set : this->_records)
    {
        auto *tile = record_set.tile().get<data::PaxTile>();
        for (auto i = 0U; i < tile->size(); ++i)
        {
            auto record_view = tile->view(i);
            auto row = std::vector<std::string>{};
            row.reserve(this->_schema.order().size());
            for (const auto index : this->_schema.order())
            {
                row.emplace_back(record_view.get(index).to_string());
            }
            text_table.emplace_back(std::move(row));
        }
    }

    return text_table.to_string();
}

nlohmann::json QueryResult::to_json() const noexcept
{
    nlohmann::json data;

    /// Add schema.
    for (const auto index : this->_schema.order())
    {
        auto column = nlohmann::json{};
        column["name"] = this->_schema.term(index).to_string();
        column["type"] = this->_schema.type(index).to_string();
        data["schema"].emplace_back(std::move(column));
    }

    /// Records as rows.
    for (const auto &record_set : this->_records)
    {
        auto *tile = record_set.tile().get<data::PaxTile>();
        for (auto i = 0U; i < tile->size(); ++i)
        {
            auto record_view = tile->view(i);
            auto row = nlohmann::json{};
            for (const auto index : this->_schema.order())
            {
                const auto type = this->_schema.type(index);
                if (type == type::Id::BIGINT)
                {
                    row.emplace_back(record_view.get(index).get<type::Id::BIGINT>());
                }
                else if (type == type::Id::INT)
                {
                    row.emplace_back(record_view.get(index).get<type::Id::INT>());
                }
                else if (type == type::Id::BOOL)
                {
                    row.emplace_back(record_view.get(index).get<type::Id::BOOL>());
                }
                else if (type == type::Id::DECIMAL)
                {
                    row.emplace_back(record_view.get(index).to_string());
                }
                else
                {
                    row.emplace_back(record_view.get(index).to_string());
                }
            }

            data["rows"].emplace_back(std::move(row));
        }
    }

    return data;
}