#pragma once
#include "value.h"
#include <cstdlib>
#include <cstring>
#include <db/topology/physical_schema.h>
#include <db/type/type.h>

namespace db::data {
class PaxRecordView
{
public:
    PaxRecordView(const topology::PhysicalSchema &schema, void *tile_data, const std::uint64_t index) noexcept
        : _schema(schema), _tile_data(std::uintptr_t(tile_data)), _index(index)
    {
    }
    PaxRecordView(PaxRecordView &&) noexcept = default;
    PaxRecordView(const PaxRecordView &) noexcept = default;
    ~PaxRecordView() noexcept = default;

    [[nodiscard]] const topology::PhysicalSchema &schema() const noexcept { return _schema; }

    void set(const std::uint16_t column_index, const type::underlying<type::INT>::value value) noexcept
    {
        const auto offset = _schema.pax_offset(column_index) + _index * _schema.type(column_index).size();
        *reinterpret_cast<type::underlying<type::INT>::value *>(_tile_data + offset) = value;
    }

    void set(const std::uint16_t column_index, const type::underlying<type::BIGINT>::value value) noexcept
    {
        const auto offset = _schema.pax_offset(column_index) + _index * _schema.type(column_index).size();
        *reinterpret_cast<type::underlying<type::BIGINT>::value *>(_tile_data + offset) = value;
    }

    void set(const std::uint16_t column_index, const type::underlying<type::DATE>::value value) noexcept
    {
        const auto offset = _schema.pax_offset(column_index) + _index * _schema.type(column_index).size();
        *reinterpret_cast<type::underlying<type::DATE>::value *>(_tile_data + offset) = value;
    }

    void set(const std::uint16_t column_index, const type::underlying<type::BOOL>::value value) noexcept
    {
        const auto offset = _schema.pax_offset(column_index) + _index * _schema.type(column_index).size();
        *reinterpret_cast<type::underlying<type::BOOL>::value *>(_tile_data + offset) = value;
    }

    void set(const std::uint16_t column_index, const type::Decimal value) noexcept
    {
        const auto offset = _schema.pax_offset(column_index) + _index * _schema.type(column_index).size();
        *reinterpret_cast<type::underlying<type::DECIMAL>::value *>(_tile_data + offset) = value.data();
    }

    void set(const std::uint16_t column_index, std::string &&value) noexcept
    {
        const auto type = _schema.type(column_index);
        const auto offset = _schema.pax_offset(column_index) + _index * type.size();
        const auto length = std::min(value.length(), std::size_t(type.char_description().length()));
        std::memmove(reinterpret_cast<type::underlying<type::CHAR> *>(_tile_data + offset), value.data(), length);
        const auto row_length = type.char_description().length();
        if (length < row_length)
        {
            std::memset(reinterpret_cast<void *>(_tile_data + offset + length), '\0', row_length - length);
        }
    }

    void set(const std::uint16_t column_index, const std::string &value) noexcept
    {
        const auto type = _schema.type(column_index);
        const auto offset = _schema.pax_offset(column_index) + _index * type.size();
        const auto length = std::min(value.length(), std::size_t(type.char_description().length()));
        std::memcpy(reinterpret_cast<type::underlying<type::CHAR> *>(_tile_data + offset), value.data(), length);
        if (length < type.char_description().length())
        {
            std::memset(reinterpret_cast<void *>(_tile_data + offset + length), '\0', sizeof(char));
        }
    }

    void set(const std::uint16_t column_index, Value &&value) noexcept
    {
        this->set(column_index, std::move(value.value()));
    }

    void set(const std::uint16_t column_index, Value::value_t &&value) noexcept
    {
        std::visit([this, column_index](auto &&v) { this->set(column_index, std::move(v)); }, std::move(value));
    }

    void set(const std::uint16_t column_index, const Value::value_t &value) noexcept
    {
        std::visit([this, column_index](const auto &v) { this->set(column_index, v); }, value);
    }

    template <bool HARD_COPY = false> [[nodiscard]] Value get(const std::uint16_t column_index) const
    {
        const auto type = _schema.type(column_index);
        const auto offset = _schema.pax_offset(column_index) + _index * type.size();
        if (type == type::Id::INT)
        {
            return Value{type, *reinterpret_cast<type::underlying<type::Id::INT>::value *>(_tile_data + offset)};
        }

        if (type == type::Id::BIGINT)
        {
            return Value{type, *reinterpret_cast<type::underlying<type::Id::BIGINT>::value *>(_tile_data + offset)};
        }

        if (type == type::Id::DECIMAL)
        {
            return Value{type, *reinterpret_cast<type::underlying<type::Id::DECIMAL>::value *>(_tile_data + offset)};
        }

        if (type == type::Id::DATE)
        {
            return Value{type, *reinterpret_cast<type::underlying<type::Id::DATE>::value *>(_tile_data + offset)};
        }

        if (type == type::Id::BOOL)
        {
            return Value{type, *reinterpret_cast<type::underlying<type::Id::BOOL>::value *>(_tile_data + offset)};
        }

        if (type == type::Id::CHAR)
        {
            auto *string = reinterpret_cast<type::underlying<type::Id::CHAR>::value *>(_tile_data + offset);

            if constexpr (HARD_COPY)
            {
                return {type, std::string(string, type.size())};
            }
            else
            {
                return {type, std::string_view(string, type.size())};
            }
        }

        return {type, 0};
    }

    template <type::Id T> [[nodiscard]] typename type::view<T>::value view(const std::uint16_t column_index) const
    {
        const auto type = _schema.type(column_index);
        const auto offset = _schema.pax_offset(column_index) + _index * type.size();
        auto *data = reinterpret_cast<typename type::underlying<T>::value *>(_tile_data + offset);

        if constexpr (T == type::Id::CHAR)
        {
            return typename type::view<T>::value{data};
        }
        else
        {
            return typename type::view<T>::value{*data};
        }
    }

private:
    /// Layout of the tuple.
    const topology::PhysicalSchema &_schema;

    /// Location where the data of the record is really stored.
    std::uintptr_t _tile_data;

    /// Index of the tuple within the tile.
    std::uint64_t _index;
};
} // namespace db::data