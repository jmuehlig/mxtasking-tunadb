#pragma once
#include "value.h"
#include <cstdlib>
#include <cstring>
#include <db/topology/physical_schema.h>
#include <db/type/type.h>
namespace db::data {
class RowRecordView
{
public:
    constexpr RowRecordView(const topology::PhysicalSchema &schema, void *data) noexcept : _schema(schema), _data(data)
    {
    }
    RowRecordView(RowRecordView &&) noexcept = default;
    RowRecordView(const RowRecordView &) noexcept = default;
    ~RowRecordView() noexcept = default;

    [[nodiscard]] const topology::PhysicalSchema &schema() const noexcept { return _schema; }
    [[nodiscard]] void *data() const noexcept { return _data; }

    void set(const std::uint16_t index, const type::underlying<type::INT>::value value) noexcept
    {
        const auto offset = _schema.row_offset(index);
        *reinterpret_cast<type::underlying<type::INT>::value *>(std::uintptr_t(_data) + offset) = value;
    }

    void set(const std::uint16_t index, const type::underlying<type::BIGINT>::value value) noexcept
    {
        const auto offset = _schema.row_offset(index);
        *reinterpret_cast<type::underlying<type::BIGINT>::value *>(std::uintptr_t(_data) + offset) = value;
    }

    void set(const std::uint16_t index, const type::underlying<type::DATE>::value value) noexcept
    {
        const auto offset = _schema.row_offset(index);
        *reinterpret_cast<type::underlying<type::DATE>::value *>(std::uintptr_t(_data) + offset) = value;
    }

    void set(const std::uint16_t index, const type::underlying<type::BOOL>::value value) noexcept
    {
        const auto offset = _schema.row_offset(index);
        *reinterpret_cast<type::underlying<type::BOOL>::value *>(std::uintptr_t(_data) + offset) = value;
    }

    void set(const std::uint16_t index, const type::Decimal value) noexcept
    {
        const auto offset = _schema.row_offset(index);
        *reinterpret_cast<type::underlying<type::DECIMAL>::value *>(std::uintptr_t(_data) + offset) = value.data();
    }

    void set(const std::uint16_t index, std::string &&value) noexcept
    {
        const auto offset = _schema.row_offset(index);
        const auto length = std::min(value.length(), std::size_t(_schema.type(index).char_description().length()));
        std::memmove(reinterpret_cast<type::underlying<type::CHAR> *>(std::uintptr_t(_data) + offset), value.data(),
                     length);
        const auto row_length = _schema.type(index).char_description().length();
        if (length < row_length)
        {
            std::memset(reinterpret_cast<void *>(std::uintptr_t(_data) + offset + length), '\0', row_length - length);
        }
    }

    void set(const std::uint16_t index, const std::string &value) noexcept
    {
        const auto offset = _schema.row_offset(index);
        const auto length = std::min(value.length(), std::size_t(_schema.type(index).char_description().length()));
        std::memcpy(reinterpret_cast<type::underlying<type::CHAR> *>(std::uintptr_t(_data) + offset), value.data(),
                    length);
        if (length < _schema.type(index).char_description().length())
        {
            std::memset(reinterpret_cast<void *>(std::uintptr_t(_data) + offset + length), '\0', sizeof(char));
        }
    }

    void set(const std::uint16_t index, Value &&value) noexcept { this->set(index, std::move(value.value())); }

    void set(const std::uint16_t index, Value::value_t &&value) noexcept
    {
        std::visit([this, index](auto &&v) { this->set(index, std::move(v)); }, std::move(value));
    }

    void set(const std::uint16_t index, const Value::value_t &value) noexcept
    {
        std::visit([this, index](const auto &v) { this->set(index, v); }, value);
    }

    template <bool HARD_COPY = false> [[nodiscard]] Value get(const std::uint16_t index) const
    {
        const auto offset = _schema.row_offset(index);
        const auto &type = _schema.type(index);
        if (type == type::Id::INT)
        {
            return Value{type,
                         *reinterpret_cast<type::underlying<type::Id::INT>::value *>(std::uintptr_t(_data) + offset)};
        }

        if (type == type::Id::BIGINT)
        {
            return Value{
                type, *reinterpret_cast<type::underlying<type::Id::BIGINT>::value *>(std::uintptr_t(_data) + offset)};
        }

        if (type == type::Id::DECIMAL)
        {
            return Value{
                type, *reinterpret_cast<type::underlying<type::Id::DECIMAL>::value *>(std::uintptr_t(_data) + offset)};
        }

        if (type == type::Id::DATE)
        {
            return Value{type,
                         *reinterpret_cast<type::underlying<type::Id::DATE>::value *>(std::uintptr_t(_data) + offset)};
        }

        if (type == type::Id::BOOL)
        {
            return Value{type,
                         *reinterpret_cast<type::underlying<type::Id::BOOL>::value *>(std::uintptr_t(_data) + offset)};
        }

        if (type == type::Id::CHAR)
        {
            auto *string = reinterpret_cast<type::underlying<type::Id::CHAR>::value *>(std::uintptr_t(_data) + offset);

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

    template <type::Id T> [[nodiscard]] typename type::view<T>::value view(const std::uint16_t offset) const
    {
        auto *data = reinterpret_cast<typename type::underlying<T>::value *>(std::uintptr_t(_data) + offset);

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
    void *_data;
};
} // namespace db::data