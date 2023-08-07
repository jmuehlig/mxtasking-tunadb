#include "text_table.h"
#include <cctype>
#include <db/util/string.h>
#include <fmt/core.h>

using namespace db::util;

TextTable TextTable::from_json(std::vector<std::string> &&header_values, std::vector<std::string> &&column_keys,
                               nlohmann::json &&values)
{
    auto table = TextTable{std::move(header_values)};
    table.reserve(values.size());

    for (auto &row : values)
    {
        auto table_row = std::vector<std::string>{};
        table_row.reserve(column_keys.size());

        for (const auto &key : column_keys)
        {
            if (row.contains(key) == false)
            {
                table_row.emplace_back("");
            }
            else
            {
                auto &value = row[key];
                if (value.is_string())
                {
                    table_row.emplace_back(value.get<std::string>());
                }
                else if (value.is_number() && value.is_number_unsigned())
                {
                    table_row.emplace_back(util::string::shorten_number(value.get<std::uint64_t>()));
                }
                else if (value.is_number() && value.is_number_integer())
                {
                    table_row.emplace_back(util::string::shorten_number(value.get<std::int64_t>()));
                }
                else if (value.is_number() && value.is_number_float())
                {
                    table_row.emplace_back(util::string::shorten_number(value.get<float>()));
                }
                else
                {
                    table_row.emplace_back("(no value)");
                }
            }
        }

        table.emplace_back(std::move(table_row));
    }

    return table;
}

void TextTable::header(std::vector<std::string> &&row_values)
{
    if (this->_table_rows.empty())
    {
        this->_table_rows.emplace_back(std::move(row_values));
    }
    else
    {
        this->_table_rows.insert(this->_table_rows.begin() + _count_head_rows, std::move(row_values));
    }

    ++_count_head_rows;
}

std::vector<std::size_t> TextTable::length_per_column() const
{
    if (_table_rows.empty())
    {
        return {};
    }

    auto column_lengths = std::vector<std::size_t>{};
    column_lengths.resize(this->_table_rows.front().size(), 0U);

    for (const auto &row : this->_table_rows)
    {
        for (auto i = 0U; i < row.size(); i++)
        {
            column_lengths[i] = std::max(column_lengths[i], printed_length(row[i]));
        }
    }

    return column_lengths;
}

std::ostream &TextTable::print_separator_line(std::ostream &stream, const std::vector<std::size_t> &column_lengths,
                                              std::string &&left, std::string &&right, std::string &&separator)
{
    stream << left;

    for (auto column = 0U; column < column_lengths.size(); ++column)
    {
        if (column != 0U)
        {
            stream << separator;
        }

        std::fill_n(std::ostream_iterator<std::string>(stream), column_lengths[column] + 2U, "─");
    }

    return stream << right << "\n";
}

std::ostream &TextTable::print_row(std::ostream &stream, const std::vector<std::size_t> &column_lengths,
                                   const std::vector<std::string> &row)
{
    for (auto i = 0U; i < row.size(); i++)
    {
        const auto &cell = row[i];
        const auto count_spaces = column_lengths[i] - printed_length(cell);
        stream << fmt::format("│ {}{} ", cell, std::string(count_spaces, ' '));
    }

    return stream << "│\n";
}

std::size_t TextTable::printed_length(const std::string &input)
{
    const auto print_size = std::count_if(input.begin(), input.end(),
                                          [](const std::uint8_t c) { return std::isprint(c) || std::iswprint(c); });

    return input.size() - ((input.size() - print_size) / 2U);
}

std::string TextTable::to_string() const noexcept
{
    if (this->_table_rows.empty())
    {
        return "";
    }

    std::stringstream stream;

    const auto length_per_column = this->length_per_column();

    TextTable::print_separator_line(stream, length_per_column, "┌", "┐", "┬");
    for (auto i = 0U; i < this->_count_head_rows; ++i)
    {
        TextTable::print_row(stream, length_per_column, this->_table_rows[i]);
    }
    TextTable::print_separator_line(stream, length_per_column, "├", "┤", "┼");

    for (auto i = std::uint64_t(this->_count_head_rows); i < this->_table_rows.size(); ++i)
    {
        TextTable::print_row(stream, length_per_column, this->_table_rows[i]);
    }

    TextTable::print_separator_line(stream, length_per_column, "└", "┘", "┴");

    stream.flush();

    return stream.str();
}

namespace db::util {
std::ostream &operator<<(std::ostream &stream, const TextTable &text_table)
{
    return stream << text_table.to_string() << std::flush;
}
} // namespace db::util
