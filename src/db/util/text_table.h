#pragma once
#include <algorithm>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

namespace db::util {
/**
 * Formats the given data and prints it as table.
 */
class TextTable
{
    friend std::ostream &operator<<(std::ostream &stream, const TextTable &text_table);

public:
    [[nodiscard]] static TextTable from_json(std::vector<std::string> &&header_values,
                                             std::vector<std::string> &&column_keys, nlohmann::json &&values);

    TextTable() = default;
    TextTable(std::vector<std::string> &&header_values) { header(std::move(header_values)); }
    ~TextTable() = default;

    /**
     * Set header for the table.
     *
     * @param row_values Header values.
     */
    void header(std::vector<std::string> &&row_values);

    /**
     * Adds a row to the table.
     *
     * @param row_values Row.
     */
    void emplace_back(std::vector<std::string> &&row_values) { _table_rows.emplace_back(std::move(row_values)); }

    /**
     * Clears the table.
     */
    void clear() { _table_rows.clear(); }

    /**
     * @return True, when no row or header was added.
     */
    [[nodiscard]] bool empty() const { return _table_rows.empty(); }

    /**
     * Reserves space for the given amount of rows.
     * @param count Amount of rows to reserve space for.
     */
    void reserve(const std::size_t count) noexcept { _table_rows.reserve(count); }

    /**
     * @return A string representing this table.
     */
    [[nodiscard]] std::string to_string() const noexcept;

private:
    std::uint8_t _count_head_rows{0U};
    std::vector<std::vector<std::string>> _table_rows;

    /**
     * Calculates the printed length of a given string.
     * @param input String to calculate length.
     * @return Number of printed characters.
     */
    [[nodiscard]] static std::size_t printed_length(const std::string &input);

    /**
     * Calculates the maximal length for each column.
     *
     * @return List of length per column.
     */
    [[nodiscard]] std::vector<std::size_t> length_per_column() const;

    /**
     * Prints a separator line to the given output stream.
     *
     * @param stream Output stream the line should be printed to.
     * @param column_lengths List of maximal length per column.
     * @return The given output stream.
     */
    static std::ostream &print_separator_line(std::ostream &stream, const std::vector<std::size_t> &column_lengths,
                                              std::string &&left, std::string &&right, std::string &&separator);

    /**
     * Prints a row to the given output stream.
     *
     * @param stream Output stream the row should be printed to.
     * @param column_lengths List of maximal length per column.
     * @param row The row that should be printed.
     * @return The given output stream.
     */
    static std::ostream &print_row(std::ostream &stream, const std::vector<std::size_t> &column_lengths,
                                   const std::vector<std::string> &row);
};
} // namespace db::util