#pragma once
#include <cstdlib>
#include <db/execution/record_token.h>
#include <db/topology/physical_schema.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <utility>
#include <vector>

namespace db::io {
class QueryResult
{
public:
    static QueryResult deserialize(const std::byte *data);

    explicit QueryResult(const topology::PhysicalSchema &schema) noexcept : _schema(schema) {}
    explicit QueryResult(topology::PhysicalSchema &&schema) noexcept : _schema(std::move(schema)) {}
    QueryResult(QueryResult &&) noexcept = default;
    ~QueryResult() noexcept = default;

    void add(execution::RecordSet &&records)
    {
        _count_records += records.tile().get<data::PaxTile>()->size();
        _records.emplace_back(std::move(records));
    }

    void add(std::vector<execution::RecordSet> &&records)
    {
        _count_records += std::accumulate(records.cbegin(), records.cend(), 0ULL, [](const auto &sum, const auto &set) {
            return sum + set.tile().template get<data::PaxTile>()->size();
        });
        std::move(records.begin(), records.end(), std::back_inserter(_records));
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const noexcept { return _schema; }
    [[nodiscard]] const std::vector<execution::RecordSet> &records() const noexcept { return _records; }
    [[nodiscard]] std::uint64_t count_records() const noexcept { return _count_records; }
    [[nodiscard]] bool empty() const noexcept { return _count_records == 0U; }

    void serialize(const std::size_t size, std::byte *data);

    [[nodiscard]] std::string to_string() const noexcept;
    [[nodiscard]] nlohmann::json to_json() const noexcept;

    [[nodiscard]] std::size_t serialized_size() const noexcept;

private:
    topology::PhysicalSchema _schema;
    std::vector<execution::RecordSet> _records;
    std::uint64_t _count_records{0U};
};
} // namespace db::io