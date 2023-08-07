#pragma once

#include <db/topology/logical_schema.h>

namespace db::plan::logical {
class Relation
{
public:
    Relation() = default;
    Relation(Relation &&) noexcept = default;
    Relation(const Relation &) = default;
    explicit Relation(topology::LogicalSchema &&schema) noexcept : _schema(std::move(schema)) {}
    explicit Relation(const topology::LogicalSchema &schema) : _schema(schema) {}
    Relation(topology::LogicalSchema &&schema, const std::uint64_t cardinality) noexcept
        : _schema(std::move(schema)), _cardinality(cardinality)
    {
    }

    ~Relation() = default;

    Relation &operator=(const Relation &) = default;
    Relation &operator=(Relation &&) = default;

    void clear()
    {
        _schema.clear();
        _cardinality = 0U;
    }

    [[nodiscard]] const topology::LogicalSchema &schema() const noexcept { return _schema; }
    [[nodiscard]] topology::LogicalSchema &schema() noexcept { return _schema; }

    [[nodiscard]] std::uint64_t cardinality() const noexcept { return _cardinality; }
    void cardinality(const std::uint64_t cardinality) noexcept { _cardinality = cardinality; }

private:
    topology::LogicalSchema _schema;
    std::uint64_t _cardinality{0U};
};
} // namespace db::plan::logical