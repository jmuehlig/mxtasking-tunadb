#pragma once

#include "operator_interface.h"
#include <cstdint>
#include <db/expression/operation.h>
#include <db/topology/physical_schema.h>
#include <memory>
#include <optional>
#include <tuple>
#include <vector>

namespace db::execution::compilation {
class AbstractAggregationOperator : public UnaryOperator
{
public:
    AbstractAggregationOperator(topology::PhysicalSchema &&schema, topology::PhysicalSchema &&aggregation_schema,
                                const topology::PhysicalSchema &incoming_schema,
                                std::vector<std::unique_ptr<db::expression::Operation>> &&aggregations);

    ~AbstractAggregationOperator() override = default;

    /**
     * Builds the schema that is used for aggregations.
     * Will be used as value-schema for hash table in grouped aggregations
     * or schema for the local-result tile in normal aggregations.
     *
     * @param operator_schema Schema of the operator.
     * @param aggregations List of operations to aggregate.
     * @return The aggregation schema.
     */
    [[nodiscard]] static topology::PhysicalSchema make_aggregation_schema(
        const topology::PhysicalSchema &operator_schema,
        const std::vector<std::unique_ptr<expression::Operation>> &aggregations);

    /**
     * Builds the schema that is used for aggregation groups.
     * Will be used as a key-schema for hash table in grouped aggregations.
     *
     * @param incoming_schema Incomeing schema.
     * @param group_terms List of group terms.
     * @return The group schema.
     */
    [[nodiscard]] static topology::PhysicalSchema make_group_schema(const topology::PhysicalSchema &incoming_schema,
                                                                    std::vector<expression::Term> &&group_terms);

protected:
    /// Schema for records produced by this operator.
    topology::PhysicalSchema _schema;

    /// The schema containing all aggregation fields.
    /// May contain a additional COUNT aggregation used
    /// for average calculations.
    topology::PhysicalSchema _aggregation_schema;

    /// Aggregations.
    std::vector<std::unique_ptr<expression::Operation>> _aggregations;

    /// Index in the schema for a COUNT operation, used for AVG operations.
    std::optional<std::uint16_t> _count_index{std::nullopt};

    /// Schema of the child operator, used to access consumed records.
    [[maybe_unused]] const topology::PhysicalSchema &_incoming_schema;

    /**
     * If the aggregations contain at least one AVG aggregation,
     * the aggregation schema will contain also a COUNT-aggregation-term.
     * During aggregation phase, we will handle AVG aggregations as SUMs
     * and divide the SUM by COUNT.
     * Therefore, we add a COUNT aggregation to the aggregations if not included.
     * However, the index of the COUNT aggregation will be returned, if any.
     * In addition, we change the term type of all aggregations to the SUM type.
     *
     * @param aggregation_schema Schema aggregations are stored. Will be change from AVG type to SUM type.
     * @param incoming_schema Incoming schema to get the SUM type.
     * @param aggregations Aggregations, may be extended by an AVG aggregation.
     * @return Index of the COUNT aggregation for AVG aggregations, if any.
     */
    [[nodiscard]] std::optional<std::uint16_t> handle_average_aggregation(
        topology::PhysicalSchema &aggregation_schema, const topology::PhysicalSchema &incoming_schema,
        std::vector<std::unique_ptr<expression::Operation>> &aggregations);

    /**
     * Creates virtual registers that are needed to aggregate.
     * The registers have to be allocated and cleard by the caller.
     *
     * @param program Program to claim/clear register.
     * @param schema Schema of the aggregation result.
     * @param aggregations Aggregations.
     * @param create_default_value If set to false, the returned values will be null.
     * @param prefix Prefix for the register names.
     * @return List of claimed registers, their db types, and default values (may be null).
     */
    [[nodiscard]] static std::vector<std::tuple<flounder::Register, type::Type, std::optional<flounder::Operand>>>
    make_aggregation_registers(flounder::Program &program, const topology::PhysicalSchema &schema,
                               const std::vector<std::unique_ptr<expression::Operation>> &aggregations,
                               std::optional<flounder::Register> local_results_vreg, bool create_default_value = true,
                               std::optional<std::string> &&prefix = std::nullopt);
};
} // namespace db::execution::compilation