#include "abstract_aggregation_operator.h"
#include <db/execution/compilation/materializer.h>

using namespace db::execution::compilation;

AbstractAggregationOperator::AbstractAggregationOperator(
    topology::PhysicalSchema &&schema, topology::PhysicalSchema &&aggregation_schema,
    const topology::PhysicalSchema &incoming_schema,
    std::vector<std::unique_ptr<db::expression::Operation>> &&aggregations)
    : _schema(std::move(schema)), _aggregation_schema(std::move(aggregation_schema)),
      _aggregations(std::move(aggregations)), _incoming_schema(incoming_schema)
{
    this->_count_index = AbstractAggregationOperator::handle_average_aggregation(
        this->_aggregation_schema, this->_incoming_schema, this->_aggregations);
}

db::topology::PhysicalSchema AbstractAggregationOperator::make_aggregation_schema(
    const topology::PhysicalSchema &operator_schema,
    const std::vector<std::unique_ptr<expression::Operation>> &aggregations)
{
    auto local_result_schema = topology::PhysicalSchema{};
    local_result_schema.reserve(operator_schema.size());

    auto has_average_operation = false;
    auto has_count_operation = false;

    for (const auto &aggregation : aggregations)
    {
        const auto index = operator_schema.index(aggregation->result().value());
        if (index.has_value())
        {
            has_average_operation |= aggregation->id() == expression::Operation::Average;
            has_count_operation |= aggregation->id() == expression::Operation::Count;

            local_result_schema.emplace_back(expression::Term{operator_schema.term(index.value())},
                                             operator_schema.type(index.value()),
                                             operator_schema.is_null(index.value()));
        }
    }

    if (has_average_operation && has_count_operation == false)
    {
        local_result_schema.emplace_back(expression::Term::make_attribute("*", true), type::Type::make_bigint(), false);
    }

    return local_result_schema;
}

db::topology::PhysicalSchema AbstractAggregationOperator::make_group_schema(
    const topology::PhysicalSchema &incoming_schema, std::vector<expression::Term> &&group_terms)
{
    auto group_schema = topology::PhysicalSchema{};
    group_schema.reserve(group_terms.size());

    /// Create schema for groups that are stored as keys within the hash tables.
    for (auto &group_term : group_terms)
    {
        const auto index = incoming_schema.index(group_term);
        assert(index.has_value() && "Group term not found in incoming schema.");
        group_schema.emplace_back(std::move(group_term), incoming_schema.type(index.value()));
    }

    return group_schema;
}

std::optional<std::uint16_t> AbstractAggregationOperator::handle_average_aggregation(
    topology::PhysicalSchema &aggregation_schema, const topology::PhysicalSchema &incoming_schema,
    std::vector<std::unique_ptr<expression::Operation>> &aggregations)
{
    auto any_avg_aggregation = std::find_if(aggregations.begin(), aggregations.end(), [](const auto &aggregation) {
        return aggregation->id() == expression::Operation::Id::Average;
    });
    if (any_avg_aggregation != aggregations.end())
    {
        for (auto &aggregation : aggregations)
        {
            if (aggregation->id() == expression::Operation::Id::Average)
            {
                auto sum_operation = aggregation->copy();
                sum_operation->id(expression::Operation::Id::Sum);
                const auto index = aggregation_schema.index(aggregation->result().value());
                if (index.has_value())
                {
                    aggregation_schema.type(index.value(), sum_operation->type(incoming_schema));
                }
            }
        }

        auto any_count = std::find_if(aggregations.begin(), aggregations.end(), [](const auto &aggregation) {
            return aggregation->id() == expression::Operation::Id::Count;
        });

        /// We found a COUNT aggreegation, use that.
        if (any_count != aggregations.end())
        {
            return std::distance(aggregations.begin(), any_count);
        }

        /// We add an extra COUNT operation that maps to the extra COUNT term.
        aggregations.emplace_back(std::make_unique<expression::UnaryOperation>(
            expression::Operation::Id::Count, std::make_unique<expression::NullaryOperation>(
                                                  expression::Term{this->_aggregation_schema.terms().back()})));
        return aggregations.size() - 1U;
    }

    return std::nullopt;
}

std::vector<std::tuple<flounder::Register, db::type::Type, std::optional<flounder::Operand>>>
AbstractAggregationOperator::make_aggregation_registers(
    flounder::Program &program, const topology::PhysicalSchema &schema,
    const std::vector<std::unique_ptr<expression::Operation>> &aggregations,
    std::optional<flounder::Register> local_results_vreg, const bool create_default_value,
    std::optional<std::string> &&prefix)
{
    auto registers = std::vector<std::tuple<flounder::Register, db::type::Type, std::optional<flounder::Operand>>>{};
    registers.reserve(aggregations.size());

    auto register_prefix = prefix.has_value() ? fmt::format("{}_", std::move(prefix.value())) : std::string{};

    for (const auto &operation : aggregations)
    {
        const auto index = schema.index(operation->result().value());
        if (index.has_value())
        {
            /// Create a register for this aggregation.
            auto vreg = program.vreg(
                fmt::format("{}{}", register_prefix, SymbolSet::make_vreg_name(operation->result().value())));

            /// Request the register with correct type.
            const auto &type = schema.type(index.value());

            /// Initialize the register with correct value.
            auto value = std::optional<flounder::Operand>{};
            if (create_default_value)
            {
                if (operation->id() == expression::Operation::Id::Count ||
                    operation->id() == expression::Operation::Id::Sum ||
                    operation->id() == expression::Operation::Id::Average)
                {
                    value = flounder::Operand{program.constant8(0)};
                }
                else if (operation->id() == expression::Operation::Id::Min ||
                         operation->id() == expression::Operation::Id::Max)
                {
                    if (local_results_vreg.has_value())
                    {
                        value = RowMaterializer::access(program, local_results_vreg.value(), 0U, schema, index.value());
                    }
                    else if (operation->id() == expression::Operation::Id::Max)
                    {
                        value = flounder::Operand{program.constant64(type.min_value())};
                    }
                    else
                    {
                        value = flounder::Operand{program.constant64(type.max_value())};
                    }
                }
            }

            registers.emplace_back(vreg, type, value);
        }
    }

    return registers;
}