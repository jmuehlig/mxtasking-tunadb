#include "aggregation_operator.h"
#include <flounder/debug.h>
#include <flounder/statement.h>
#include <fmt/core.h>
#include <mx/tasking/runtime.h>

using namespace db::execution::compilation;

AggregationOperator::AggregationOperator(topology::PhysicalSchema &&schema,
                                         topology::PhysicalSchema &&aggregation_schema,
                                         const topology::PhysicalSchema &incoming_schema,
                                         std::vector<std::unique_ptr<db::expression::Operation>> &&aggregations)
    : AbstractAggregationOperator(std::move(schema), std::move(aggregation_schema), incoming_schema,
                                  std::move(aggregations))
{
    /// Align the local aggregation result schema to 64byte to avoid false sharing.
    this->_aggregation_schema.align_to(64U);

    const auto count_cores = mx::tasking::runtime::workers();
    this->_local_results =
        std::make_shared<LocalAggregationResult>(topology::PhysicalSchema{this->_aggregation_schema}, count_cores);
    for (auto worker_id = 0U; worker_id < count_cores; ++worker_id)
    {
        auto record = this->_local_results->at(worker_id);
        for (const auto &aggregation : this->_aggregations)
        {
            const auto index = this->_aggregation_schema.index(aggregation->result().value());
            if (index.has_value())
            {
                const auto type = this->_aggregation_schema.type(index.value());
                if (aggregation->id() == expression::Operation::Id::Count ||
                    aggregation->id() == expression::Operation::Id::Sum ||
                    aggregation->id() == expression::Operation::Id::Average)
                {
                    record.set(index.value(), data::Value{type, data::Value::make_zero(type.id())});
                }
                else if (aggregation->id() == expression::Operation::Id::Min)
                {
                    record.set(index.value(), data::Value{type, type.max_value()});
                }
                else if (aggregation->id() == expression::Operation::Id::Max)
                {
                    record.set(index.value(), data::Value{type, type.min_value()});
                }
            }
        }
    }
}

void AggregationOperator::produce(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    /// During execution, the aggregation operator aggregates locally
    /// within two passes: For each tile, the data is aggregated (1) and then
    /// written to a core-local result (2).
    if (phase == GenerationPhase::execution)
    {
        /// Store the pointer to the core local result.
        this->_local_aggregation_result_vreg = program.vreg("local_aggregation_result");
        program.arguments() << program.request_vreg64(this->_local_aggregation_result_vreg.value())
                            << program.get_arg2(this->_local_aggregation_result_vreg.value());

        /// Request registers for local aggregation of the given tile..
        auto registers = AbstractAggregationOperator::make_aggregation_registers(
            program, this->_aggregation_schema, this->_aggregations, this->_local_aggregation_result_vreg);
        for (auto [vreg, type, value] : registers)
        {
            this->_consume_aggregation_result_registers.emplace_back(vreg);
            program.header() << program.request_vreg(vreg, type.register_width());
            if (value.has_value())
            {
                if (value->is_constant() && value->constant().value_as_int64() == 0)
                {
                    program.header() << program.xor_(vreg, vreg);
                }
                else
                {
                    program.header() << program.mov(vreg, value.value());
                }
            }
        }

        /// Let the children produce code (scan loop, filters, arithmetic, ...).
        /// This will call (on way up) the consume() code where aggregation takes place.
        this->child()->produce(phase, program, context);

        /// Materialize aggregation register values into local aggregation result.
        this->merge_results_into_core_local(program, context);

        for (auto aggregation_register : registers)
        {
            program << program.clear(std::get<0>(aggregation_register));
        }
        program << program.clear(this->_local_aggregation_result_vreg.value());
    }

    /// During finalization phase
    else if (phase == GenerationPhase::finalization)
    {
        auto context_guard = flounder::ContextGuard{program, "Aggregation"};

        /// Request registers for local aggregation.
        auto registers = AbstractAggregationOperator::make_aggregation_registers(program, this->_aggregation_schema,
                                                                                 this->_aggregations, std::nullopt);
        for (auto [vreg, type, value] : registers)
        {
            this->_finalize_aggregation_result_registers.emplace_back(vreg);
            program.header() << program.request_vreg(vreg, type.register_width());
            if (value.has_value())
            {
                if (value->is_constant() && value->constant().value_as_int64() == 0U)
                {
                    program << program.xor_(vreg, vreg);
                }
                else
                {
                    program << program.mov(vreg, value.value());
                }
            }
        }

        /// Register for the local results (process one after the other).
        auto local_result_vreg = program.vreg("local_result_record");

        /// Register for the end of the local result iterator.
        auto local_result_end_vreg = program.vreg("local_result_end");

        /// Set begin pointing to the begin of the local results tile.
        program << program.request_vreg64(local_result_vreg)
                << program.mov(local_result_vreg, program.address(this->_local_results->tile()->begin()));

        /// End is equals to |schema| * |local result rows| + address of begin of the result tile.
        const auto local_result_end = this->_local_results->tile()->schema().row_size() * this->_local_results->size() +
                                      std::uintptr_t(this->_local_results->tile()->begin());
        program << program.request_vreg64(local_result_end_vreg)
                << program.mov(local_result_end_vreg, program.constant64(local_result_end));

        {
            auto merge_loop = flounder::ForEach(program, local_result_vreg, local_result_end_vreg,
                                                this->_local_results->tile()->schema().row_size(), "merge_loop");

            /// Merge all aggregations from the local result into the global result registers.
            for (auto aggregation_id = 0U; aggregation_id < this->_aggregations.size(); ++aggregation_id)
            {
                const auto &aggregation = this->_aggregations[aggregation_id];
                auto aggregation_register = this->_finalize_aggregation_result_registers[aggregation_id];

                const auto index = this->_aggregation_schema.index(aggregation->result().value());
                if (index.has_value())
                {
                    /// The local result for this aggregation is stored here.
                    auto local_result_aggregation_address =
                        program.mem(local_result_vreg, this->_aggregation_schema.row_offset(index.value()),
                                    this->_aggregation_schema.type(index.value()).register_width());

                    if (aggregation->id() == expression::Operation::Id::Count ||
                        aggregation->id() == expression::Operation::Id::Sum ||
                        aggregation->id() == expression::Operation::Id::Average)
                    {
                        /// Merge.
                        program << program.add(aggregation_register, local_result_aggregation_address);
                    }
                    else if (aggregation->id() == expression::Operation::Id::Min)
                    {
                        program << program.cmp(local_result_aggregation_address, aggregation_register)
                                << program.cmovle(aggregation_register, local_result_aggregation_address);
                    }
                    else if (aggregation->id() == expression::Operation::Id::Max)
                    {
                        program << program.cmp(local_result_aggregation_address, aggregation_register)
                                << program.cmovge(aggregation_register, local_result_aggregation_address);
                    }
                }
            }
        }
        program << program.clear(local_result_vreg) << program.clear(local_result_end_vreg);

        /// Write the values of aggregation merge to the result record.
        for (auto aggregation_id = 0U; aggregation_id < this->_aggregations.size(); ++aggregation_id)
        {
            const auto &aggregation = this->_aggregations[aggregation_id];
            auto aggregation_register = this->_finalize_aggregation_result_registers[aggregation_id];
            const auto index = this->_schema.index(aggregation->result().value());
            if (index.has_value())
            {
                /// Calculate the average; other aggregations are already done.
                if (aggregation->id() == expression::Operation::Id::Average && this->_count_index.has_value())
                {
                    const auto type = this->_schema.type(index.value());
                    if (type == type::Id::DECIMAL)
                    {
                        program << program.imul(aggregation_register,
                                                program.constant64(std::pow(10U, type.decimal_description().scale())));
                    }

                    /// For AVERAGE divide aggregation_register (which is SUM) by COUNT and store into
                    /// aggregation_target_address.
                    program << program.fdiv(aggregation_register,
                                            this->_finalize_aggregation_result_registers[this->_count_index.value()]);
                }

                context.symbols().set(aggregation->result().value(), aggregation_register);
            }
        }

        this->child()->produce(phase, program, context);

        for (auto aggregation_vreg : this->_finalize_aggregation_result_registers)
        {
            program << program.clear(aggregation_vreg);
        }
    }
    else if (phase == GenerationPhase::prefetching)
    {
        this->child()->produce(phase, program, context);
    }
}

void AggregationOperator::consume(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    if (phase == GenerationPhase::finalization || phase == GenerationPhase::prefetching)
    {
        this->parent()->consume(phase, program, context);
        return;
    }

    auto context_guard = flounder::ContextGuard{program, "Aggregation"};

    for (auto i = 0U; i < this->_aggregations.size(); ++i)
    {
        auto &operation = this->_aggregations[i];
        if (operation->id() == expression::Operation::Id::Count)
        {
            /// Count does not need to access the record.
            program << program.add(this->_consume_aggregation_result_registers[i], program.constant8(1));
        }
        else
        {
            /// Every other aggregation need to access the record.
            /// The record is arranged like the incoming schema.
            auto *aggregation = reinterpret_cast<expression::UnaryOperation *>(operation.get());

            /// Emit code for the operation (could be simple attribute access or more complex arithmetic).
            const auto aggregated_term = aggregation->child()->result().value();
            auto operation_register = context.symbols().get(aggregated_term);

            /// Perform aggregation, averages are performed like sums and calculated at the merge pass.
            if (operation->id() == expression::Operation::Id::Sum ||
                operation->id() == expression::Operation::Id::Average)
            {
                program << program.add(this->_consume_aggregation_result_registers[i], operation_register);
            }
            else if (operation->id() == expression::Operation::Id::Min)
            {
                //                flounder::Debug::print_int32(program, operation_register);
                //                flounder::Debug::print_int32(program, this->_consume_aggregation_result_registers[i]);
                program << program.cmp(operation_register, this->_consume_aggregation_result_registers[i])
                        << program.cmovle(this->_consume_aggregation_result_registers[i], operation_register);

                //                flounder::Debug::print_int32(program, this->_consume_aggregation_result_registers[i]);
                //                flounder::Debug::say_hello(program);
            }
            else if (operation->id() == expression::Operation::Id::Max)
            {
                program << program.cmp(operation_register, this->_consume_aggregation_result_registers[i])
                        << program.cmovge(this->_consume_aggregation_result_registers[i], operation_register);
            }
        }

        /// Release the requested symbols.
        expression::for_each_term(operation, [&program, &context](const expression::Term &term) {
            if (term.is_attribute())
            {
                context.symbols().release(program, term);
            }
        });
    }
}

void AggregationOperator::request_symbols(GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        symbols.request(this->_aggregations);
    }

    this->child()->request_symbols(phase, symbols);
}

void AggregationOperator::merge_results_into_core_local(flounder::Program &program, CompilationContext &context)
{
    auto context_guard = flounder::ContextGuard{program, "Aggregation"};

    /// Write back the results of this batch to the local results.
    for (auto i = 0U; i < this->_aggregations.size(); ++i)
    {
        auto &operation = this->_aggregations[i];
        const auto index = this->_aggregation_schema.index(operation->result().value());
        if (index.has_value())
        {
            /// Local results are stored here.
            auto local_result_address = program.mem(this->_local_aggregation_result_vreg.value(),
                                                    this->_aggregation_schema.row_offset(index.value()),
                                                    this->_aggregation_schema.type(index.value()).register_width());

            if (operation->id() == expression::Operation::Id::Count ||
                operation->id() == expression::Operation::Id::Sum ||
                operation->id() == expression::Operation::Id::Average)
            {
                /// Aggregate batch and local result.
                program << program.add(local_result_address, this->_consume_aggregation_result_registers[i]);
            }
            else if (operation->id() == expression::Operation::Id::Min ||
                     operation->id() == expression::Operation::Id::Max)
            {
                program << program.mov(local_result_address, this->_consume_aggregation_result_registers[i]);
            }
        }
    }

    this->parent()->consume(GenerationPhase::execution, program, context);
}

std::unique_ptr<OutputProviderInterface> AggregationOperator::output_provider(const GenerationPhase phase)
{
    if (phase == GenerationPhase::execution)
    {
        return std::make_unique<AggregationOutputProvider>(this->_local_results);
    }

    return this->child()->output_provider(phase);
}
