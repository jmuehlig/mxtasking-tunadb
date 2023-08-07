#include "partition_operator.h"
#include <db/execution/compilation/hash.h>
#include <db/execution/compilation/hash_emitter.h>
#include <db/execution/compilation/materializer.h>
#include <db/execution/compilation/prefetcher.h>
#include <db/execution/compilation/scan_loop.h>
#include <flounder/lib.h>
#include <flounder/statement.h>
#include <mx/tasking/runtime.h>

using namespace db::execution::compilation;

void PartitionOperator::produce(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    /// During execution, we map each record to a specific core
    /// and materialize the record to a core-to-core tile.
    if (phase == GenerationPhase::execution)
    {
        /// Pass 0 will re-use the scan loop of the scan operator.
        if (this->is_first_pass())
        {
            /// This will emit all child's produce() and consume until this operator.
            auto partition_context_guard = flounder::ContextGuard{program, "Partition"};
            this->child()->produce(phase, program, context);
        }

        /// Further passes are pipeline-breaker and therefore will produce their own scan loop.
        else
        {
            auto scan_context_guard = flounder::ContextGuard{program, "Scan"};
            {
                auto scan_loop = PaxScanLoop{program, context, fmt::format("partition_{:d}", this->_pass),
                                             this->child()->schema(), true};
                {
                    auto partition_context_guard = flounder::ContextGuard{program, "Partition"};
                    this->consume(phase, program, context);
                }
            }
        }
    }

    else if (phase == GenerationPhase::prefetching)
    {
        if (this->is_first_pass())
        {
            this->child()->produce(phase, program, context);
        }
        else
        {
            this->_count_prefetches = PrefetchCallbackGenerator::produce(program, this->child()->schema());
        }
    }
}

void PartitionOperator::consume(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    if (phase == GenerationPhase::finalization || phase == GenerationPhase::prefetching)
    {
        this->parent()->consume(phase, program, context);
        return;
    }

    auto context_guard = flounder::ContextGuard{program, "Partition"};

    /// The first pass needs to calculate the partition hash.
    auto partition_hash_vreg = std::optional<flounder::Register>{std::nullopt};
    if (this->is_first_pass())
    {
        if (config::is_use_hash_for_partitioning() == false && this->_partition_terms.size() == 1U)
        {
            partition_hash_vreg = context.symbols().get(this->_partition_terms.front());
        }
        else
        {
            /// Hash the join term for partition calculation.
            auto partition_term_vregs = std::vector<flounder::Register>{};
            auto partition_term_types = std::vector<type::Type>{};
            for (const auto &term : this->_partition_terms)
            {
                partition_term_vregs.emplace_back(context.symbols().get(term));
                partition_term_types.emplace_back(this->_schema.type(this->_schema.index(term).value()));
            }

            partition_hash_vreg =
                HashEmitter<MurmurHash>::hash(MurmurHash{0U}, program, partition_term_vregs, partition_term_types);
        }

        /// Every but the last pass needs to materialize the hash.
        if (context.symbols().is_requested(PartitionOperator::partition_hash_term))
        {
            context.symbols().set(PartitionOperator::partition_hash_term, partition_hash_vreg.value());
        }
    }
    else
    {
        /// The hash was already calculated and materialized by the first pass,
        /// just load it.
        partition_hash_vreg = context.symbols().get(PartitionOperator::partition_hash_term);
    }

    /// Calculate the partition id.
    auto calculator = PartitionCalculator{this->_radix_bits};
    auto partition_id_vreg = program.vreg(PartitionOperator::partition_id_term.to_string());
    program << program.request_vreg64(partition_id_vreg);

    /// Simplified way if we are calculating pass 0.
    if (this->_pass == 0U)
    {
        /// Bits for this pass, i.e., 4bits -> 15 = 1111
        const auto bit_mask = calculator.mask(0U);

        program << program.mov(partition_id_vreg, partition_hash_vreg.value())
                << program.and_(partition_id_vreg, program.constant16(bit_mask));
    }

    /// More generic way, if we are calculating pass > 0
    else
    {
        auto tmp_partition_hash_vreg = program.vreg("tmp_partition_hash");
        program << program.request_vreg64(tmp_partition_hash_vreg)
                << program.xor_(partition_id_vreg, partition_id_vreg);

        auto shift_radix_bits = 0U;
        for (auto pass = 0U; pass <= this->_pass; ++pass)
        {
            /// Bits for this pass, i.e., 4bits -> 15 = 1111
            const auto bit_mask = calculator.mask(pass);

            program << program.mov(tmp_partition_hash_vreg, partition_hash_vreg.value());

            /// Shift away the bits already used for partitioning.
            if (pass > 0U)
            {
                program << program.shr(tmp_partition_hash_vreg, program.constant8(shift_radix_bits));
            }

            /// Let the bits for this pass only remain.
            program << program.and_(tmp_partition_hash_vreg, program.constant16(bit_mask));

            /// Calculate the offset for this partition.
            if (pass < this->_pass)
            {
                program << program.imul(tmp_partition_hash_vreg,
                                        program.constant32(calculator.multiplier(pass, this->_pass)));
            }

            /// Add this pass to the partition id.
            program << program.add(partition_id_vreg, tmp_partition_hash_vreg);

            /// For the next pass, the current used bits needs to be shifted out.
            shift_radix_bits += this->_radix_bits[pass];
        }

        program << program.clear(tmp_partition_hash_vreg);
    }

    /// Clear all symbols.
    context.symbols().release(program, this->_partition_terms);

    /// The parent may filter out records before materializing
    /// or (after or without filtering) materializes the records.
    context.symbols().set(PartitionOperator::partition_id_term, partition_id_vreg);
    this->parent()->consume(phase, program, context);

    /// Every but the last partition pass need the hash to materialize it.
    if (this->is_last_pass())
    {
        program << program.clear(partition_hash_vreg.value());
    }
}

void PartitionOperator::request_symbols(const GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        /// Request the join term to map tuples based on the join term to channels.
        symbols.request(this->_partition_terms);

        if (this->is_first_pass() == false && this->is_last_pass())
        {
            /// For the last pass, the hash term is not part of the schema.
            /// Hence, we have to request it explicitly.
            symbols.request(PartitionOperator::partition_hash_term);
        }
    }

    /// Only the first pass will be compiled to a program together
    /// with its children (like a scan). Other passes will be compiled
    /// as a standalone program.
    if (this->is_first_pass())
    {
        this->child()->request_symbols(phase, symbols);
    }
}

void MaterializePartitionOperator::produce(GenerationPhase phase, flounder::Program &program,
                                           CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        this->_partition_emitter_array_vreg = program.vreg("partition_emitter_array");
        program.arguments() << program.request_vreg64(this->_partition_emitter_array_vreg.value())
                            << program.get_argument(2U, this->_partition_emitter_array_vreg.value());

        this->child()->produce(phase, program, context);

        /// Emit all tiles that are not empty after all records are consumed.
        program << program.clear(this->_partition_emitter_array_vreg.value());
    }

    /// During finalization, all not flushed tiles will be emitted to the graph.
    else if (phase == GenerationPhase::finalization)
    {
        /// Emit all record sets that were not full during normal consuming.
        auto partition_emitter_vreg = program.vreg("partition_emitter");
        program.arguments() << program.request_vreg64(partition_emitter_vreg)
                            << program.get_arg2(partition_emitter_vreg);

        if (this->_is_last_pass)
        {
            flounder::FunctionCall{program, std::uintptr_t(&PartitionFinalizer<true>::emit)}.call(
                {flounder::Operand{partition_emitter_vreg}});
        }
        else
        {
            flounder::FunctionCall{program, std::uintptr_t(&PartitionFinalizer<false>::emit)}.call(
                {flounder::Operand{partition_emitter_vreg}});
        }

        program << program.clear(partition_emitter_vreg);
    }

    else if (phase == GenerationPhase::prefetching)
    {
        this->child()->produce(phase, program, context);
    }
}

void MaterializePartitionOperator::consume(GenerationPhase phase, flounder::Program &program,
                                           CompilationContext &context)
{
    if (phase == GenerationPhase::finalization || phase == GenerationPhase::prefetching)
    {
        return;
    }

    const auto count_partitions =
        this->_is_last_pass ? this->_partitions.size() : (this->_partitions.size() / mx::tasking::runtime::workers());

    auto context_guard = flounder::ContextGuard{program, "Materialize Partition"};

    auto partition_id_vreg = context.symbols().get(PartitionOperator::partition_id_term);

    /// Calculate the offset with offset = target_worker_id * sizeof(PartitionEmitter) + PartitionEmitter::tile_offset()
    auto target_tile_offset_vreg = program.vreg("target_tile_offset");
    program << program.request_vreg64(target_tile_offset_vreg)
            << program.mov(target_tile_offset_vreg, partition_id_vreg)
            << program.imul(target_tile_offset_vreg, program.constant32(sizeof(PartitionEmitter)))
            << program.add(target_tile_offset_vreg, program.constant32(PartitionEmitter::tile_offset()));

    /// Get the tile_ptr which is at [partition_emitter_array + target_offset] and mask it.
    auto target_tile_vreg = program.vreg("target_tile");
    program << program.request_vreg64(target_tile_vreg)
            << program.mov(
                   target_tile_vreg,
                   program.mem(this->_partition_emitter_array_vreg.value(), target_tile_offset_vreg,
                               MaterializePartitionedOutputProvider::WorkerLocalPartition::partition_emiter_offset(
                                   count_partitions)))
            << program.clear(target_tile_offset_vreg)
            << program.and_(target_tile_vreg, program.constant64(0xFFFFFFFFFFFF));

    /// Get the size of the tile.
    constexpr auto target_tile_size_register_width =
        flounder::register_width_t<MaterializePartitionedOutputProvider::WorkerLocalPartition::size_type>::value();
    auto target_tile_size_local_addr =
        program.mem(this->_partition_emitter_array_vreg.value(), partition_id_vreg,
                    sizeof(MaterializePartitionedOutputProvider::WorkerLocalPartition::size_type), 0U,
                    target_tile_size_register_width);

    /// Load the tile size to materialize
    auto target_tile_size_vreg = program.vreg("target_tile_size");
    program << program.request_vreg32u(target_tile_size_vreg)
            << program.mov(target_tile_size_vreg, target_tile_size_local_addr);

    /// Materialize all records to the tile.
    PaxMaterializer::materialize(program, context.symbols(), this->_schema, target_tile_vreg, target_tile_size_vreg);

    /// Release all symbols needed for materialization.
    context.symbols().release(program, this->_schema.terms());

    /// Increment size.
    program << program.add(target_tile_size_local_addr, program.constant8(1));

    /// Check, if the tile is full and we need to emit to the graph.
    {
        auto if_tile_is_full = flounder::If{
            program,
            flounder::IsGreaterEquals{flounder::Operand{target_tile_size_vreg},
                                      flounder::Operand{program.constant16(config::tuples_per_tile() - 1U)}, false},
            "if_target_tile_is_full"};

        program
            /// Write back the size to the tile.
            << program.clear(target_tile_size_vreg)
            << program.mov(program.mem(target_tile_vreg, data::PaxTile::size_offset(), flounder::RegisterWidth::r64),
                           program.constant32(config::tuples_per_tile()))

            /// Clear the size
            << program.mov(target_tile_size_local_addr, program.constant16(0));

        auto partition_emitter_vreg = program.vreg("partition_emitter");
        program << program.request_vreg64(partition_emitter_vreg)
                << program.mov(partition_emitter_vreg, partition_id_vreg)
                << program.imul(partition_emitter_vreg, program.constant32(sizeof(PartitionEmitter)))
                << program.lea(
                       partition_emitter_vreg,
                       program.mem(partition_emitter_vreg, this->_partition_emitter_array_vreg.value(),
                                   MaterializePartitionedOutputProvider::WorkerLocalPartition::partition_emiter_offset(
                                       count_partitions)));

        flounder::FunctionCall(program, std::uintptr_t(&PartitionEmitter::emit))
            .call({flounder::Operand{partition_emitter_vreg}});
        program << program.clear(partition_emitter_vreg);
    }

    context.symbols().release(program, PartitionOperator::partition_id_term);

    program << program.clear(target_tile_vreg);
}

void MaterializePartitionOperator::request_symbols(GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        /// Request all values for materialization.
        symbols.request(this->_schema.terms());

        /// Request the partition id calculated by the partition operator.
        symbols.request(PartitionOperator::partition_id_term);
    }

    this->child()->request_symbols(phase, symbols);
}

std::unique_ptr<OutputProviderInterface> MaterializePartitionOperator::output_provider(GenerationPhase phase)
{
    if (phase == GenerationPhase::execution)
    {
        return std::move(this->_output_provider);
    }

    return nullptr;
}

std::optional<std::pair<mx::tasking::dataflow::annotation<db::execution::RecordSet>::FinalizationType,
                        std::vector<mx::resource::ptr>>>
MaterializePartitionOperator::finalization_data() noexcept
{
    const auto count_workers = mx::tasking::runtime::workers();
    auto finalizer = std::vector<mx::resource::ptr>{};
    finalizer.reserve(count_workers);

    if (this->_is_last_pass)
    {
        auto *pending_counter =
            new (std::aligned_alloc(64U, sizeof(std::atomic_uint16_t))) std::atomic_uint16_t{count_workers};

        const auto resource_boundness = this->parent()->parent() != nullptr
                                            ? this->parent()->parent()->resource_boundness()
                                            : mx::tasking::annotation::resource_boundness::mixed;
        for (auto worker_id = std::uint16_t(0U); worker_id < count_workers; ++worker_id)
        {
            auto *partition_finalizer = new (std::aligned_alloc(64U, sizeof(PartitionFinalizer<true>)))
                PartitionFinalizer<true>(worker_id, this->_output_provider->partition_emitter(),
                                         this->_partitions.size(), pending_counter,
                                         std::make_optional(std::ref(this->_output_provider->partitions())),
                                         resource_boundness, this->_is_emit_last_pass);
            finalizer.emplace_back(partition_finalizer,
                                   mx::resource::information{worker_id, mx::synchronization::primitive::ScheduleAll});
        }
    }
    else
    {
        for (auto worker_id = std::uint16_t(0U); worker_id < count_workers; ++worker_id)
        {
            auto *partition_finalizer = new (std::aligned_alloc(64U, sizeof(PartitionFinalizer<false>)))
                PartitionFinalizer<false>(worker_id, this->_output_provider->partition_emitter(),
                                          this->_partitions.size() / count_workers);
            finalizer.emplace_back(partition_finalizer,
                                   mx::resource::information{worker_id, mx::synchronization::primitive::ScheduleAll});
        }
    }

    return std::make_pair(mx::tasking::dataflow::annotation<RecordSet>::FinalizationType::parallel,
                          std::move(finalizer));
}