#include "partition_filter_operator.h"
#include "partition_operator.h"
#include <bit>
#include <db/execution/compilation/hash.h>
#include <flounder/statement.h>

using namespace db::execution::compilation;

flounder::Register PartitionFilter::emit_bloom_filter_address(flounder::Program &program,
                                                              BloomFilterDescriptor bloom_filter_descriptor,
                                                              flounder::Register partition_id_vreg, type::Type key_type,
                                                              flounder::Register key_vreg)
{
    constexpr auto bytes_per_block = 8U; /// 64bit per block.
    auto bytes_per_partition = bloom_filter_descriptor.blocks_per_partition() * bytes_per_block;

    auto block_hash_vreg = MurmurHash{0xBD3BCCDDCD9C6DF9}.emit(program, key_type, key_vreg);

    /// block_id_in_partition_offset = hash % blocks_fper_partition * bytes_per_block
    program << program.and_(block_hash_vreg, program.constant32(bloom_filter_descriptor.blocks_per_partition() - 1U))
            << program.shl(block_hash_vreg, program.constant8(std::popcount(bytes_per_block - 1U)));

    /// block_id = filter_address + (partition_id * bytes_per_partition) + block_id_in_partition_offset
    auto bloom_filter_address_vreg = program.vreg("blocked_bloom_filter_address");
    program << program.request_vreg64(bloom_filter_address_vreg)
            << program.mov(bloom_filter_address_vreg, partition_id_vreg);
    if ((bytes_per_partition & (bytes_per_partition - 1)) == 0)
    {
        program << program.shl(bloom_filter_address_vreg, program.constant8(std::popcount(bytes_per_partition - 1U)));
    }
    else
    {
        program << program.imul(bloom_filter_address_vreg, program.constant64(bytes_per_partition));
    }
    program << program.add(bloom_filter_address_vreg, block_hash_vreg) << program.clear(block_hash_vreg)
            << program.add(bloom_filter_address_vreg, program.address(bloom_filter_descriptor.filter()));

    return bloom_filter_address_vreg;
}

flounder::Register PartitionFilter::emit_search_mask(flounder::Program &program, type::Type key_type,
                                                     flounder::Register key_vreg)
{
    /// Reg for search masks.
    auto search_mask_vreg = program.vreg("bf_search_mask");
    auto search_mask_bit_vreg = program.vreg("bf_par_search_mask");
    program << program.request_vreg64(search_mask_vreg) << program.xor_(search_mask_vreg, search_mask_vreg)
            << program.request_vreg64(search_mask_bit_vreg);

    auto hash_vreg = CRC32Hash{}.emit(program, key_type, key_vreg);
    auto hash_pass_vreg = program.vreg("hash_pass");
    program << program.request_vreg8(hash_pass_vreg)

            /// Consume 1/4 8 bits of the hash.
            << program.mov(hash_pass_vreg, hash_vreg) << program.and_(hash_pass_vreg, program.constant8(63))
            << program.mov(search_mask_bit_vreg, program.constant8(1))
            << program.shl(search_mask_bit_vreg, hash_pass_vreg)
            << program.or_(search_mask_vreg, search_mask_bit_vreg)

            /// Consume 2/4 8 bits of the hash.
            << program.shr(hash_vreg, program.constant8(8))

            << program.mov(hash_pass_vreg, hash_vreg) << program.and_(hash_pass_vreg, program.constant8(63))
            << program.mov(search_mask_bit_vreg, program.constant8(1))
            << program.shl(search_mask_bit_vreg, hash_pass_vreg)
            << program.or_(search_mask_vreg, search_mask_bit_vreg)

            /// Consume 3/4 8 bits of the hash.
            << program.shr(hash_vreg, program.constant8(8))

            << program.mov(hash_pass_vreg, hash_vreg) << program.and_(hash_pass_vreg, program.constant8(63))
            << program.mov(search_mask_bit_vreg, program.constant8(1))
            << program.shl(search_mask_bit_vreg, hash_pass_vreg)
            << program.or_(search_mask_vreg, search_mask_bit_vreg)

            /// Consume 4/4 8 bits of the hash.
            << program.shr(hash_vreg, program.constant8(8))

            << program.mov(hash_pass_vreg, hash_vreg) << program.and_(hash_pass_vreg, program.constant8(63))
            << program.mov(search_mask_bit_vreg, program.constant8(1))
            << program.shl(search_mask_bit_vreg, hash_pass_vreg) << program.or_(search_mask_vreg, search_mask_bit_vreg)

            << program.clear(search_mask_bit_vreg) << program.clear(hash_vreg) << program.clear(hash_pass_vreg);

    return search_mask_vreg;
}

void PartitionFilterBuildOperator::produce(const GenerationPhase phase, flounder::Program &program,
                                           CompilationContext &context)
{
    this->child()->produce(phase, program, context);
}

void PartitionFilterBuildOperator::consume(const GenerationPhase phase, flounder::Program &program,
                                           CompilationContext &context)
{
    if (phase == GenerationPhase::execution)
    {
        auto context_guard = flounder::ContextGuard{program, "Partition Filter Build"};

        auto build_term_vreg = context.symbols().get(this->_build_term);
        auto partition_id_vreg = context.symbols().get(PartitionOperator::partition_id_term);

        /// Get address von the bloom filter block.
        auto bloom_filter_address_vreg = PartitionFilter::emit_bloom_filter_address(
            program, this->_bloom_filter_descriptor, partition_id_vreg, this->_build_term_type, build_term_vreg);

        context.symbols().release(program, PartitionOperator::partition_id_term);

        /// Reg for search masks.
        auto search_mask_vreg = PartitionFilter::emit_search_mask(program, this->_build_term_type, build_term_vreg);

        context.symbols().release(program, this->_build_term);

        /// Set block.
        program << program.or_(program.mem(bloom_filter_address_vreg), search_mask_vreg)
                << program.clear(search_mask_vreg) << program.clear(bloom_filter_address_vreg);

        if (this->parent() != nullptr)
        {
            this->parent()->consume(phase, program, context);
        }
    }
    else if (phase == GenerationPhase::prefetching)
    {
        if (this->parent() != nullptr)
        {
            this->parent()->consume(phase, program, context);
        }
    }
}

void PartitionFilterBuildOperator::request_symbols(const GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        symbols.request(this->_build_term);
        symbols.request(PartitionOperator::partition_id_term);
    }

    this->child()->request_symbols(phase, symbols);
}

void PartitionFilterProbeOperator::produce(const GenerationPhase phase, flounder::Program &program,
                                           CompilationContext &context)
{
    this->child()->produce(phase, program, context);
}

void PartitionFilterProbeOperator::consume(const GenerationPhase phase, flounder::Program &program,
                                           CompilationContext &context)
{
    if (phase == GenerationPhase::prefetching)
    {
        this->parent()->consume(phase, program, context);
        return;
    }

    auto context_guard = flounder::ContextGuard{program, "Partition Filter Probe"};

    std::optional<flounder::Register> partition_id_vreg{std::nullopt};
    if (this->_radix_bits.size() == 1U)
    {
        partition_id_vreg = context.symbols().get(PartitionOperator::partition_id_term);
    }
    else
    {
        auto partition_hash_vreg = context.symbols().get(PartitionOperator::partition_hash_term);
        /// Calculate the partition id.
        auto calculator = PartitionCalculator{this->_radix_bits};
        auto tmp_partition_hash_vreg = program.vreg("tmp_partition_hash");
        partition_id_vreg = program.vreg("final_partition_id");
        program << program.request_vreg64(tmp_partition_hash_vreg) << program.request_vreg64(partition_id_vreg.value())
                << program.xor_(partition_id_vreg.value(), partition_id_vreg.value());

        auto shift_radix_bits = 0U;
        const auto last_pass = this->_radix_bits.size() - 1U;
        for (auto pass = 0U; pass <= last_pass; ++pass)
        {
            /// Bits for this pass, i.e., 4bits -> 15 = 1111
            const auto bit_mask = calculator.mask(pass);

            program << program.mov(tmp_partition_hash_vreg, partition_hash_vreg);
            if (shift_radix_bits > 0U)
            {
                /// Shift away the bits already used for partitioning.
                program << program.shr(tmp_partition_hash_vreg, program.constant8(shift_radix_bits));
            }

            /// Let the bits for this pass only remain.
            program << program.and_(tmp_partition_hash_vreg, program.constant8(bit_mask));

            /// Calculate the offset for this partition.
            if (pass < last_pass)
            {
                program << program.imul(tmp_partition_hash_vreg,
                                        program.constant32(calculator.multiplier(pass, last_pass)));
            }

            /// Add this pass to the partition id.
            program << program.add(partition_id_vreg.value(), tmp_partition_hash_vreg);

            /// For the next pass, the current used bits needs to be shifted out.
            shift_radix_bits += this->_radix_bits[pass];
        }

        program << program.clear(tmp_partition_hash_vreg);
        context.symbols().release(program, PartitionOperator::partition_hash_term);
    }

    auto probe_key_vreg = context.symbols().get(this->_probe_term);
    const auto probe_key_type = this->child()->schema().type(this->child()->schema().index(this->_probe_term).value());

    auto bloom_filter_block_address_vreg = PartitionFilter::emit_bloom_filter_address(
        program, this->_bloom_filter_descriptor, partition_id_vreg.value(), probe_key_type, probe_key_vreg);

    if (this->_radix_bits.size() == 1U)
    {
        context.symbols().release(program, PartitionOperator::partition_id_term);
    }
    else
    {
        program << program.clear(partition_id_vreg.value());
    }

    /// Reg for search masks.
    auto search_mask_vreg = PartitionFilter::emit_search_mask(program, probe_key_type, probe_key_vreg);

    context.symbols().release(program, this->_probe_term);

    /// Test block.
    auto test_vreg = program.vreg("test_filter_vreg");
    program << program.request_vreg64(test_vreg) << program.mov(test_vreg, program.mem(bloom_filter_block_address_vreg))
            << program.and_(test_vreg, search_mask_vreg) << program.cmp(test_vreg, search_mask_vreg)
            << program.jne(context.label_next_record()) << program.clear(search_mask_vreg) << program.clear(test_vreg)
            << program.clear(bloom_filter_block_address_vreg);

    this->parent()->consume(phase, program, context);
}

void PartitionFilterProbeOperator::request_symbols(const GenerationPhase phase, SymbolSet &symbols)
{
    if (phase == GenerationPhase::execution)
    {
        if (this->_radix_bits.size() == 1U)
        {
            symbols.request(PartitionOperator::partition_id_term);
        }
        else
        {
            symbols.request(PartitionOperator::partition_hash_term);
        }
        symbols.request(this->_probe_term);
    }

    this->child()->request_symbols(phase, symbols);
}