#pragma once

#include "operator_interface.h"

namespace db::execution::compilation {

class BloomFilterDescriptor
{
public:
    constexpr BloomFilterDescriptor(std::byte *filter, const std::uint64_t blocks_per_partition) noexcept
        : _filter(filter), _blocks_per_partition(blocks_per_partition)
    {
    }

    ~BloomFilterDescriptor() noexcept = default;

    [[nodiscard]] std::byte *filter() const noexcept { return _filter; }
    [[nodiscard]] std::uint64_t blocks_per_partition() const noexcept { return _blocks_per_partition; }

private:
    std::byte *_filter;
    const std::uint64_t _blocks_per_partition;
};

class PartitionFilter
{
public:
    [[nodiscard]] static flounder::Register emit_bloom_filter_address(flounder::Program &program,
                                                                      BloomFilterDescriptor bloom_filter_descriptor,
                                                                      flounder::Register partition_id_vreg,
                                                                      type::Type key_type, flounder::Register key_vreg);
    [[nodiscard]] static flounder::Register emit_search_mask(flounder::Program &program, type::Type key_type,
                                                             flounder::Register key_vreg);
};

class PartitionFilterBuildOperator final : public UnaryOperator
{
public:
    explicit PartitionFilterBuildOperator(const expression::Term &build_term, type::Type build_term_type,
                                          BloomFilterDescriptor bloom_filter_descriptor)
        : _build_term(build_term), _build_term_type(build_term_type), _bloom_filter_descriptor(bloom_filter_descriptor)
    {
    }

    ~PartitionFilterBuildOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return this->child()->dependencies();
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) override
    {
        return this->child()->output_provider(phase);
    }

    [[nodiscard]] std::string to_string() const override { return child()->to_string(); }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        container.insert(std::make_pair("Build #Blocks / Partition",
                                        util::string::shorten_number(_bloom_filter_descriptor.blocks_per_partition())));

        this->child()->emit_information(container);
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    /// The schema of this operator will always be empty.
    const topology::PhysicalSchema _schema;

    /// The term to build the hash table with. The key
    /// will be inserted into the bloom filter.
    const expression::Term _build_term;

    const type::Type _build_term_type;

    /// Descriptor of the bloom filter to calculate offset to
    /// the bloom filter and mod count bits.
    const BloomFilterDescriptor _bloom_filter_descriptor;
};

class PartitionFilterProbeOperator final : public UnaryOperator
{
public:
    PartitionFilterProbeOperator(topology::PhysicalSchema &&schema, const expression::Term &probe_term,
                                 const std::vector<std::uint8_t> &radix_bits,
                                 BloomFilterDescriptor bloom_filter_descriptor) noexcept
        : _schema(std::move(schema)), _probe_term(probe_term), _radix_bits(radix_bits),
          _bloom_filter_descriptor(bloom_filter_descriptor)
    {
    }

    ~PartitionFilterProbeOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return this->child()->dependencies();
    }

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(const GenerationPhase phase) override
    {
        return this->child()->output_provider(phase);
    }

    [[nodiscard]] std::string to_string() const override { return child()->to_string(); }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        const auto count_partitions =
            std::accumulate(_radix_bits.begin() + 1U, _radix_bits.end(), std::pow(2U, _radix_bits.front()),
                            [](const auto sum, const auto bits) { return sum * std::pow(2U, bits); });

        container.insert(std::make_pair("Probed #Blocks / Partition",
                                        util::string::shorten_number(_bloom_filter_descriptor.blocks_per_partition())));
        //        container.insert(std::make_pair("#Partitions", util::string::shorten_number(count_partitions)));
        container.insert(std::make_pair(
            "Probed Bloom Filter Size",
            util::string::shorten_data_size(_bloom_filter_descriptor.blocks_per_partition() * count_partitions * 8U)));

        this->child()->emit_information(container);
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    topology::PhysicalSchema _schema;

    const expression::Term _probe_term;

    /// Radix bits to calculate the partition if needed.
    const std::vector<std::uint8_t> _radix_bits;

    /// Descriptor of the bloom filter to calculate offset to
    /// the bloom filter and mod count bits.
    const BloomFilterDescriptor _bloom_filter_descriptor;
};

} // namespace db::execution::compilation