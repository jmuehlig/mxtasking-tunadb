#pragma once

#include "operator_interface.h"
#include <db/execution/compilation/flounder_record_set_emitter.h>
#include <db/execution/compilation/record_token.h>
#include <db/expression/term.h>
#include <fmt/core.h>
#include <mx/memory/global_heap.h>
#include <mx/tasking/runtime.h>

namespace db::execution::compilation {
class PartitionCalculator
{
public:
    constexpr PartitionCalculator(const std::vector<std::uint8_t> &radix_bits) noexcept : _radix_bits(radix_bits) {}

    ~PartitionCalculator() noexcept = default;

    [[nodiscard]] std::uint16_t mask(const std::uint8_t pass) const noexcept { return count(pass) - 1U; }

    [[nodiscard]] std::uint16_t count(const std::uint8_t pass) const noexcept
    {
        return std::pow(2U, _radix_bits[pass]);
    }

    [[nodiscard]] std::uint16_t multiplier(const std::uint8_t current_pass, const std::uint8_t last_pass) const noexcept
    {
        if (current_pass == last_pass)
        {
            return 1U;
        }

        if (current_pass == last_pass - 1U)
        {
            return count(last_pass);
        }

        return count(current_pass + 1U) * multiplier(current_pass + 1U, last_pass);
    }

private:
    const std::vector<std::uint8_t> &_radix_bits;
};

class PartitionOperator final : public UnaryOperator
{
public:
    inline static expression::Term partition_hash_term = expression::Term::make_attribute("partition_hash");
    inline static expression::Term partition_id_term = expression::Term::make_attribute("partition_id");

    PartitionOperator(topology::PhysicalSchema &&schema, std::vector<expression::Term> partition_terms,
                      const std::vector<std::uint8_t> &radix_bits, const std::uint8_t pass) noexcept
        : _pass(pass), _radix_bits(radix_bits), _schema(std::move(schema)), _partition_terms(std::move(partition_terms))
    {
    }

    ~PartitionOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(const GenerationPhase /*phase*/) override
    {
        return nullptr;
    }

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        if (this->is_first_pass() == false)
        {
            return std::make_optional(OperatorProgramContext{this->child().get()});
        }

        return child()->dependencies();
    }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::TokenGenerator<RecordSet>> input_data_generator()
        const override
    {
        if (this->is_first_pass() && this->child() != nullptr)
        {
            return this->child()->input_data_generator();
        }

        return nullptr;
    }

    [[nodiscard]] std::uint8_t count_prefeches() const override
    {
        if (this->is_first_pass()) [[likely]]
        {
            return this->child()->count_prefeches();
        }

        return _count_prefetches;
    }

    [[nodiscard]] std::string to_string() const override
    {
        if (this->is_first_pass())
        {
            return child()->to_string();
        }

        return fmt::format("Partition (#{:d}) {{ {} }}", _pass, this->child()->pipeline_identifier());
    }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        container.insert(std::make_pair("#Radix Bits", std::to_string(std::uint32_t(_radix_bits[_pass]))));
        container.insert(
            std::make_pair("#Partitions", std::to_string(std::uint32_t(std::pow(2U, _radix_bits[_pass])))));

        if (is_first_pass())
        {
            this->child()->emit_information(container);
        }
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

private:
    /// Consecutive number of the partition phase, starting at 0.
    const std::uint8_t _pass{0U};

    /// Number of bits used for partitioning (i.e., 3 bits = 8 partitions).
    /// Bits of all phases are stored to calculate sub partitions;
    /// current bits are _radix_bits[_phase].
    const std::vector<std::uint8_t> _radix_bits;

    /// Schema of the emitted data.
    topology::PhysicalSchema _schema;

    /// Terms that will be used for partitioning.
    /// All records with the same join term/same group
    /// will be distributed to the same partition.
    std::vector<expression::Term> _partition_terms;

    std::uint8_t _count_prefetches;

    [[nodiscard]] bool is_first_pass() const noexcept { return _pass == 0U; }

    [[nodiscard]] bool is_last_pass() const noexcept { return is_last_pass(_pass); }

    [[nodiscard]] bool is_last_pass(const std::uint8_t pass) const noexcept { return pass == _radix_bits.size() - 1U; }
};

/**
 * Every worker writes tuples to its local set of partition emitters.
 * Each worker has multiple partition emitters:
 *  - For the last pass, one per partition.
 *  - For every but the last pass, one per partition but partitions are not shared.
 */
class MaterializePartitionedOutputProvider final : public OutputProviderInterface
{
public:
    class WorkerLocalPartition
    {
    public:
        template <std::uint64_t S> struct tile_size_type
        {
            using size_t = std::uint32_t;
        };

        template <> struct tile_size_type<64U>
        {
            using size_t = std::uint8_t;
        };

        template <> struct tile_size_type<128U>
        {
            using size_t = std::uint8_t;
        };

        template <> struct tile_size_type<256>
        {
            using size_t = std::uint8_t;
        };

        template <> struct tile_size_type<512U>
        {
            using size_t = std::uint16_t;
        };

        template <> struct tile_size_type<1024>
        {
            using size_t = std::uint16_t;
        };

        using size_type = tile_size_type<config::tuples_per_tile()>::size_t;

        [[nodiscard]] static std::size_t size(const std::uint64_t count_partitions) noexcept
        {
            return sizeof(PartitionEmitter) * count_partitions + sizeof(size_type) * count_partitions;
        }

        [[nodiscard]] static std::size_t partition_emiter_offset(const std::uint64_t count_partitions) noexcept
        {
            return sizeof(size_type) * count_partitions;
        }

        [[nodiscard]] PartitionEmitter *partition_emitter(const std::uint64_t count_partitions) noexcept
        {
            return reinterpret_cast<PartitionEmitter *>(std::uintptr_t(this) +
                                                        partition_emiter_offset(count_partitions));
        }

        [[nodiscard]] size_type *partition_tile_sizes() noexcept { return reinterpret_cast<size_type *>(this); }
    };

    MaterializePartitionedOutputProvider(const std::uint16_t count_workers,
                                         const std::vector<mx::resource::ptr> &partitions,
                                         topology::PhysicalSchema &&schema, const bool is_last_pass,
                                         std::unique_ptr<std::byte> &&bloom_filter = nullptr)
        : _is_last_pass(is_last_pass), _schema(std::move(schema)), _count_workers(count_workers),
          _partitions(partitions), _bloom_filter(std::move(bloom_filter))
    {
        _partition_emitter.resize(count_workers, nullptr);
    }

    ~MaterializePartitionedOutputProvider() override
    {
        for (auto worker_id = 0U; worker_id < _count_workers; ++worker_id)
        {
            if (_partition_emitter[worker_id] != nullptr)
            {
                /// The last pass will only have one set of partitions.
                /// Every but the last will have one set of partitions for every worker,
                /// to emit the batches locally.
                const auto count_partitions = _is_last_pass ? _partitions.size() : _partitions.size() / _count_workers;

                auto *partition_emitter = _partition_emitter[worker_id]->partition_emitter(count_partitions);
                for (auto partition_id = 0U; partition_id < count_partitions; ++partition_id)
                {
                    partition_emitter[partition_id].~PartitionEmitter();
                }
                mx::memory::GlobalHeap::free(_partition_emitter[worker_id],
                                             WorkerLocalPartition::size(count_partitions));
            }
        }

        if (this->_is_last_pass == false)
        {
            for (auto squad : _partitions)
            {
                mx::tasking::runtime::delete_squad<mx::tasking::TaskSquad>(squad);
            }
        }

        /// Release the bloom filter manually. This is a bit dirty,
        /// since we used a unique_ptr. The unique_ptr, however, is to
        /// indicate the ownership. The bloomfilter was allocated using
        /// aligned alloc which will not be freed correctly by unique_ptr.
        auto *bloom_filter = _bloom_filter.release();
        std::free(bloom_filter);
    }

    std::uintptr_t get(const std::uint16_t worker_id,
                       std::optional<std::reference_wrapper<const RecordToken>> /*token*/,
                       mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &graph,
                       mx::tasking::dataflow::NodeInterface<execution::RecordSet> *node) override
    {
        assert(worker_id < _partition_emitter.size());

        if (_partition_emitter[worker_id] == nullptr)
        {
            /// For every but the last pass, the planner created |partitions| * |worker| partitions.
            /// The "real" number (for every but the last pass) is the number of partitions / number of workers.
            const auto count_partitions = _partitions.size() / (_is_last_pass ? 1U : _count_workers);

            /// For every worker, we build a set of "Graph Context" objects, where each graph context holds
            /// its own record set that could be annotated (with a target worker id or a task squad).
            /// Thus, each worker will have an array with graph contexts for each partition.
            auto *worker_local_partition_emitter =
                reinterpret_cast<WorkerLocalPartition *>(mx::memory::GlobalHeap::allocate(
                    mx::tasking::runtime::numa_node_id(worker_id), WorkerLocalPartition::size(count_partitions)));
            std::memset(worker_local_partition_emitter, '\0',
                        sizeof(WorkerLocalPartition::size_type) * count_partitions);
            auto *partition_emiter = worker_local_partition_emitter->partition_emitter(count_partitions);

            /// Skip partitions that do not belong to this worker since every worker
            /// has its own partitions.
            const auto partition_offset = this->_is_last_pass ? 0U : worker_id * count_partitions;

            /// Every worker gets its own record set for each partition.
            for (auto partition_id = 0U; partition_id < count_partitions; ++partition_id)
            {
                std::ignore = new (&partition_emiter[partition_id])
                    PartitionEmitter(worker_id, _partitions[partition_offset + partition_id], _schema, graph, node);
            }

            _partition_emitter[worker_id] = worker_local_partition_emitter;
        }

        return std::uintptr_t(_partition_emitter[worker_id]);
    }

    [[nodiscard]] const std::vector<WorkerLocalPartition *> &partition_emitter() const noexcept
    {
        return _partition_emitter;
    }

    [[nodiscard]] const std::vector<mx::resource::ptr> &partitions() const noexcept { return _partitions; }

private:
    /// Indicates whether the worker share the partitions (last phase = true)
    /// or each worker has its own set of partitions (last phase = false).
    const bool _is_last_pass;

    /// Schema of the data emitted to the graph.
    const topology::PhysicalSchema _schema;

    /// Number of workers.
    const std::uint16_t _count_workers;

    /// Each worker (vector) has a bunch of partition emitters (one for each partition).
    std::vector<WorkerLocalPartition *> _partition_emitter;

    /// Partitions. In case, this is NOT the last partition pass,
    /// every worker has its own partition set:
    /// [w0_p0, w0_p1, ..., w0_pX, w1_p0, w1_p1, ..., w1_pX, ... wY_pX]
    std::vector<mx::resource::ptr> _partitions;

    /// The bloom filter will only be held because the output provider
    /// lives to the end of the query.
    std::unique_ptr<std::byte> _bloom_filter;
};

/**
 * The finalizer will be given out as finalize data, once per core.
 * Every core will emit its partition emitters (with left tuples on the
 * record set).
 * If IS_LAST_PASS == false, every worker will spawn its partitions.
 * If IS_LAST_PASS == true, the last worker will spawn all partitions.
 */
template <bool IS_LAST_PASS> class PartitionFinalizer
{
public:
    constexpr PartitionFinalizer(
        const std::uint16_t worker_id,
        const std::vector<MaterializePartitionedOutputProvider::WorkerLocalPartition *> &partition_emitters,
        const std::uint32_t count_partitions, std::atomic_uint16_t *awaited_workers,
        std::optional<std::reference_wrapper<const std::vector<mx::resource::ptr>>> &&partitions,
        const enum mx::tasking::annotation::resource_boundness boundness, const bool is_spawn_partitions)
        : _worker_id(worker_id), _partition_emitters(partition_emitters),
          _count_worker_local_partitions(count_partitions), _awaited_workers(awaited_workers),
          _last_pass_partitions(std::move(partitions)), _resource_boundness(boundness),
          _is_spawn_partitions(is_spawn_partitions)
    {
    }

    constexpr PartitionFinalizer(
        const std::uint16_t worker_id,
        const std::vector<MaterializePartitionedOutputProvider::WorkerLocalPartition *> &partition_emitters,
        const std::uint32_t count_partitions)
        : PartitionFinalizer(worker_id, partition_emitters, count_partitions, nullptr, std::nullopt,
                             mx::tasking::annotation::resource_boundness::mixed, true)
    {
    }

    ~PartitionFinalizer() noexcept = default;

    __attribute__((noinline)) static void emit(const std::uintptr_t partition_finalizer_address)
    {
        assert(partition_finalizer_address != 0U);
        auto *partition_finalizer = reinterpret_cast<PartitionFinalizer<IS_LAST_PASS> *>(partition_finalizer_address);
        auto *worker_local_partition_emitters =
            partition_finalizer->_partition_emitters[partition_finalizer->_worker_id];

        if (worker_local_partition_emitters != nullptr)
        {
            auto *partition_tile_sizes = worker_local_partition_emitters->partition_tile_sizes();
            auto *partition_emitters =
                worker_local_partition_emitters->partition_emitter(partition_finalizer->_count_worker_local_partitions);
            for (auto partition_id = 0U; partition_id < partition_finalizer->_count_worker_local_partitions;
                 ++partition_id)
            {
                auto &partition_emitter = partition_emitters[partition_id];
                partition_emitter.emit_record_set_to_graph(false, partition_tile_sizes[partition_id]);

                /// For pre-passes (not the last), every worker spawns its own partitions.
                if constexpr (IS_LAST_PASS == false)
                {
                    mx::tasking::runtime::spawn(partition_emitter.partition(), partition_finalizer->_worker_id);
                }
            }
        }

        /// If this is the last partition pass (IS_LAST_PASS),
        /// the last worker will emit all partitions.
        if constexpr (IS_LAST_PASS)
        {
            if (partition_finalizer->_awaited_workers->fetch_sub(1U) == 1U)
            {
                if (partition_finalizer->_is_spawn_partitions)
                {
                    for (const auto partition : partition_finalizer->_last_pass_partitions.value().get())
                    {
                        mx::tasking::runtime::spawn(partition, partition_finalizer->_resource_boundness,
                                                    partition_finalizer->_worker_id);
                    }
                }
                else
                {
                    for (const auto partition : partition_finalizer->_last_pass_partitions.value().get())
                    {
                        mx::tasking::runtime::flush_squad(partition);
                    }
                }

                std::free(partition_finalizer->_awaited_workers);
            }
        }

        std::free(partition_finalizer);
    }

private:
    const std::uint16_t _worker_id;

    const std::vector<MaterializePartitionedOutputProvider::WorkerLocalPartition *> &_partition_emitters;

    const std::uint32_t _count_worker_local_partitions;

    /// For the last pass, the last worker will emit all partitions.
    std::atomic_uint16_t *_awaited_workers{nullptr};

    /// Since not every worker has partitions, for the last pass, we store
    /// all partitions separatly, so the last worker cann emit all partitions.
    std::optional<std::reference_wrapper<const std::vector<mx::resource::ptr>>> _last_pass_partitions{std::nullopt};

    const enum mx::tasking::annotation::resource_boundness _resource_boundness{
        mx::tasking::annotation::resource_boundness::mixed};

    /// Some times (build side and no bloom filter), we can delay to spawn the partitions,
    /// so that the build side is executed right before the probe side to have to hash table
    /// in cache.
    bool _is_spawn_partitions;
};

class PartitionNodeCompleteCallback final
    : public mx::tasking::dataflow::annotation<RecordSet>::CompletionCallbackInterface
{
public:
    constexpr PartitionNodeCompleteCallback() noexcept = default;
    ~PartitionNodeCompleteCallback() noexcept override = default;

    [[nodiscard]] bool is_complete() noexcept override { return --_count == 0U; }

private:
    std::uint8_t _count{3U};
};

class MaterializePartitionOperator final : public UnaryOperator
{
public:
    MaterializePartitionOperator(topology::PhysicalSchema &&schema, std::vector<mx::resource::ptr> &&partitions,
                                 const bool is_last_pass, const bool is_emit_last_pass,
                                 std::unique_ptr<std::byte> &&bloom_filter) noexcept
        : _is_last_pass(is_last_pass), _is_emit_last_pass(is_emit_last_pass), _schema(std::move(schema)),
          _partitions(std::move(partitions))
    {
        this->_output_provider = std::make_unique<MaterializePartitionedOutputProvider>(
            mx::tasking::runtime::workers(), _partitions, topology::PhysicalSchema{_schema}, is_last_pass,
            std::move(bloom_filter));
    }

    MaterializePartitionOperator(topology::PhysicalSchema &&schema, const std::vector<mx::resource::ptr> &partitions,
                                 const bool is_last_pass, const bool is_emit_last_pass) noexcept
        : MaterializePartitionOperator(std::move(schema), std::vector<mx::resource::ptr>{partitions}, is_last_pass,
                                       is_emit_last_pass, nullptr)
    {
    }

    ~MaterializePartitionOperator() noexcept override = default;

    void produce(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;
    void consume(GenerationPhase phase, flounder::Program &program, CompilationContext &context) override;

    void request_symbols(GenerationPhase phase, SymbolSet &symbols) override;

    [[nodiscard]] std::optional<
        std::pair<mx::tasking::dataflow::annotation<RecordSet>::FinalizationType, std::vector<mx::resource::ptr>>>
    finalization_data() noexcept override;

    [[nodiscard]] std::unique_ptr<OutputProviderInterface> output_provider(GenerationPhase phase) override;

    [[nodiscard]] std::optional<OperatorProgramContext> dependencies() const override
    {
        return child()->dependencies();
    }

    [[nodiscard]] bool is_finalize_pipeline_premature() const noexcept override
    {
        if constexpr (config::is_relocate_radix_join() == false)
        {
            return false;
        }
        else
        {
            return _is_emit_last_pass == false;
        }
    }

    [[nodiscard]] std::string to_string() const override { return child()->to_string(); }

    void emit_information(std::unordered_map<std::string, std::string> &container) override
    {
        container.insert(std::make_pair("Schema", _schema.to_string()));

        this->child()->emit_information(container);
    }

    [[nodiscard]] const topology::PhysicalSchema &schema() const override { return _schema; }

    [[nodiscard]] std::unique_ptr<mx::tasking::dataflow::annotation<RecordSet>::CompletionCallbackInterface>
    completion_callback() override
    {
        return std::make_unique<PartitionNodeCompleteCallback>();
    }

private:
    /// Flag if this is the last partition pass before building/probing.
    const bool _is_last_pass;

    /// If we have no bloom filter, we can delay the build phase right before the probe phase.
    /// Doing so, the probe will find the hashtable in cache.
    const bool _is_emit_last_pass{true};

    /// Schema of the emitted data.
    topology::PhysicalSchema _schema;

    /// List of available partitions (could be hash tables if this is the last phase).
    std::vector<mx::resource::ptr> _partitions;

    /// Virtual register containing the pointer to the partition emitter array.
    std::optional<flounder::Register> _partition_emitter_array_vreg{std::nullopt};

    /// The output provider for the finalization phase will be created and stored temporarly.
    std::unique_ptr<MaterializePartitionedOutputProvider> _output_provider{nullptr};
};
} // namespace db::execution::compilation