#pragma once

#include "compilation_node.h"
#include <cstdint>
#include <db/execution/record_token.h>
#include <db/topology/physical_schema.h>
#include <mx/resource/ptr.h>
#include <mx/tasking/dataflow/producer.h>

namespace db::execution::compilation {
class AbstractRecordSetEmitter
{
public:
    AbstractRecordSetEmitter(const std::uint16_t worker_id, const topology::PhysicalSchema &schema,
                             mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &graph,
                             mx::tasking::dataflow::NodeInterface<execution::RecordSet> *node)
        : _worker_id(worker_id), _schema(schema), _graph(graph), _node(node),
          _record_set(RecordSet::make_record_set(_schema, _worker_id)),
          _prefetch_descriptor(make_prefetch_descriptor(node)),
          _boundness(node->out()->annotation().resource_boundness())
    {
    }

    virtual ~AbstractRecordSetEmitter() = default;

protected:
    /// The worker id, that emits data. Will be the
    /// same as mapped to the task squad.
    const std::uint16_t _worker_id;

    /// The schema needed by record sets. Need to be stored
    /// at the output provider to live during query execution.
    const topology::PhysicalSchema &_schema;

    /// The graph where record sets will be emitted to.
    mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &_graph;

    /// The node that emits the record sets.
    mx::tasking::dataflow::NodeInterface<execution::RecordSet> *_node;

    /// Record set to emit.
    execution::RecordSet _record_set;

    /// Prefetch mask for the next node.
    const mx::tasking::PrefetchDescriptor _prefetch_descriptor;

    /// Resource boundness.
    const enum mx::tasking::annotation::resource_boundness _boundness;

    [[nodiscard]] static mx::tasking::PrefetchDescriptor make_prefetch_descriptor(
        mx::tasking::dataflow::NodeInterface<execution::RecordSet> *emitting_node)
    {
        if (mx::tasking::runtime::prefetch_distance().is_enabled() && emitting_node != nullptr)
        {
            if (auto *operator_node = dynamic_cast<db::execution::compilation::CompilationNode *>(emitting_node);
                operator_node != nullptr)
            {
                const auto count_prefetches = operator_node->count_prefetches();
                const auto prefetch_callback = operator_node->prefetch_callback();
                if (count_prefetches > 0U && prefetch_callback.has_value())
                {
                    return mx::tasking::PrefetchCallback::make(count_prefetches, prefetch_callback.value());
                }
            }
        }

        return mx::tasking::PrefetchDescriptor{};
    }
};

class MaterializeEmitter final : AbstractRecordSetEmitter
{
public:
    MaterializeEmitter(const std::uint16_t worker_id, const topology::PhysicalSchema &schema,
                       mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &graph,
                       mx::tasking::dataflow::NodeInterface<execution::RecordSet> *node)
        : AbstractRecordSetEmitter(worker_id, schema, graph, node)
    {
    }

    ~MaterializeEmitter() override = default;

    __attribute__((noinline)) static auto tile_offset()
    {
        return offsetof(MaterializeEmitter, _record_set) + RecordSet::tile_offset();
    }

    __attribute__((noinline)) static void *emit(const std::uintptr_t materialize_emitter_address)
    {
        return reinterpret_cast<MaterializeEmitter *>(materialize_emitter_address)->emit_record_set_to_graph();
    }

private:
    void *emit_record_set_to_graph()
    {
        if (_record_set.tile().get<data::PaxTile>()->size() > 0U)
        {
            /// Create annotation for the to-emit token.
            auto annotation = mx::tasking::annotation{_worker_id};
            annotation.set(mx::tasking::PrefetchHint{_prefetch_descriptor, _record_set.tile()});

            if constexpr (mx::tasking::config::is_consider_resource_bound_workers())
            {
                annotation.set(_boundness);
            }

            /// Steal the (written) record set and emit it as token to the graph.
            auto token = execution::RecordToken{std::move(_record_set), annotation};
            _graph.emit(_worker_id, _node, std::move(token));

            _record_set = execution::RecordSet::make_record_set(_schema, _worker_id);
        }

        return _record_set.tile().get();
    }
};

/**
 * Pre-Partitions are local partitions.
 * Every worker partitions its tuple into (pre-partitions)
 * that are scheduled as a batch and partitioned into
 * more fine-grained partitions.
 * The last partition pass will end up in "normal" partitions.
 */
class PartitionEmitter final : AbstractRecordSetEmitter
{
public:
    PartitionEmitter(const std::uint16_t worker_id, mx::resource::ptr partition, const topology::PhysicalSchema &schema,
                     mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &graph,
                     mx::tasking::dataflow::NodeInterface<execution::RecordSet> *node)
        : AbstractRecordSetEmitter(worker_id, schema, graph, node), _partition(partition)
    {
    }

    ~PartitionEmitter() override = default;

    __attribute__((noinline)) static void emit(const std::uintptr_t partition_emitter_address)
    {
        reinterpret_cast<PartitionEmitter *>(partition_emitter_address)->emit_record_set_to_graph(true, std::nullopt);
    }

    __attribute__((noinline)) static auto tile_offset()
    {
        return offsetof(PartitionEmitter, _record_set) + RecordSet::tile_offset();
    }

    [[nodiscard]] mx::resource::ptr partition() const noexcept { return _partition; }

    void emit_record_set_to_graph(const bool is_create_new_record_set, std::optional<std::uint32_t> tile_size)
    {
        if (tile_size.has_value() && tile_size.value() > 0U)
        {
            _record_set.tile().get<data::PaxTile>()->size(tile_size.value());
        }
        else
        {
            tile_size = _record_set.tile().get<data::PaxTile>()->size();
        }

        if (tile_size.value_or(0U) > 0U)
        {
            _record_set.tile().get<data::PaxTile>()->size(tile_size.value());

            /// Create annotation for the to-emit token.
            auto annotation = mx::tasking::annotation{mx::tasking::annotation::access_intention::readonly, _partition};
            annotation.set(mx::tasking::PrefetchHint{_prefetch_descriptor, _record_set.tile()});

            if constexpr (mx::tasking::config::is_consider_resource_bound_workers())
            {
                annotation.set(_boundness);
            }

            /// Steal the (written) record set and emit it as token to the graph.
            _record_set.secondary_input(_partition); // TODO: Maybe remove and use destination of task annotation
            auto token = execution::RecordToken{std::move(_record_set), annotation};
            _graph.emit(_worker_id, _node, std::move(token));

            if (is_create_new_record_set)
            {
                _record_set = execution::RecordSet::make_record_set(_schema, _worker_id);
            }
        }
    }

private:
    /// Partition the record set will be emitted to.
    const mx::resource::ptr _partition;
};
} // namespace db::execution::compilation