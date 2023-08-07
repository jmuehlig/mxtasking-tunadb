//#pragma once
//#include <cstdint>
//#include <db/config.h>
//#include <db/execution/compilation/compilation_node.h>
//#include <db/execution/record_token.h>
//#include <mx/tasking/dataflow/producer.h>
//#include <mx/tasking/runtime.h>
//
// namespace db::execution::compilation {
// class alignas(64) GraphContext
//{
// public:
//    GraphContext(const topology::PhysicalSchema &schema,
//                 mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &graph, const std::uint16_t worker_id,
//                 mx::tasking::dataflow::NodeInterface<execution::RecordSet> *node, RecordSet &&record_set) noexcept
//        : GraphContext(schema, graph, worker_id, worker_id, node, std::move(record_set))
//    {
//    }
//
//    GraphContext(const topology::PhysicalSchema &schema,
//                 mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &graph, const std::uint16_t worker_id,
//                 const std::uint16_t target_worker_id, mx::tasking::dataflow::NodeInterface<execution::RecordSet>
//                 *node, RecordSet &&record_set) noexcept
//        : _schema(schema), _graph(graph), _node(node), _worker_id(worker_id), _destination(target_worker_id),
//          _record_set(std::move(record_set)), _next_node_prefetch_mask(prefetch_mask(node))
//    {
//    }
//
//    GraphContext(const topology::PhysicalSchema &schema,
//                 mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &graph, const std::uint16_t worker_id,
//                 const mx::resource::ptr hash_table, mx::tasking::dataflow::NodeInterface<execution::RecordSet> *node,
//                 RecordSet &&record_set) noexcept
//        : _schema(schema), _graph(graph), _node(node), _worker_id(worker_id), _destination(hash_table),
//          _record_set(std::move(record_set)), _next_node_prefetch_mask(prefetch_mask(node->out()))
//    {
//        _record_set.secondary_input(hash_table);
//    }
//
//    ~GraphContext() noexcept = default;
//
//    static auto tile_offset() { return offsetof(GraphContext, _record_set) + RecordSet::tile_offset(); }
//
//    template <bool create_new_record_set> std::uintptr_t emit_from_worker(const std::uint16_t worker_id)
//    {
//        /// Mark all records as "visible".
//        _record_set.set();
//
//        if (_record_set.is_any_set())
//        {
//            /// Create annotation for the to-emit token.
//            auto annotation = mx::tasking::annotation{mx::tasking::annotation::access_intention::readonly,
//                                                      _record_set.tile(), _next_node_prefetch_mask};
//
//            /// If the destination is a squad, set it specifically.
//            /// Otherwise, the worker id of the tile will be set when creating a tile.
//            std::uint16_t target_worker_id;
//            if (std::holds_alternative<mx::resource::ptr>(_destination))
//            {
//                const auto squad = std::get<mx::resource::ptr>(_destination);
//                annotation.set(squad);
//                target_worker_id = squad.worker_id();
//            }
//            else
//            {
//                target_worker_id = std::get<std::uint16_t>(_destination);
//            }
//
//            /// Steal the (written) record set and emit it as token to the graph.
//            auto token = execution::RecordToken{std::move(_record_set), annotation};
//            _graph.emit(worker_id, _node, std::move(token));
//
//            /// Create a new temp. record set for the next tuples.
//            if constexpr (create_new_record_set)
//            {
//                _record_set = execution::RecordSet::make_record_set(_schema, target_worker_id);
//                _record_set.secondary_input(token.data().secondary_input());
//            }
//        }
//
//        return static_cast<std::uintptr_t>(_record_set.tile());
//    }
//
//    /**
//     * Emits the current RecordSet into the graph and opens a new one that can be written by operators.
//     * @param graph_context_pointer
//     * @return Pointer to the new tile that will be written.
//     */
//    __attribute__((noinline)) static std::uintptr_t emit(const std::uintptr_t graph_context_pointer)
//    {
//        auto *graph_context = reinterpret_cast<GraphContext *>(graph_context_pointer);
//        return graph_context->emit_from_worker<true>(graph_context->_worker_id);
//    }
//
// private:
//    /// The schema of the RecordToken that will be emitted.
//    const topology::PhysicalSchema &_schema;
//
//    /// The graph where tokens will be emitted.
//    mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &_graph;
//
//    /// Node that will emit data.
//    mx::tasking::dataflow::NodeInterface<execution::RecordSet> *_node;
//
//    /// The worker id which will emit.
//    const std::uint16_t _worker_id;
//
//    /// The target of the emitted token. Can be a specific worker id or a task squad.
//    const std::variant<std::uint16_t, mx::resource::ptr> _destination;
//
//    /// The actual record set that will be emitted.
//    execution::RecordSet _record_set;
//
//    /// Prefetch mask for the next node.
//    const mx::tasking::PrefetchMask _next_node_prefetch_mask;
//
//    [[nodiscard]] static mx::tasking::PrefetchMask prefetch_mask(
//        mx::tasking::dataflow::NodeInterface<execution::RecordSet> *subsequent_node)
//    {
//        if (subsequent_node != nullptr && mx::tasking::runtime::prefetch_distance().is_enabled())
//        {
//            auto mask = mx::tasking::PrefetchPattern::sequential().size(sizeof(data::Tile)).is_temporal().build();
//
//            if (auto *next = dynamic_cast<CompilationNode *>(subsequent_node))
//            {
//                const auto node_mask = next->prefetch_mask();
//                if (node_mask.has_value())
//                {
//                    mask |= node_mask.value();
//                }
//            }
//
//            return mask;
//        }
//
//        return mx::tasking::PrefetchMask{};
//    }
//};
//} // namespace db::execution::compilation