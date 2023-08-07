#pragma once

#include "b_link_tree.h"
#include "insert_separator_task.h"
#include "node.h"
#include "task.h"
#include <optional>

namespace db::index::blinktree {
template <typename K, typename V, class L> class LookupTask final : public Task<K, V, L>
{
private:
    constexpr static inline std::array<std::uint32_t, 2U> CYCLES{{/* TRAVERSAL = */ 247U, /*LOOKUP = */ 564U}};

public:
    LookupTask(const K key, L &listener) noexcept : Task<K, V, L>(key, listener)
    {
        this->annotation().cycles(CYCLES[0U]);
    }

    ~LookupTask() override { this->_listener.found(_worker_id, this->_key, _value); }

    [[nodiscard]] std::uint64_t trace_id() const noexcept override
    {
        return (Task<K, V, L>::TRACE_ID | 1U << 8U) | static_cast<std::uint8_t>(_is_lookup);
    }

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    V _value;
    std::uint16_t _worker_id{0U};
    bool _is_lookup{false};
};

template <typename K, typename V, typename L>
mx::tasking::TaskResult LookupTask<K, V, L>::execute(const std::uint16_t worker_id)
{
    auto *annotated_node = this->annotation().resource().template get<Node<K, V>>();

    // Is the node related to the key?
    if (annotated_node->high_key() <= this->_key)
    {
        this->annotate(annotated_node->right_sibling(), Node<K, V>::PrefetchHint::for_traversal());
        return mx::tasking::TaskResult::make_succeed(this);
    }

    // If we are accessing an inner node, pick the next related child.
    if (annotated_node->is_inner())
    {
        const auto child = annotated_node->child(this->_key);
        this->annotate(child, annotated_node->is_branch() == false ? Node<K, V>::PrefetchHint::for_traversal()
                                                                   : Node<K, V>::PrefetchHint::for_lookup());

        this->_is_lookup = annotated_node->is_branch();
        this->annotation().cycles(CYCLES[static_cast<std::uint8_t>(annotated_node->is_branch())]);
        return mx::tasking::TaskResult::make_succeed(this);
    }

    // We are accessing the correct leaf.
    const auto index = annotated_node->index(this->_key);
    if (annotated_node->leaf_key(index) == this->_key)
    {
        this->_value = annotated_node->value(index);
    }
    _worker_id = worker_id;

    return mx::tasking::TaskResult::make_remove();
}
} // namespace db::index::blinktree
