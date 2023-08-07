#pragma once

#include "b_link_tree.h"
#include "insert_separator_task.h"
#include "node.h"
#include "task.h"
#include <iostream>

namespace db::index::blinktree {
template <typename K, typename V, class L> class UpdateTask final : public Task<K, V, L>
{
private:
    constexpr static inline std::array<std::uint32_t, 2U> CYCLES{{/* TRAVERSAL = */ 255U, /*UPDATE = */ 811U}};

public:
    constexpr UpdateTask(const K key, const V value, L &listener) noexcept : Task<K, V, L>(key, listener), _value(value)
    {
        this->annotation().cycles(CYCLES[0U]);
    }

    ~UpdateTask() override = default;

    [[nodiscard]] std::uint64_t trace_id() const noexcept override
    {
        return (Task<K, V, L>::TRACE_ID | 1U << 7U) |
               static_cast<std::uint8_t>(this->annotation().is_readonly() == false);
    }

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    const V _value;
};

template <typename K, typename V, typename L>
mx::tasking::TaskResult UpdateTask<K, V, L>::execute(const std::uint16_t worker_id)
{
    auto *node = this->annotation().resource().template get<Node<K, V>>();

    // Is the node related to the key?
    if (node->high_key() <= this->_key)
    {
        this->annotate(node->right_sibling(), Node<K, V>::PrefetchHint::for_traversal());
        return mx::tasking::TaskResult::make_succeed(this);
    }

    // If we are accessing an inner node, pick the next related child.
    if (node->is_inner())
    {
        const auto child = node->child(this->_key);
        this->annotate(child, node->is_branch() == false ? Node<K, V>::PrefetchHint::for_traversal()
                                                         : Node<K, V>::PrefetchHint::for_update());
        this->annotate(node->is_branch() ? mx::tasking::annotation::access_intention::write
                                         : mx::tasking::annotation::access_intention::readonly);
        this->annotation().cycles(CYCLES[static_cast<std::uint8_t>(node->is_branch())]);
        return mx::tasking::TaskResult::make_succeed(this);
    }

    // If the task is still reading, but this is a leaf,
    // spawn again as writer.
    if (node->is_leaf() && this->annotation().is_readonly())
    {
        this->annotation().cycles(CYCLES[1U]);
        this->annotate(mx::tasking::annotation::access_intention::write);
        return mx::tasking::TaskResult::make_succeed(this);
    }

    // We are accessing the correct leaf.
    const auto index = node->index(this->_key);
    const auto key = node->leaf_key(index);
    if (key == this->_key)
    {
        node->value(index, this->_value);
        this->_listener.updated(worker_id, key, this->_value);
    }
    else
    {
        this->_listener.missing(worker_id, key);
    }

    return mx::tasking::TaskResult::make_remove();
}
} // namespace db::index::blinktree
