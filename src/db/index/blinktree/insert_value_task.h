#pragma once

#include "b_link_tree.h"
#include "insert_separator_task.h"
#include "node.h"
#include "task.h"
#include <mx/tasking/runtime.h>
#include <vector>

namespace db::index::blinktree {
template <typename K, typename V, class L> class InsertValueTask final : public Task<K, V, L>
{
private:
    constexpr static inline std::array<std::uint32_t, 2U> CYCLES{{/* TRAVERSAL = */ 440U, /*INSERT = */ 1015U}};

public:
    constexpr InsertValueTask(const K key, const V value, BLinkTree<K, V> *tree, L &listener) noexcept
        : Task<K, V, L>(key, listener), _tree(tree), _value(value)
    {
        this->annotation().cycles(CYCLES[0U]);
    }

    ~InsertValueTask() override = default;

    [[nodiscard]] std::uint64_t trace_id() const noexcept override
    {
        return (Task<K, V, L>::TRACE_ID | 1U << 5U) |
               static_cast<std::uint8_t>(this->annotation().is_readonly() == false);
    }

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    BLinkTree<K, V> *_tree;
    const V _value;
};

template <typename K, typename V, class L>
mx::tasking::TaskResult InsertValueTask<K, V, L>::execute(const std::uint16_t worker_id)
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
        const auto is_branch = annotated_node->is_branch();

        const auto child = annotated_node->child(this->_key);
        this->annotate(child,
                       is_branch ? Node<K, V>::PrefetchHint::for_insert() : Node<K, V>::PrefetchHint::for_traversal());
        this->annotate(is_branch ? mx::tasking::annotation::access_intention::write
                                 : mx::tasking::annotation::access_intention::readonly);
        this->annotation().cycles(CYCLES[static_cast<std::uint8_t>(is_branch)]);
        return mx::tasking::TaskResult::make_succeed(this);
    }

    // Is it a leaf, but we are still reading? Upgrade to writer.
    if (annotated_node->is_leaf() && this->annotation().is_readonly())
    {
        this->annotate(this->annotation().resource(), Node<K, V>::PrefetchHint::for_insert());
        this->annotate(mx::tasking::annotation::access_intention::write);
        this->annotation().cycles(CYCLES[1U]);
        return mx::tasking::TaskResult::make_succeed(this);
    }

    // At this point, we are accessing the related leaf and we are in writer mode.
    const auto index = annotated_node->index(this->_key);
    if (index < annotated_node->size() && annotated_node->leaf_key(index) == this->_key)
    {
        this->_listener.inserted(worker_id, this->_key, this->_value);
        return mx::tasking::TaskResult::make_remove();
    }

    if (annotated_node->full() == false)
    {
        annotated_node->insert(index, this->_value, this->_key);
        this->_listener.inserted(worker_id, this->_key, this->_value);
        return mx::tasking::TaskResult::make_remove();
    }

    auto [right, key] = this->_tree->split(this->annotation().resource(), this->_key, this->_value);
    if (annotated_node->parent() != nullptr)
    {
        auto *task = mx::tasking::runtime::new_task<InsertSeparatorTask<K, V, L>>(worker_id, key, right, this->_tree,
                                                                                  this->_listener);
        task->annotate(annotated_node->parent(), Node<K, V>::PrefetchHint::for_insert());
        return mx::tasking::TaskResult::make_succeed_and_remove(task);
    }

    this->_tree->create_new_root(this->annotation().resource(), right, key);
    this->_listener.inserted(worker_id, this->_key, this->_value);
    return mx::tasking::TaskResult::make_remove();
}
} // namespace db::index::blinktree
