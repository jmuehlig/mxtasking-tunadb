#pragma once

#include "b_link_tree.h"
#include "node.h"
#include "task.h"
#include <mx/tasking/runtime.h>

namespace db::index::blinktree {
template <typename K, typename V, class L> class InsertSeparatorTask final : public Task<K, V, L>
{
private:
    constexpr static auto inline CYCLES = 1805U;

public:
    constexpr InsertSeparatorTask(const K key, const mx::resource::ptr separator, BLinkTree<K, V> *tree,
                                  L &listener) noexcept
        : Task<K, V, L>(key, listener), _tree(tree), _separator(separator)
    {
        this->annotation().cycles(CYCLES);
    }

    ~InsertSeparatorTask() override = default;

    [[nodiscard]] std::uint64_t trace_id() const noexcept override { return (Task<K, V, L>::TRACE_ID | 1U << 6U); }

    mx::tasking::TaskResult execute(std::uint16_t worker_id) override;

private:
    BLinkTree<K, V> *_tree;
    mx::resource::ptr _separator;
};

template <typename K, typename V, class L>
mx::tasking::TaskResult InsertSeparatorTask<K, V, L>::execute(const std::uint16_t worker_id)
{
    auto *annotated_node = this->annotation().resource().template get<Node<K, V>>();

    // Is the node related to the key?
    if (annotated_node->high_key() <= this->_key)
    {
        this->annotate(annotated_node->right_sibling(), Node<K, V>::PrefetchHint::for_insert());
        return mx::tasking::TaskResult::make_succeed(this);
    }

    // At this point, we are accessing the related leaf and we are in writer mode.
    if (!annotated_node->full())
    {
        const auto index = annotated_node->index(this->_key);
        annotated_node->insert(index, this->_separator, this->_key);
        this->_separator.template get<Node<K, V>>()->parent(this->annotation().resource());
        this->_listener.inserted(worker_id, this->_key, 0U);
        return mx::tasking::TaskResult::make_remove();
    }

    auto [right, key] = this->_tree->split(this->annotation().resource(), this->_key, this->_separator);
    if (annotated_node->parent() != nullptr)
    {
        this->_separator = right;
        this->_key = key;
        this->annotate(annotated_node->parent(), Node<K, V>::PrefetchHint::for_insert());
        return mx::tasking::TaskResult::make_succeed(this);
    }

    this->_tree->create_new_root(this->annotation().resource(), right, key);
    this->_listener.inserted(worker_id, this->_key, 0U);
    return mx::tasking::TaskResult::make_remove();
}
} // namespace db::index::blinktree
