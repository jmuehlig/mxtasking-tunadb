#pragma once

#include <cstdint>
#include <db/plan/logical/node/node_interface.h>
#include <db/plan/logical/node_child_iterator.h>
#include <db/plan/logical/relation.h>
#include <db/topology/database.h>
#include <functional>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace db::plan::optimizer {
class PlanView
{
public:
    using node_t = logical::NodeInterface *;
    using child_map_t = std::unordered_map<node_t, std::pair<node_t, node_t>>;

    PlanView(const topology::Database &database, const std::unique_ptr<logical::NodeInterface> &root)
        : _database(database)
    {
        auto child_iterator = logical::TreeNodeChildIterator{};
        extract_nodes(child_iterator, nullptr, root.get());
    }

    PlanView(const topology::Database &database, logical::NodeChildIterator &child_iterator,
             logical::NodeInterface *root)
        : _database(database)
    {
        extract_nodes(child_iterator, nullptr, root);
    }

    PlanView(const topology::Database &database, node_t root, PlanView::child_map_t &&child_map);

    PlanView(const topology::Database &database, node_t root) : _database(database)
    {
        _node_parent.insert(std::make_pair(root, nullptr));
    }

    PlanView(PlanView &&other) noexcept
        : _database(other._database), _node_children(std::move(other._node_children)),
          _node_parent(std::move(other._node_parent))
    {
    }

    PlanView(const PlanView &) = default;

    ~PlanView() = default;

    PlanView &operator=(PlanView &&other) noexcept
    {
        this->_node_children = std::move(other._node_children);
        this->_node_parent = std::move(other._node_parent);
        return *this;
    }

    /**
     * Replaces the given original_node with a new node.
     * The new_node will inherit the children of the original_node;
     * the parent linkage will be updated from original_node to new_node.
     *
     * @param original_node Node to be replaced.
     * @param new_node Node to replace the original_node.
     */
    void replace(node_t original_node, node_t new_node);

    /**
     * Replaces the given original_node with full sub plan.
     * All children of the original_node will be removed.
     *
     * @param original_node Node to be replaced.
     * @param plan Plan that replaces the node.
     */
    void replace(node_t original_node, PlanView &&plan);

    /**
     * Removes the given node_to_move from its original position and moves
     * it between node and child_node:
     * Before: node -> child_node
     * After: node -> node_to_move -> child_node
     *
     * @param node New parent of the node to move.
     * @param child_node Old child of node (and new child of node_to_move).
     * @param node_to_move Node to move.
     * @return True, if the move was successfull.
     */
    [[nodiscard]] bool move_between(node_t node, node_t child_node, node_t node_to_move);

    /**
     * Inserts a new node between first and second.
     * Before: first -> second
     * After: first -> node_to_insert -> second
     *
     * @param first New parent of the node to insert.
     * @param second Child of first, new child of the node to insert.
     * @param node_to_insert Node that should be inserted.
     */
    void insert_between(node_t first, node_t second, node_t node_to_insert);

    /**
     * Inserts the (sub-) plan as a child of the given node.
     *
     * @param node Node after the plan should be inserted.
     * @param plan Plan to insert.
     */
    void insert_after(node_t node, PlanView &&plan);

    /**
     * Erases the given node from the plan.
     * Linkage to children and parent will be removed.
     *
     * @param node Node to remove.
     */
    void erase(node_t node);

    /**
     * Swaps the children of the given node so that
     * child[0] will be child[1] and vice versa.
     *
     * @param node Node to replace children.
     * @return True, when the node has two children that were swapped.
     */
    [[nodiscard]] bool swap_children(node_t node);

    /**
     * @param node
     * @return Pair of both child pointers of the given node.
     */
    [[nodiscard]] const std::pair<node_t, node_t> &children(const node_t node) const { return _node_children.at(node); }

    /**
     * @param node
     * @return True, if the given node has children.
     */
    [[nodiscard]] bool has_children(node_t node) const { return _node_children.contains(node); }

    /**
     * @return List of pairs (node, parent).
     */
    [[nodiscard]] const std::unordered_map<node_t, node_t> &nodes_and_parent() const { return _node_parent; }

    /**
     * @return A list of all nodes. The list will be copied, use it carefully.
     */
    [[nodiscard]] std::vector<node_t> extract_nodes() const
    {
        auto nodes = std::vector<node_t>{};
        std::transform(_node_parent.begin(), _node_parent.end(), std::back_inserter(nodes),
                       [](const auto &iterator) { return iterator.first; });

        return nodes;
    }

    /**
     *
     * @param node
     * @return The parent node of the given node or null.
     */
    [[nodiscard]] node_t parent(const node_t node) const
    {
        auto iterator = _node_parent.find(node);
        if (iterator != _node_parent.end()) [[likely]]
        {
            return iterator->second;
        }

        return nullptr;
    }

    /**
     * @return The root node of this plan.
     */
    [[nodiscard]] node_t root() const;

    /**
     * Creates a plan view containing a sub plan
     * starting with the given node as a root.
     *
     * @param node Root of the sub plan.
     * @return Sub plan.
     */
    [[nodiscard]] PlanView subplan(node_t node) const;

    /**
     * @return A sub plan containing all nodes until the first join (the joins is not included).
     */
    [[nodiscard]] PlanView subplan_until_join() const;

    [[nodiscard]] const topology::Database &database() const noexcept { return _database; }

    template <typename T, typename... Args> [[nodiscard]] T *make_node(Args &&...arguments)
    {
        return new T(std::forward<Args>(arguments)...);
    }

private:
    const topology::Database &_database;

    /// Representing node -> children[2] linkage. Every node may have up to two children.
    child_map_t _node_children;

    /// Representing node -> parent linkage; every node has one parent.
    std::unordered_map<node_t, node_t> _node_parent;

    explicit PlanView(const topology::Database &database) : _database(database) {}

    void extract_nodes(logical::NodeChildIterator &child_iterator, node_t parent, node_t node);

    /**
     * Removes the given node and all of its children
     * recursively from the plan.
     * In contrast to "erase", the relations will not be updated.
     *
     * @param node Node to be removed.
     */
    void remove(node_t node);

    /**
     * Inserts a node as a child of the given parent node.
     * Insertions is recursively: The children map is scanned
     * and children of the root will be inserted (and their children,...).
     *
     * @param node Node to insert.
     * @param sub_tree_children Children map of the node to insert.
     */
    void insert(node_t parent, const child_map_t &sub_tree_children);
};

class PlanViewNodeChildIterator final : public logical::NodeChildIterator
{
public:
    constexpr explicit PlanViewNodeChildIterator(const optimizer::PlanView &plan_view) noexcept : _plan_view(plan_view)
    {
    }
    ~PlanViewNodeChildIterator() noexcept override = default;

    logical::NodeInterface *child(const db::plan::logical::UnaryNode *node) const override
    {
        return std::get<0>(_plan_view.children((PlanView::node_t)node));
    }

    std::pair<logical::NodeInterface *, logical::NodeInterface *> children(
        const logical::BinaryNode *node) const override
    {
        return _plan_view.children((PlanView::node_t)node);
    }

private:
    const optimizer::PlanView &_plan_view;
};
} // namespace db::plan::optimizer