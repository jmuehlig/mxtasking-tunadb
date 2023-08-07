#include "plan_view.h"
#include <db/exception/plan_exception.h>
#include <ecpp/static_vector.hpp>
#include <queue>
#include <unordered_set>

using namespace db::plan::optimizer;

PlanView::PlanView(const topology::Database &database, node_t root, PlanView::child_map_t &&child_map)
    : _database(database)
{
    this->_node_parent.insert(std::make_pair(root, nullptr));
    this->insert(root, child_map);
}

void PlanView::replace(const PlanView::node_t original_node, const PlanView::node_t new_node)
{
    /// Update the connection to the parent (both connections: original_node -> parent and original_node <- parent).
    /// After that, original_node will be replaced by new_node.
    if (this->_node_parent.find(original_node) != this->_node_parent.end())
    {
        /// Parent of the node thats going to be replaced.
        auto *const parent = this->_node_parent.at(original_node);

        /// Remove the connection "node to be replaced" -> parent.
        this->_node_parent.erase(original_node);

        /// Make a new connection: new_node -> parent.
        this->_node_parent.insert(std::make_pair(new_node, parent));

        /// Remove the connection "node to be replaced" <- parent and
        /// emplace the new node instead.
        if (this->_node_children.find(parent) != this->_node_children.end())
        {
            auto &children = this->_node_children.at(parent);
            if (std::get<0>(children) == original_node)
            {
                std::get<0>(children) = new_node;
            }
            else if (std::get<1>(children) == original_node)
            {
                std::get<1>(children) = new_node;
            }
        }
    }

    /// Get all pairs where original_node is a parent and
    /// collect the children. Create a new list [(original_children, new_node)].
    auto new_parents = ecpp::static_vector<std::pair<node_t, node_t>, 4U>{};
    for (auto &child : this->_node_parent)
    {
        if (std::get<1>(child) == original_node)
        {
            new_parents.emplace_back(std::make_pair(std::get<0>(child), new_node));
        }
    }

    /// Remove all entries where original_node is the parent.
    for (auto it = this->_node_parent.begin(); it != this->_node_parent.end();)
    {
        if (std::get<1>(*it) == original_node)
        {
            it = this->_node_parent.erase(it);
        }
        else
        {
            ++it;
        }
    }

    /// Persist the entries in the node_parent map.
    for (auto &parent : new_parents)
    {
        this->_node_parent.insert(std::move(parent));
    }

    /// Remove the entry where original_node was the parent and insert
    /// a new entry where new_node is the parent of the original_nodes children.
    if (this->_node_children.find(original_node) != this->_node_children.end())
    {
        auto &children = this->_node_children.at(original_node);
        this->_node_children.insert(std::make_pair(new_node, children));
        this->_node_children.erase(original_node);
    }
}

void PlanView::replace(const node_t original_node, PlanView &&plan)
{
    auto *parent = this->parent(original_node);

    /// Remove all children of the replaced node from the plan.
    if (this->has_children(original_node)) [[likely]]
    {
        const auto &children = this->children(original_node);
        if (std::get<0>(children) != nullptr) [[likely]]
        {
            this->remove(std::get<0>(children));
        }
        if (std::get<1>(children) != nullptr) [[likely]]
        {
            this->remove(std::get<1>(children));
        }
    }

    /// Insert schemes and linkage from the new plan.
    auto *root = plan.root();
    this->_node_parent.merge(std::move(plan._node_parent));
    this->_node_children.merge(std::move(plan._node_children));

    /// Update the parent-child connection for the new parent.
    this->_node_parent.insert_or_assign(root, parent);

    /// Remove the original node.
    this->_node_parent.erase(original_node);
    this->_node_children.erase(original_node);

    if (parent != nullptr) [[likely]]
    {
        auto &parent_children = this->_node_children.at(parent);
        if (std::get<0>(parent_children) == original_node) [[likely]]
        {
            std::get<0>(parent_children) = root;
        }
        else
        {
            std::get<1>(parent_children) = root;
        }
    }
}

bool PlanView::move_between(const PlanView::node_t node, const PlanView::node_t child_node,
                            const PlanView::node_t node_to_move)
{
    if (node_to_move->is_unary())
    {
        this->erase(node_to_move);
        this->insert_between(node, child_node, node_to_move);
        return true;
    }

    return false;
}

bool PlanView::swap_children(const node_t node)
{
    auto node_iterator = this->_node_children.find(node);
    if (node_iterator != this->_node_children.end())
    {
        auto &children = node_iterator->second;
        if (std::get<0>(children) != nullptr && std::get<1>(children) != nullptr)
        {
            std::swap(std::get<0>(children), std::get<1>(children));

            return true;
        }
    }

    return false;
}

PlanView::node_t PlanView::root() const
{
    for (auto [node, parent] : this->_node_parent)
    {
        if (parent == nullptr)
        {
            return node;
        }
    }

    return nullptr;
}

void PlanView::insert_between(const PlanView::node_t first, const PlanView::node_t second,
                              const PlanView::node_t node_to_insert)
{
    if (node_to_insert->is_unary())
    {
        // Was:     first -> second
        // Will be: first -> node_to_insert -> second

        // Set node_to_insert as child of first
        auto &children = this->_node_children[first];
        if (std::get<0>(children) == second)
        {
            std::get<0>(children) = node_to_insert;
        }
        else if (std::get<1>(children) == second)
        {
            std::get<1>(children) = node_to_insert;
        }

        // Set first as parent of node_to_insert
        this->_node_parent[node_to_insert] = first;

        // Set second as child of node_to_insert
        this->_node_children[node_to_insert] = {second, nullptr};
        this->_node_parent[second] = node_to_insert;
    }
}

void PlanView::insert_after(db::plan::optimizer::PlanView::node_t node, db::plan::optimizer::PlanView &&plan)
{
    auto *plan_root = plan.root();
    this->_node_children.insert(plan._node_children.begin(), plan._node_children.end());
    this->_node_parent.insert(plan._node_parent.begin(), plan._node_parent.end());

    auto iterator = this->_node_children.find(node);
    if (iterator == this->_node_children.end())
    {
        this->_node_children.insert(std::make_pair(node, std::make_pair(plan_root, nullptr)));
    }
    else if (std::get<0>(iterator->second) == nullptr)
    {
        std::get<0>(iterator->second) = plan_root;
    }
    else
    {
        std::get<1>(iterator->second) = plan_root;
    }

    if (auto parent_iterator = this->_node_parent.find(plan_root); parent_iterator != this->_node_parent.end())
    {
        parent_iterator->second = node;
    }
    else
    {
        this->_node_parent.insert(std::make_pair(plan_root, node));
    }
}

void PlanView::erase(const PlanView::node_t node)
{
    /// Binary nodes can not be erased since there will be at
    /// least one child without parent.
    if (node->is_binary())
    {
        throw exception::PlanningException{"Can not erase a binary node."};
    }

    auto *parent = PlanView::node_t{nullptr};
    auto *child = PlanView::node_t{nullptr};

    /// Pick the child that becomes a new parent.
    /// Remove the node to erase from the children list.
    auto children_iterator = this->_node_children.find(node);
    if (children_iterator != this->_node_children.end())
    {
        const auto &children = children_iterator->second;
        if (std::get<0>(children) != nullptr)
        {
            child = std::get<0>(children);
        }

        this->_node_children.erase(node);
    }

    /// Pick the new parent for the possible child.
    /// Remove the node to erase from the parent list.
    auto parent_iterator = this->_node_parent.find(node);
    if (parent_iterator != this->_node_parent.end())
    {
        parent = parent_iterator->second;
        this->_node_parent.erase(node);
    }

    /// Set up new connection between parent and child of the node to erase.
    if (parent != nullptr && child != nullptr)
    {
        /// Parent gets a new child.
        auto &parent_children = this->_node_children.at(parent);
        if (parent->is_binary() && std::get<1>(parent_children) == node)
        {
            std::get<1>(parent_children) = child;
        }
        else
        {
            std::get<0>(parent_children) = child;
        }

        /// Child gets a new parent.
        this->_node_parent[child] = parent;
    }
    else if (child != nullptr)
    {
        /// We have a child, but no parent (we removed the root).
        /// Since root identification comes from this->_node_parent,
        /// we will not erase the entry, but set to null.
        this->_node_parent.at(child) = nullptr;
    }
    else if (parent != nullptr)
    {
        this->_node_children.erase(parent);
    }
}

void PlanView::remove(const node_t node)
{
    if (this->_node_children.contains(node))
    {
        const auto &children = this->_node_children.at(node);
        if (std::get<0>(children) != nullptr)
        {
            this->remove(std::get<0>(children));
        }
        if (std::get<1>(children) != nullptr)
        {
            this->remove(std::get<1>(children));
        }
        this->_node_children.erase(node);
    }
    this->_node_parent.erase(node);
}

void PlanView::insert(node_t parent, const child_map_t &sub_tree_children)
{
    auto children = sub_tree_children.find(parent);
    if (children != sub_tree_children.end())
    {
        auto *left_child = std::get<0>(children->second);
        auto *right_child = std::get<1>(children->second);

        if (left_child != nullptr)
        {
            this->_node_parent.insert(std::make_pair(left_child, parent));
            this->_node_children.insert(std::make_pair(parent, std::make_pair(left_child, right_child)));

            this->insert(left_child, sub_tree_children);
        }

        if (right_child != nullptr)
        {
            this->_node_parent.insert(std::make_pair(right_child, parent));

            this->insert(right_child, sub_tree_children);
        }
    }
}

void PlanView::extract_nodes(logical::NodeChildIterator &child_iterator, const PlanView::node_t parent,
                             const PlanView::node_t node)
{
    this->_node_parent.insert(std::make_pair(node, parent));

    if (node->is_unary())
    {
        auto *child = child_iterator.child(reinterpret_cast<logical::UnaryNode *>(node));
        this->_node_children.insert(std::make_pair(node, std::make_pair(child, node_t(nullptr))));
        extract_nodes(child_iterator, node, child);
    }
    else if (node->is_binary())
    {
        const auto children = child_iterator.children(reinterpret_cast<logical::BinaryNode *>(node));
        this->_node_children.insert(std::make_pair(node, children));

        extract_nodes(child_iterator, node, std::get<0>(children));
        extract_nodes(child_iterator, node, std::get<1>(children));
    }
}

PlanView PlanView::subplan(db::plan::optimizer::PlanView::node_t node) const
{
    auto child_iterator = PlanViewNodeChildIterator{*this};
    return PlanView{this->_database, child_iterator, node};
}

PlanView PlanView::subplan_until_join() const
{
    auto sub_plan = PlanView{this->_database};

    auto *last_node = node_t{nullptr};
    auto *current_node = this->root();
    while (current_node != nullptr && current_node->is_binary() == false)
    {
        sub_plan._node_parent.insert(std::make_pair(current_node, last_node));
        if (last_node != nullptr)
        {
            sub_plan._node_children.insert(std::make_pair(last_node, std::make_pair(current_node, nullptr)));
        }

        last_node = current_node;
        if (auto iterator = this->_node_children.find(current_node); iterator != this->_node_children.end())
        {
            current_node = std::get<0>(iterator->second);
        }
        else
        {
            current_node = nullptr;
        }
    }

    return sub_plan;
}