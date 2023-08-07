#pragma once

#include <db/expression/term.h>
#include <db/plan/logical/node_child_iterator.h>
#include <db/plan/logical/relation.h>
#include <db/topology/database.h>
#include <fmt/core.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <vector>

namespace db::plan::logical {
class NodeInterface
{
public:
    enum QueryType
    {
        EXPLAIN,
        SAMPLE,
        COMMAND,
        CONFIGURATION,
        SELECT,
        CREATE,
        INSERT,
        STOP
    };

    explicit NodeInterface(std::string &&name) noexcept : _name(std::move(name)) {}
    virtual ~NodeInterface() = default;

    [[nodiscard]] virtual QueryType query_type() const noexcept = 0;
    [[nodiscard]] virtual bool is_nullary() const { return false; }
    [[nodiscard]] virtual bool is_unary() const { return false; }
    [[nodiscard]] virtual bool is_binary() const { return false; }

    [[nodiscard]] const std::string &name() const { return _name; }

    [[nodiscard]] const Relation &relation() const { return _relation; }
    void relation(Relation &&relation) noexcept { _relation = relation; }

    [[nodiscard]] virtual Relation &emit_relation(const topology::Database &database,
                                                  const NodeChildIterator &child_iterator,
                                                  bool include_cardinality) = 0;

    [[nodiscard]] virtual nlohmann::json to_json(const topology::Database & /*database*/) const
    {
        auto json = nlohmann::json{};
        json["name"] = _name;
        json["output"] = _relation.schema().to_string();
        json["cardinality"] = _relation.cardinality();

        return json;
    }

protected:
    std::string _name;
    Relation _relation;
};

class NullaryNode : public NodeInterface
{
public:
    explicit NullaryNode(std::string &&name) noexcept : NodeInterface(std::move(name)) {}
    ~NullaryNode() override = default;

    [[nodiscard]] bool is_nullary() const override { return true; }

    [[nodiscard]] virtual std::uint64_t cardinality(const topology::Database &database) const = 0;
    [[nodiscard]] virtual topology::LogicalSchema schema(const topology::Database &database) const = 0;

    [[nodiscard]] Relation &emit_relation(const topology::Database &database,
                                          const NodeChildIterator & /*child_iterator*/,
                                          const bool include_cardinality) override
    {
        if (include_cardinality)
        {
            return _relation = Relation{this->schema(database), this->cardinality(database)};
        }

        return _relation = Relation{this->schema(database)};
    }
};

class UnaryNode : public NodeInterface
{
public:
    explicit UnaryNode(std::string &&name) noexcept : NodeInterface(std::move(name)) {}
    ~UnaryNode() override = default;

    [[nodiscard]] bool is_unary() const override { return true; }

    void child(std::unique_ptr<NodeInterface> &&child) { _child = std::move(child); }
    [[nodiscard]] const std::unique_ptr<NodeInterface> &child() const { return _child; }

    [[nodiscard]] std::unique_ptr<NodeInterface> &child() { return _child; }

    [[nodiscard]] virtual std::uint64_t cardinality(const topology::Database &database,
                                                    const NodeChildIterator &child_iterator) const = 0;
    [[nodiscard]] virtual topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const = 0;

    [[nodiscard]] Relation &emit_relation(const topology::Database &database, const NodeChildIterator &child_iterator,
                                          const bool include_cardinality) override
    {
        std::ignore = child_iterator.child(this)->emit_relation(database, child_iterator, include_cardinality);

        if (include_cardinality)
        {
            return _relation = Relation{this->schema(child_iterator), this->cardinality(database, child_iterator)};
        }

        return _relation = Relation{this->schema(child_iterator)};
    }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = NodeInterface::to_json(database);
        json["childs"][0] = _child->to_json(database);
        return json;
    }

private:
    std::unique_ptr<NodeInterface> _child{nullptr};
};

class BinaryNode : public NodeInterface
{
public:
    BinaryNode(std::string &&name, std::unique_ptr<NodeInterface> &&left_child,
               std::unique_ptr<NodeInterface> &&right_child) noexcept
        : NodeInterface(std::move(name)), _left_child(std::move(left_child)), _right_child(std::move(right_child))
    {
    }
    ~BinaryNode() override = default;

    [[nodiscard]] bool is_binary() const override { return true; }

    void left_child(std::unique_ptr<NodeInterface> &&child) { _left_child = std::move(child); }
    [[nodiscard]] const std::unique_ptr<NodeInterface> &left_child() const { return _left_child; }

    [[nodiscard]] std::unique_ptr<NodeInterface> &left_child() { return _left_child; }

    void right_child(std::unique_ptr<NodeInterface> &&child) { _right_child = std::move(child); }
    [[nodiscard]] const std::unique_ptr<NodeInterface> &right_child() const { return _right_child; }

    [[nodiscard]] std::unique_ptr<NodeInterface> &right_child() { return _right_child; }

    [[nodiscard]] virtual std::uint64_t cardinality(const topology::Database &database,
                                                    const NodeChildIterator &child_iterator) const = 0;
    [[nodiscard]] virtual topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const = 0;

    [[nodiscard]] Relation &emit_relation(const topology::Database &database, const NodeChildIterator &child_iterator,
                                          const bool include_cardinality) override
    {
        auto [left_child, right_child] = child_iterator.children(this);
        std::ignore = left_child->emit_relation(database, child_iterator, include_cardinality);
        std::ignore = right_child->emit_relation(database, child_iterator, include_cardinality);

        if (include_cardinality)
        {
            return _relation = Relation{this->schema(child_iterator), this->cardinality(database, child_iterator)};
        }

        return _relation = Relation{this->schema(child_iterator)};
    }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = NodeInterface::to_json(database);
        json["childs"][0] = _left_child->to_json(database);
        json["childs"][1] = _right_child->to_json(database);
        return json;
    }

private:
    std::unique_ptr<NodeInterface> _left_child{nullptr};
    std::unique_ptr<NodeInterface> _right_child{nullptr};
};

class NotSchematizedNode : public NullaryNode
{
public:
    explicit NotSchematizedNode(std::string &&name) : NullaryNode(std::move(name)) {}
    ~NotSchematizedNode() override = default;

    [[nodiscard]] std::uint64_t cardinality(const topology::Database & /*database*/) const override { return 0U; }
    [[nodiscard]] topology::LogicalSchema schema(const topology::Database & /*database*/) const override
    {
        return topology::LogicalSchema{};
    }
};
} // namespace db::plan::logical