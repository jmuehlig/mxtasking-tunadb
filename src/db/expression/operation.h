#pragma once

#include "annotation.h"
#include "attribute.h"
#include "term.h"
#include <cstdint>
#include <db/parser/node_interface.h>
#include <db/topology/logical_schema.h>
#include <db/topology/physical_schema.h>
#include <db/udf/descriptor.h>
#include <fmt/core.h>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

namespace db::expression {
class Operation
{
public:
    enum Id : std::uint8_t
    {
        Identity = 0U,
        IdentityList = 1U,
        Cast = 2U,
        Count = 4U,
        Average = 5U,
        Sum = 6U,
        Min = 7U,
        Max = 8U,
        Add = 16U,
        Sub = 17U,
        Multiply = 18U,
        Divide = 19U,
        And = 32U,
        Or = 33U,
        Equals = 60U,
        NotEquals = 61U,
        Lesser = 62U,
        LesserEquals = 63U,
        Greater = 64U,
        GreaterEquals = 65U,
        Between = 66U,
        Like = 70U,
        StartsWith = 71U,
        EndsWith = 72U,
        Contains = 73U,
        In = 80U,
        BetweenOperands = 90U,
        Case = 100U,
        WhenThen = 101U,
        Else = 102U,

        IsTrue = 110U,
        IsFalue = 111U,

        Exists = 120U,

        UserDefinedFunction = 130U,
    };

    virtual ~Operation() = default;

    [[nodiscard]] bool is_nullary() const noexcept { return _id == Identity; }
    [[nodiscard]] bool is_unary() const noexcept
    {
        return (_id >= static_cast<std::uint8_t>(Id::Cast) && _id <= static_cast<std::uint8_t>(Id::Max)) ||
               _id == Id::Else || _id == Id::IsTrue || _id == Id::IsTrue;
    }
    [[nodiscard]] bool is_aggregation() const noexcept
    {
        return _id >= static_cast<std::uint8_t>(Id::Count) && _id <= static_cast<std::uint8_t>(Id::Max);
    }
    [[nodiscard]] bool is_logical_connective() const noexcept { return _id == Id::And || _id == Id::Or; }
    [[nodiscard]] bool is_comparison() const noexcept
    {
        return _id >= static_cast<std::uint8_t>(Id::Equals) && _id <= static_cast<std::uint8_t>(Id::In);
    }
    [[nodiscard]] bool is_arithmetic() const noexcept
    {
        return (_id >= static_cast<std::uint8_t>(Id::Add) && _id <= static_cast<std::uint8_t>(Id::Divide)) ||
               _id == Id::Cast || _id == Id::Case;
    }
    [[nodiscard]] bool is_cast() const noexcept { return _id == Id::Cast; }
    [[nodiscard]] bool is_case() const noexcept { return _id == Id::Case; }
    [[nodiscard]] bool is_binary() const noexcept
    {
        return is_logical_connective() || is_comparison() || (is_arithmetic() && _id != Id::Cast && _id != Id::Case) ||
               _id == Id::WhenThen || _id == Id::BetweenOperands;
    }
    [[nodiscard]] bool is_nullary_list() const noexcept { return _id == Id::IdentityList; }
    [[nodiscard]] bool is_list() const noexcept { return _id == Id::Case; }

    [[nodiscard]] bool is_user_defined_function() const noexcept { return _id == Id::UserDefinedFunction; }

    [[nodiscard]] Id id() const noexcept { return _id; }
    void id(const Id operation_id) noexcept { _id = operation_id; }
    [[nodiscard]] const std::optional<Term> &result() const noexcept { return _result; }
    [[nodiscard]] std::optional<Term> &result() noexcept { return _result; }

    void alias(std::string &&alias) noexcept
    {
        if (_result.has_value())
        {
            _result->alias(std::move(alias));
        }
    }

    [[nodiscard]] const Annotation &annotation() const noexcept { return _annotation; }
    [[nodiscard]] Annotation &annotation() noexcept { return _annotation; }

    [[nodiscard]] virtual std::unique_ptr<Operation> copy() const = 0;
    [[nodiscard]] virtual std::string to_string(std::uint16_t level = 0U) const noexcept = 0;
    [[nodiscard]] virtual type::Type type(const topology::LogicalSchema &schema) const = 0;
    [[nodiscard]] type::Type type(const topology::PhysicalSchema &schema) const
    {
        return type(reinterpret_cast<const topology::LogicalSchema &>(schema));
    }

    [[nodiscard]] virtual bool is_equals(const std::unique_ptr<Operation> &other) const noexcept = 0;

protected:
    explicit Operation(const Id operation_id) : _id(operation_id), _result(std::nullopt) {}
    Operation(const Id operation_id, Term &&result) : _id(operation_id), _result(std::move(result)) {}
    Operation(const Id operation_id, std::optional<Term> &&result) : _id(operation_id), _result(std::move(result)) {}

    Id _id;
    std::optional<Term> _result;
    Annotation _annotation;
};

class NullaryOperation : public Operation
{
public:
    explicit NullaryOperation(Term &&term) : Operation(Operation::Identity, term) {}
    NullaryOperation(const NullaryOperation &) = default;
    NullaryOperation(NullaryOperation &&) = default;

    ~NullaryOperation() override = default;

    void term(Term &&term) noexcept { _result.emplace(std::move(term)); }

    [[nodiscard]] const Term &term() const { return _result.value(); }
    [[nodiscard]] Term &term() { return _result.value(); }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        return std::make_unique<NullaryOperation>(Term{_result.value()});
    }

    [[nodiscard]] std::string to_string(std::uint16_t /*level*/ = 0U) const noexcept override
    {
        return _result->to_string();
    }

    [[nodiscard]] type::Type type(const topology::LogicalSchema &schema) const override
    {
        if (_result->is_attribute())
        {
            const auto index = schema.index(_result.value());
            if (index.has_value())
            {
                return schema.type(index.value());
            }
        }

        if (_result->is_value())
        {
            return _result->get<data::Value>().type();
        }

        return type::Type{};
    }

    [[nodiscard]] bool is_equals(const std::unique_ptr<Operation> &other) const noexcept override
    {
        return other->id() == _id && other->result() == _result;
    }
};

class UnaryOperation : public Operation
{
public:
    UnaryOperation(const Id operation_id, Term &&result, std::unique_ptr<Operation> &&child)
        : Operation(operation_id, std::move(result)), _child(std::move(child))
    {
    }
    UnaryOperation(const Id operation_id, std::optional<Term> &&result, std::unique_ptr<Operation> &&child)
        : Operation(operation_id, std::move(result)), _child(std::move(child))
    {
    }
    UnaryOperation(const Id operation_id, std::unique_ptr<Operation> &&child)
        : Operation(operation_id), _child(std::move(child))
    {
        _result = Term::make_attribute(this->UnaryOperation::to_string(), true);
    }

    ~UnaryOperation() override = default;

    [[nodiscard]] const std::unique_ptr<Operation> &child() const { return _child; }
    [[nodiscard]] std::unique_ptr<Operation> &child() { return _child; }
    void child(std::unique_ptr<Operation> &&child) { _child = std::move(child); }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        return std::make_unique<UnaryOperation>(
            _id, _result.has_value() ? std::make_optional(Term{_result.value()}) : std::nullopt, _child->copy());
    }

    [[nodiscard]] std::string to_string(const std::uint16_t /*level*/ = 0U) const noexcept override
    {
        switch (_id)
        {
        case Id::Count:
            return fmt::format("COUNT({})", child()->to_string(0U));
        case Id::Sum:
            return fmt::format("SUM({})", child()->to_string(0U));
        case Id::Average:
            return fmt::format("AVG({})", child()->to_string(0U));
        case Id::Min:
            return fmt::format("MIN({})", child()->to_string(0U));
        case Id::Max:
            return fmt::format("MAX({})", child()->to_string(0U));
        case Id::Else:
            return fmt::format("ELSE {}", child()->to_string(0U));
        case Id::IsTrue:
            return child()->to_string(0U);
        case Id::IsFalue:
            return child()->to_string(0U);
        default:
            return fmt::format("UNKNOWN({})", child()->to_string(0U));
        }
    }

    [[nodiscard]] type::Type type(const topology::LogicalSchema &schema) const override
    {
        if (_id == Id::Count)
        {
            return type::Type::make_bigint();
        }

        if (_id == Id::Min || _id == Id::Max)
        {
            return _child->type(schema);
        }

        if (_id == Id::Sum)
        {
            const auto child_type = _child->type(schema);
            return child_type + child_type;
        }

        if (_id == Id::Average)
        {
            const auto child_type = _child->type(schema);
            return child_type / type::Type::make_bigint();
        }

        if (_id == Id::Else)
        {
            return _child->type(schema);
        }

        if (_id == Id::IsTrue || _id == Id::IsFalue)
        {
            return type::Type::make_bool();
        }

        return type::Type{};
    }

    [[nodiscard]] bool is_equals(const std::unique_ptr<Operation> &other) const noexcept override
    {
        return other->id() == _id && _child->is_equals(reinterpret_cast<UnaryOperation *>(other.get())->child());
    }

private:
    std::unique_ptr<Operation> _child;
};

class CastOperation : public UnaryOperation
{
public:
    CastOperation(std::unique_ptr<Operation> &&child, type::Type type) noexcept
        : UnaryOperation(Id::Cast, std::move(child)), _type(type)
    {
        auto result = this->child()->result();
        if (result.has_value())
        {
            if (result->is_value())
            {
                _result = Term{data::Value{_type, result->get<data::Value>().as(_type).value()}, true};
            }
            else if (result->is_attribute())
            {
                _result = Term::make_attribute(this->CastOperation::to_string(), true);
            }
            else
            {
                _result = result;
            }
        }
    }

    ~CastOperation() override = default;

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        return std::make_unique<CastOperation>(child()->copy(), _type);
    }

    [[nodiscard]] std::string to_string(const std::uint16_t /*level*/ = 0U) const noexcept override
    {
        return fmt::format("CAST({} AS {})", child()->to_string(0U), _type.to_string());
    }

    [[nodiscard]] type::Type type(const topology::LogicalSchema & /*schema*/) const override { return _type; }
    [[nodiscard]] type::Type type() const noexcept { return _type; }

    [[nodiscard]] bool is_equals(const std::unique_ptr<Operation> &other) const noexcept override
    {
        return UnaryOperation::is_equals(other) && _type == reinterpret_cast<CastOperation *>(other.get())->type();
    }

private:
    type::Type _type;
};

class BinaryOperation : public Operation
{
public:
    BinaryOperation(const Id operation_id, Term &&result, std::unique_ptr<Operation> &&left_child,
                    std::unique_ptr<Operation> &&right_child)
        : Operation(operation_id, std::move(result)), _left_child(std::move(left_child)),
          _right_child(std::move(right_child))
    {
    }
    BinaryOperation(const Id operation_id, std::optional<Term> &&result, std::unique_ptr<Operation> &&left_child,
                    std::unique_ptr<Operation> &&right_child)
        : Operation(operation_id, std::move(result)), _left_child(std::move(left_child)),
          _right_child(std::move(right_child))
    {
    }
    BinaryOperation(const Id operation_id, std::unique_ptr<Operation> &&left_child,
                    std::unique_ptr<Operation> &&right_child)
        : Operation(operation_id), _left_child(std::move(left_child)), _right_child(std::move(right_child))
    {
        _result = Term::make_attribute(this->BinaryOperation::to_string(), true);
    }

    ~BinaryOperation() override = default;

    [[nodiscard]] const std::unique_ptr<Operation> &left_child() const { return _left_child; }
    [[nodiscard]] const std::unique_ptr<Operation> &right_child() const { return _right_child; }
    [[nodiscard]] std::unique_ptr<Operation> &left_child() { return _left_child; }
    [[nodiscard]] std::unique_ptr<Operation> &right_child() { return _right_child; }

    void left_child(std::unique_ptr<Operation> &&child) { _left_child = std::move(child); }

    void right_child(std::unique_ptr<Operation> &&child) { _right_child = std::move(child); }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        auto operation = std::make_unique<BinaryOperation>(
            _id, _result.has_value() ? std::make_optional(Term{_result.value()}) : std::nullopt, _left_child->copy(),
            _right_child->copy());
        operation->annotation() = this->annotation();
        return operation;
    }

    [[nodiscard]] std::string to_string(const std::uint16_t level = 0U) const noexcept override
    {
        auto as_string = std::string{};
        auto left = left_child()->to_string(level + 1U);
        auto right = right_child()->to_string(level + 1U);
        switch (_id)
        {
        case Id::Add:
            as_string = fmt::format("{}+{}", std::move(left), std::move(right));
            break;
        case Id::Sub:
            as_string = fmt::format("{}-{}", std::move(left), std::move(right));
            break;
        case Id::Multiply:
            as_string = fmt::format("{}*{}", std::move(left), std::move(right));
            break;
        case Id::Divide:
            as_string = fmt::format("{}/{}", std::move(left), std::move(right));
            break;
        case Id::And:
            as_string = fmt::format("{} AND {}", std::move(left), std::move(right));
            break;
        case Id::Or:
            as_string = fmt::format("{} OR {}", std::move(left), std::move(right));
            break;
        case Id::Equals:
            as_string = fmt::format("{} = {}", std::move(left), std::move(right));
            break;
        case Id::LesserEquals:
            as_string = fmt::format("{} <= {}", std::move(left), std::move(right));
            break;
        case Id::Lesser:
            as_string = fmt::format("{} < {}", std::move(left), std::move(right));
            break;
        case Id::GreaterEquals:
            as_string = fmt::format("{} >= {}", std::move(left), std::move(right));
            break;
        case Id::Greater:
            as_string = fmt::format("{} > {}", std::move(left), std::move(right));
            break;
        case Id::NotEquals:
            as_string = fmt::format("{} <> {}", std::move(left), std::move(right));
            break;
        case Id::Between:
            as_string = fmt::format("{} BETWEEN {}", std::move(left), std::move(right));
            break;
        case Id::BetweenOperands:
            as_string = fmt::format("({},{})", std::move(left), std::move(right));
            break;
        case Id::Like:
            as_string = fmt::format("{} LIKE {}", std::move(left), std::move(right));
            break;
        case Id::StartsWith:
            as_string = fmt::format("{} STARTS WITH {}", std::move(left), std::move(right));
            break;
        case Id::EndsWith:
            as_string = fmt::format("{} ENDS WITH {}", std::move(left), std::move(right));
            break;
        case Id::Contains:
            as_string = fmt::format("{} CONTAINS {}", std::move(left), std::move(right));
            break;
        case Id::WhenThen:
            as_string = fmt::format("WHEN {} THEN {}", left_child()->to_string(level + 1U),
                                    right_child()->to_string(level + 1U));
            break;
        case Id::In:
            as_string =
                fmt::format("{} IN {}", left_child()->to_string(level + 1U), right_child()->to_string(level + 1U));
            break;
        default:
            as_string = fmt::format("{} ? {}", std::move(left), std::move(right));
        }

        if (level > 0U)
        {
            return fmt::format("({})", std::move(as_string));
        }

        return as_string;
    }

    [[nodiscard]] type::Type type(const topology::LogicalSchema &schema) const override
    {
        if (_id == Id::Add)
        {
            return _left_child->type(schema) + _right_child->type(schema);
        }

        if (_id == Id::Sub)
        {
            return _left_child->type(schema) - _right_child->type(schema);
        }

        if (_id == Id::Multiply)
        {
            return _left_child->type(schema) * _right_child->type(schema);
        }

        if (_id == Id::Divide)
        {
            return _left_child->type(schema) / _right_child->type(schema);
        }

        if (is_comparison() || is_logical_connective())
        {
            return type::Type::make_bool();
        }

        if (_id == Id::WhenThen)
        {
            return _right_child->type(schema);
        }

        return type::Type{};
    }

    void invert()
    {
        switch (_id)
        {
        case Operation::Id::Lesser:
            _id = Operation::Id::Greater;
            break;
        case Operation::Id::LesserEquals:
            _id = Operation::Id::GreaterEquals;
            break;
        case Operation::Id::GreaterEquals:
            _id = Operation::Id::LesserEquals;
            break;
        case Operation::Id::Greater:
            _id = Operation::Id::Lesser;
            break;
        default:
            break;
        }
        std::swap(_left_child, _right_child);
    }

    [[nodiscard]] bool is_equals(const std::unique_ptr<Operation> &other) const noexcept override
    {
        if (_id != other->id())
        {
            return false;
        }

        auto *binary_operation = reinterpret_cast<BinaryOperation *>(other.get());

        return _left_child->is_equals(binary_operation->left_child()) &&
               _right_child->is_equals(binary_operation->right_child());
    }

private:
    std::unique_ptr<Operation> _left_child;
    std::unique_ptr<Operation> _right_child;
};

class NullaryListOperation final : public Operation
{
public:
    NullaryListOperation() noexcept : Operation(Operation::Id::IdentityList) {}

    explicit NullaryListOperation(std::vector<Term> &&terms) noexcept
        : Operation(Operation::Id::IdentityList), _terms(std::move(terms))
    {
    }

    ~NullaryListOperation() override = default;

    [[nodiscard]] const std::vector<Term> &terms() const { return _terms; }
    [[nodiscard]] std::vector<Term> &terms() { return _terms; }

    void terms(std::vector<Term> &&terms) { _terms = std::move(terms); }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        auto terms = std::vector<Term>{};
        terms.reserve(_terms.size());
        for (const auto &item : _terms)
        {
            terms.emplace_back(item);
        }

        return std::make_unique<NullaryListOperation>(std::move(terms));
    }

    [[nodiscard]] std::string to_string(const std::uint16_t /*level*/ = 0U) const noexcept override
    {
        if (_terms.empty())
        {
            return "[]";
        }

        auto terms = std::vector<std::string>{};
        std::transform(_terms.begin(), _terms.end(), std::back_inserter(terms),
                       [](const auto &term) { return term.to_string(); });

        return fmt::format("[{}]", fmt::join(std::move(terms), ","));
    }

    [[nodiscard]] type::Type type(const topology::LogicalSchema & /*schema*/) const override
    {
        if (_terms.empty())
        {
            return type::Type{};
        }

        return _terms.front().get<data::Value>().type();
    }

    [[nodiscard]] bool is_equals(const std::unique_ptr<Operation> &other) const noexcept override
    {
        return _id == other->id() && _terms == reinterpret_cast<NullaryListOperation *>(other.get())->terms();
    }

private:
    std::vector<Term> _terms;
};

class ListOperation : public Operation
{
public:
    explicit ListOperation(const Operation::Id operation_id) : Operation(operation_id)
    {
        _result = Term::make_attribute(this->Operation::to_string(), true);
    }

    ListOperation(const Operation::Id operation_id, std::vector<std::unique_ptr<Operation>> &&children) noexcept
        : Operation(operation_id), _children(std::move(children))
    {
        _result = Term::make_attribute(this->ListOperation::to_string(), true);
    }

    ~ListOperation() override = default;

    [[nodiscard]] std::size_t size() const noexcept { return _children.size(); }
    [[nodiscard]] const std::vector<std::unique_ptr<Operation>> &children() const noexcept { return _children; }
    [[nodiscard]] std::vector<std::unique_ptr<Operation>> &children() noexcept { return _children; }

    void emplace_back(std::unique_ptr<Operation> &&child) { _children.emplace_back(std::move(child)); }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        auto children = std::vector<std::unique_ptr<Operation>>{};
        children.reserve(_children.size());

        for (const auto &child : _children)
        {
            children.emplace_back(child->copy());
        }

        return std::make_unique<ListOperation>(_id, std::move(children));
    }

    [[nodiscard]] std::string to_string(const std::uint16_t /*level */ = 0U) const noexcept override
    {
        if (_id == Operation::Id::Case)
        {
            auto children = std::vector<std::string>{};
            std::transform(_children.begin(), _children.end(), std::back_inserter(children),
                           [](const auto &child) { return child->to_string(); });

            return fmt::format("CASE {} END", fmt::join(std::move(children), " "));
        }

        return "List Operation";
    }

    [[nodiscard]] type::Type type(const topology::LogicalSchema &schema) const override
    {
        if (_children.empty() == false)
        {
            auto type = _children.front()->type(schema);
            for (auto i = 1U; i < _children.size(); ++i)
            {
                auto child_type = _children[i]->type(schema);
                if (child_type != type)
                {
                    return std::min(child_type, type);
                }

                if (type == type::Id::CHAR)
                {
                    if (type.char_description().length() < child_type.char_description().length())
                    {
                        type = child_type;
                    }
                }
            }

            return type;
        }

        return type::Type{};
    }

    [[nodiscard]] bool is_equals(const std::unique_ptr<Operation> &other) const noexcept override
    {
        if (_id != other->id())
        {
            return false;
        }

        auto *list_operation = reinterpret_cast<ListOperation *>(other.get());
        if (_children.size() != list_operation->_children.size())
        {
            return false;
        }

        for (auto child_id = 0U; child_id < _children.size(); ++child_id)
        {
            if (_children[child_id]->is_equals(list_operation->_children[child_id]) == false)
            {
                return false;
            }
        }

        return true;
    }

private:
    std::vector<std::unique_ptr<Operation>> _children;
};

class UserDefinedFunctionOperation : public Operation
{
public:
    UserDefinedFunctionOperation(std::string &&function_name,
                                 std::vector<std::unique_ptr<Operation>> &&children) noexcept
        : Operation(Operation::Id::UserDefinedFunction), _function_name(std::move(function_name)),
          _children(std::move(children))
    {
        _result = Term::make_attribute(this->UserDefinedFunctionOperation::to_string(), true);
    }

    ~UserDefinedFunctionOperation() override = default;

    [[nodiscard]] std::size_t size() const noexcept { return _children.size(); }
    [[nodiscard]] const std::vector<std::unique_ptr<Operation>> &children() const noexcept { return _children; }
    [[nodiscard]] std::vector<std::unique_ptr<Operation>> &children() noexcept { return _children; }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override
    {
        auto children = std::vector<std::unique_ptr<Operation>>{};
        children.reserve(_children.size());

        for (const auto &child : _children)
        {
            children.emplace_back(child->copy());
        }

        return std::make_unique<UserDefinedFunctionOperation>(std::string{_function_name}, std::move(children));
    }

    [[nodiscard]] std::string to_string(std::uint16_t level = 0U) const noexcept override;

    [[nodiscard]] type::Type type(const topology::LogicalSchema &schema) const override;

    [[nodiscard]] bool is_equals(const std::unique_ptr<Operation> &other) const noexcept override
    {
        if (other->id() != Operation::Id::UserDefinedFunction)
        {
            return false;
        }

        auto *udf_operation = reinterpret_cast<UserDefinedFunctionOperation *>(other.get());
        if (_children.size() != udf_operation->_children.size())
        {
            return false;
        }

        for (auto child_id = 0U; child_id < _children.size(); ++child_id)
        {
            if (_children[child_id]->is_equals(udf_operation->_children[child_id]) == false)
            {
                return false;
            }
        }

        return true;
    }

    [[nodiscard]] std::optional<std::reference_wrapper<const udf::Descriptor>> descriptor() const noexcept
    {
        return _descriptor;
    }

    void descriptor(std::reference_wrapper<const udf::Descriptor> descriptor) noexcept { _descriptor = descriptor; }

    [[nodiscard]] const std::string &function_name() const noexcept { return _function_name; }

private:
    std::string _function_name;
    std::vector<std::unique_ptr<Operation>> _children;
    std::optional<std::reference_wrapper<const udf::Descriptor>> _descriptor{std::nullopt};
};

class ExistsOperation : public Operation
{
public:
    explicit ExistsOperation(std::unique_ptr<parser::NodeInterface> &&sub_query)
        : Operation(Operation::Exists), _sub_query(std::move(sub_query))
    {
    }
    ExistsOperation(ExistsOperation &&) = default;

    ~ExistsOperation() override = default;

    [[nodiscard]] std::unique_ptr<parser::NodeInterface> &sub_query() noexcept { return _sub_query; }

    [[nodiscard]] std::unique_ptr<Operation> copy() const override { return nullptr; }

    [[nodiscard]] std::string to_string(std::uint16_t /*level*/ = 0U) const noexcept override { return "SUB QUERY"; }

    [[nodiscard]] type::Type type(const topology::LogicalSchema & /*schema*/) const override { return type::Type{}; }

    [[nodiscard]] bool is_equals(const std::unique_ptr<Operation> &other) const noexcept override
    {
        return other->id() == _id && other->result() == _result;
    }

private:
    std::unique_ptr<parser::NodeInterface> _sub_query;
};

using nullary_callback_t = std::function<void(const std::unique_ptr<NullaryOperation> &)>;
using unary_callback_t = std::function<void(const std::unique_ptr<UnaryOperation> &)>;
using binary_callback_t = std::function<void(const std::unique_ptr<BinaryOperation> &)>;
using list_callback_t = std::function<void(const std::unique_ptr<ListOperation> &)>;
using attribute_callback_t = std::function<void(Attribute &)>;
using term_callback_t = std::function<void(const Term &)>;

void visit(nullary_callback_t &&nullary_callback, unary_callback_t &&unary_callback,
           binary_callback_t &&binary_callback, list_callback_t &&list_callback,
           const std::unique_ptr<Operation> &operation);
inline void visit(nullary_callback_t &&nullary_callback, const std::unique_ptr<Operation> &operation)
{
    expression::visit(
        std::move(nullary_callback), [](const std::unique_ptr<UnaryOperation> & /*unary_operation*/) {},
        [](const std::unique_ptr<BinaryOperation> & /*binary_operation*/) {},
        [](const std::unique_ptr<ListOperation> & /*list_operation*/) {}, operation);
}
void for_each_attribute(const std::unique_ptr<Operation> &operation, attribute_callback_t &&callback);
void for_each_term(const std::unique_ptr<Operation> &operation, term_callback_t &&callback);
void for_each_comparison(const std::unique_ptr<Operation> &operation, binary_callback_t &&callback);
std::vector<Attribute> attributes(const std::unique_ptr<Operation> &operation);
std::vector<Attribute> attributes(std::unique_ptr<Operation> &&operation);
std::vector<NullaryOperation> nullaries(const std::unique_ptr<Operation> &operation, bool attribute_required = false);
std::vector<NullaryOperation> nullaries(std::unique_ptr<Operation> &&operation, bool attribute_required = false);
} // namespace db::expression