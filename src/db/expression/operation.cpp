#include "operation.h"

using namespace db::expression;

std::string UserDefinedFunctionOperation::to_string(const std::uint16_t /*level*/) const noexcept
{
    auto children = std::vector<std::string>{};
    std::transform(this->_children.begin(), this->_children.end(), std::back_inserter(children),
                   [](const auto &child) { return child->to_string(); });

    auto return_type = std::string{""};
    if (this->_descriptor.has_value())
    {
        return_type = fmt::format(" -> {}", this->_descriptor->get().return_type().to_string());
    }

    return fmt::format("{}({}){}", this->_function_name, fmt::join(std::move(children), ","), std::move(return_type));
}

db::type::Type UserDefinedFunctionOperation::type(const topology::LogicalSchema & /*schema*/) const
{
    if (this->_descriptor.has_value())
    {
        return this->_descriptor->get().return_type();
    }

    return type::Type{};
}

void db::expression::visit(nullary_callback_t &&nullary_callback, unary_callback_t &&unary_callback,
                           binary_callback_t &&binary_callback, list_callback_t &&list_callback,
                           const std::unique_ptr<Operation> &operation)
{
    if (operation->is_nullary())
    {
        nullary_callback(reinterpret_cast<const std::unique_ptr<NullaryOperation> &>(operation));
    }
    else if (operation->is_unary())
    {
        const auto &unary = reinterpret_cast<const std::unique_ptr<UnaryOperation> &>(operation);
        unary_callback(unary);
        expression::visit(std::move(nullary_callback), std::move(unary_callback), std::move(binary_callback),
                          std::move(list_callback), unary->child());
    }
    else if (operation->is_binary())
    {
        const auto &binary = reinterpret_cast<const std::unique_ptr<BinaryOperation> &>(operation);
        binary_callback(binary);

        expression::visit(nullary_callback_t{nullary_callback}, unary_callback_t{unary_callback},
                          binary_callback_t{binary_callback}, list_callback_t{list_callback}, binary->left_child());
        expression::visit(std::move(nullary_callback), std::move(unary_callback), std::move(binary_callback),
                          std::move(list_callback), binary->right_child());
    }
    else if (operation->is_list())
    {
        const auto &list = reinterpret_cast<const std::unique_ptr<ListOperation> &>(operation);
        list_callback(list);
        for (const auto &child : list->children())
        {
            expression::visit(nullary_callback_t{nullary_callback}, unary_callback_t{unary_callback},
                              binary_callback_t{binary_callback}, list_callback_t{list_callback}, child);
        }
    }
}

std::vector<db::expression::Attribute> db::expression::attributes(const std::unique_ptr<Operation> &operation)
{
    std::vector<Attribute> attributes;
    expression::visit(
        [&attributes](const std::unique_ptr<NullaryOperation> &nullary) {
            if (nullary->term().is_attribute())
            {
                attributes.push_back(nullary->term().get<Attribute>());
            }
        },
        [](const std::unique_ptr<UnaryOperation> & /*unary*/) {},
        [](const std::unique_ptr<BinaryOperation> & /*binary*/) {},
        [](const std::unique_ptr<ListOperation> & /*list*/) {}, operation);
    return attributes;
}

std::vector<db::expression::Attribute> db::expression::attributes(std::unique_ptr<Operation> &&operation)
{
    std::vector<Attribute> attributes;
    expression::visit(
        [&attributes](const std::unique_ptr<NullaryOperation> &nullary) {
            if (nullary->term().is_attribute())
            {
                attributes.emplace_back(std::move(nullary->term().get<Attribute>()));
            }
        },
        [](const std::unique_ptr<UnaryOperation> & /*unary*/) {},
        [](const std::unique_ptr<BinaryOperation> & /*binary*/) {},
        [](const std::unique_ptr<ListOperation> & /*list*/) {}, operation);
    return attributes;
}

std::vector<db::expression::NullaryOperation> db::expression::nullaries(const std::unique_ptr<Operation> &operation,
                                                                        const bool attribute_required)
{
    std::vector<NullaryOperation> nullaries;
    expression::visit(
        [&nullaries, attribute_required](const std::unique_ptr<NullaryOperation> &nullary) {
            if (attribute_required == false || nullary->term().is_attribute())
            {
                nullaries.emplace_back(*nullary);
            }
        },
        [](const std::unique_ptr<UnaryOperation> & /*unary*/) {},
        [](const std::unique_ptr<BinaryOperation> & /*binary*/) {},
        [](const std::unique_ptr<ListOperation> & /*list*/) {}, operation);
    return nullaries;
}

std::vector<db::expression::NullaryOperation> db::expression::nullaries(std::unique_ptr<Operation> &&operation,
                                                                        const bool attribute_required)
{
    std::vector<NullaryOperation> nullaries;
    expression::visit(
        [&nullaries, attribute_required](const std::unique_ptr<NullaryOperation> &nullary) {
            if (attribute_required == false || nullary->term().is_attribute())
            {
                nullaries.emplace_back(std::move(*nullary));
            }
        },
        [](const std::unique_ptr<UnaryOperation> & /*unary*/) {},
        [](const std::unique_ptr<BinaryOperation> & /*binary*/) {},
        [](const std::unique_ptr<ListOperation> & /*list*/) {}, operation);
    return nullaries;
}

void db::expression::for_each_attribute(const std::unique_ptr<Operation> &operation, attribute_callback_t &&callback)
{
    expression::visit(
        [&callback](const std::unique_ptr<NullaryOperation> &nullary) {
            if (nullary->term().is_attribute())
            {
                callback(nullary->term().get<Attribute>());
            }
        },
        [](const std::unique_ptr<UnaryOperation> & /*unary*/) {},
        [](const std::unique_ptr<BinaryOperation> & /*binary*/) {},
        [](const std::unique_ptr<ListOperation> & /*list*/) {}, operation);
}

void db::expression::for_each_term(const std::unique_ptr<Operation> &operation, term_callback_t &&callback)
{
    expression::visit([&callback](const std::unique_ptr<NullaryOperation> &nullary) { callback(nullary->term()); },
                      [](const std::unique_ptr<UnaryOperation> & /*unary*/) {},
                      [](const std::unique_ptr<BinaryOperation> & /*binary*/) {},
                      [](const std::unique_ptr<ListOperation> & /*list*/) {}, operation);
}

void db::expression::for_each_comparison(const std::unique_ptr<Operation> &operation, binary_callback_t &&callback)
{
    expression::visit([](const std::unique_ptr<NullaryOperation> & /*nullary*/) {},
                      [](const std::unique_ptr<UnaryOperation> & /*unary*/) {},
                      [&callback](const std::unique_ptr<BinaryOperation> &binary) {
                          if (binary->is_comparison())
                          {
                              callback(binary);
                          }
                      },
                      [](const std::unique_ptr<ListOperation> & /*list*/) {}, operation);
}