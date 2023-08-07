#pragma once
#include "node_interface.h"
#include <db/exception/plan_exception.h>
#include <db/plan/logical/table.h>
#include <fmt/format.h>

namespace db::plan::logical {
class ProjectionNode final : public UnaryNode
{
public:
    ProjectionNode(std::vector<expression::Term> &&terms) : UnaryNode("Projection"), _projected_terms(std::move(terms))
    {
    }

    ~ProjectionNode() override = default;

    [[nodiscard]] QueryType query_type() const noexcept override { return NodeInterface::QueryType::SELECT; }

    [[nodiscard]] std::uint64_t cardinality(const topology::Database & /*database*/,
                                            const NodeChildIterator &child_iterator) const override
    {
        return child_iterator.child(this)->relation().cardinality();
    }

    [[nodiscard]] topology::LogicalSchema schema(const NodeChildIterator &child_iterator) const override
    {
        const auto &child_schema = child_iterator.child(this)->relation().schema();

        auto schema = topology::LogicalSchema{};
        schema.reserve(child_schema.size());

        for (const auto &projected_term : _projected_terms)
        {
            if (projected_term.is_attribute())
            {
                const auto &attribute = projected_term.get<expression::Attribute>();
                if (attribute.is_asterisk())
                {
                    /// SELECT <alias>.* FROM ...
                    if (attribute.source().has_value())
                    {
                        for (auto i = 0U; i < child_schema.terms().size(); ++i)
                        {
                            const auto &child_term = child_schema.term(i);
                            if (child_term.is_attribute() &&
                                child_term.get<expression::Attribute>().source() == attribute.source() &&
                                child_term.is_generated() == false)
                            {
                                schema.emplace_back(
                                    expression::Term{expression::Attribute{child_term.get<expression::Attribute>(),
                                                                           attribute.is_print_table_name()}},
                                    child_schema.type(i));
                            }
                        }
                    }
                    /// SELECT * FROM ...
                    else
                    {
                        for (auto i = 0U; i < child_schema.terms().size(); ++i)
                        {
                            if (child_schema.term(i).is_generated() == false)
                            {
                                schema.emplace_back(child_schema.term(i), child_schema.type(i));
                            }
                        }
                    }
                }
                else
                {
                    const auto child_index = child_schema.index(projected_term);
                    if (child_index.has_value())
                    {
                        schema.emplace_back(projected_term, child_schema.type(child_index.value()));
                    }
                    else
                    {
                        throw exception::AttributeNotFoundException{projected_term.to_string()};
                    }
                }
            }
            else
            {
                const auto child_index = child_schema.index(projected_term);
                if (child_index.has_value())
                {
                    schema.emplace_back(projected_term, child_schema.type(child_index.value()));
                }
                else
                {
                    throw exception::AttributeNotFoundException{projected_term.to_string()};
                }
            }
        }

        return schema;
    }

    [[nodiscard]] nlohmann::json to_json(const topology::Database &database) const override
    {
        auto json = UnaryNode::to_json(database);
        auto operations = std::vector<std::string>{};
        std::transform(_projected_terms.begin(), _projected_terms.end(), std::back_inserter(operations),
                       [](const auto &term) { return term.to_string(); });
        json["data"]["Projections"] = fmt::format("{}", fmt::join(std::move(operations), ", "));
        return json;
    }

private:
    const std::vector<expression::Term> _projected_terms;
};
} // namespace db::plan::logical