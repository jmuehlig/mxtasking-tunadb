#include <db/topology/physical_schema.h>
#include <gtest/gtest.h>

TEST(DB, PhysicalSchema)
{
    auto schema = db::topology::PhysicalSchema{};
    ASSERT_TRUE(schema.empty());
    ASSERT_EQ(schema.size(), 0U);
    ASSERT_EQ(schema.row_size(), 0U);

    schema.emplace_back(db::expression::Term::make_attribute("ID"), db::type::Type::make_bigint());
    ASSERT_FALSE(schema.empty());
    ASSERT_EQ(schema.size(), 1U);
    ASSERT_EQ(schema.row_size(), sizeof(db::type::underlying<db::type::Id::BIGINT>::value));

    {
        const auto index = schema.index("ID");
        ASSERT_TRUE(index.has_value());
        ASSERT_EQ(index.value(), 0U);
        ASSERT_EQ(schema.row_offset(index.value()), 0U);
        ASSERT_EQ(schema.type(index.value()), db::type::Id::BIGINT);
        ASSERT_EQ(schema.order()[index.value()], 0U);
    }

    schema.emplace_back(db::expression::Term::make_attribute("Name"), db::type::Type::make_char(50U));
    ASSERT_FALSE(schema.empty());
    ASSERT_EQ(schema.size(), 2U);
    ASSERT_EQ(schema.row_size(), sizeof(db::type::underlying<db::type::Id::BIGINT>::value) + 50U);

    {
        const auto index = schema.index("Name");
        ASSERT_TRUE(index.has_value());
        ASSERT_EQ(index.value(), 1U);
        ASSERT_EQ(schema.row_offset(index.value()), sizeof(db::type::underlying<db::type::Id::BIGINT>::value));
        ASSERT_EQ(schema.type(index.value()), db::type::Id::CHAR);
        ASSERT_EQ(schema.order()[index.value()], 1U);
    }

    schema.align_to(64U);
    ASSERT_EQ(schema.row_size(), 64U);
}