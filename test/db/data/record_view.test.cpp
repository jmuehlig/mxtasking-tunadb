#include <cstdlib>
#include <db/data/row_record_view.h>
#include <db/topology/physical_schema.h>
#include <gtest/gtest.h>

TEST(DB, RecordView)
{
    auto schema = db::topology::PhysicalSchema{};
    schema.emplace_back(db::expression::Term::make_attribute("ID"), db::type::Type::make_bigint());
    schema.emplace_back(db::expression::Term::make_attribute("IsStudent"), db::type::Type::make_bool());
    schema.emplace_back(db::expression::Term::make_attribute("Name"), db::type::Type::make_char(32U));

    auto *data = std::malloc(schema.row_size());

    auto record = db::data::RowRecordView{schema, data};
    record.set(0U, db::type::underlying<db::type::BIGINT>::value(1337U));
    ASSERT_EQ(record.get(0U).get<db::type::BIGINT>(), 1337U);

    record.set(1U, db::type::underlying<db::type::BOOL>::value(false));
    ASSERT_FALSE(record.get(1U).get<db::type::BOOL>());
    record.set(1U, db::type::underlying<db::type::BOOL>::value(true));
    ASSERT_TRUE(record.get(1U).get<db::type::BOOL>());

    record.set(0U, db::type::underlying<db::type::BIGINT>::value(42U));
    ASSERT_EQ(record.get(0U).get<db::type::BIGINT>(), 42U);

    std::free(data);
}