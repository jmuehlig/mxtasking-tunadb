#include <gtest/gtest.h>
#include <mx/util/core_set.h>

TEST(MxTasking, core_set)
{
    {
        auto core_set = mx::util::core_set{};
        EXPECT_FALSE(static_cast<bool>(core_set));
        EXPECT_EQ(core_set.count_cores(), 0U);
    }

    {
        auto core_set = mx::util::core_set::build(1U);
        EXPECT_TRUE(static_cast<bool>(core_set));
        EXPECT_EQ(core_set.count_cores(), 1U);
        EXPECT_EQ(core_set[0], 0U);
        EXPECT_EQ(core_set.max_core_id(), 0U);
    }

    {
        auto core_set = mx::util::core_set{0U, 2U};
        EXPECT_TRUE(static_cast<bool>(core_set));
        EXPECT_EQ(core_set.count_cores(), 2U);
        EXPECT_EQ(core_set[0], 0U);
        EXPECT_EQ(core_set[1], 2U);
        EXPECT_EQ(core_set.max_core_id(), 2U);
    }
}