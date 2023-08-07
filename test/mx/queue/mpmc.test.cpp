#include "queue_item.h"
#include <array>
#include <gtest/gtest.h>
#include <mx/queue/bound_mpmc.h>

TEST(MxTasking, MPMCQueue)
{
    auto queue = mx::queue::BoundMPMC<test::mx::queue::Item *>{8U};
    EXPECT_TRUE(queue.empty());

    auto queue_item = test::mx::queue::Item{};
    queue.push_back(&queue_item);
    EXPECT_FALSE(queue.empty());
    auto *pulled_item = queue.pop_front();
    EXPECT_EQ(&queue_item, pulled_item);
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.pop_front_or(nullptr), nullptr);

    std::array<test::mx::queue::Item, 9U> items{};
    for (auto i = 0U; i < 8U; ++i)
    {
        queue.push_back(&items[i]);
    }

    EXPECT_FALSE(queue.try_push_back(&items[8U]));

    for (auto i = 0U; i < 8U; ++i)
    {
        EXPECT_EQ(queue.pop_front(), &items[i]);
    }
    EXPECT_TRUE(queue.empty());
}