#include "queue_item.h"
#include <gtest/gtest.h>
#include <mx/queue/mpsc.h>

TEST(MxTasking, MPSCQueue)
{
    auto queue = mx::queue::MPSC<test::mx::queue::Item>{};
    EXPECT_EQ(queue.empty(), true);

    auto queue_item = test::mx::queue::Item{};
    queue.push_back(&queue_item);
    EXPECT_EQ(queue.empty(), false);
    auto *pulled_item = queue.pop_front();
    EXPECT_EQ(&queue_item, pulled_item);
    EXPECT_EQ(queue.empty(), true);
    EXPECT_EQ(queue.pop_front(), nullptr);
}