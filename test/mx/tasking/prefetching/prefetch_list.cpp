#include <gtest/gtest.h>
#include <mx/tasking/prefetch_descriptor.h>

TEST(MxTasking, PrefetchList)
{
    {
        auto list = mx::tasking::PrefetchList{};
        auto descriptor = list.make(true);
        auto list_view = mx::tasking::PrefetchListView{descriptor};
        ASSERT_EQ(list_view.size(), 0U);
    }

    {
        auto list = mx::tasking::PrefetchList{};
        list.add(960U);
        auto descriptor = list.make(true);
        auto list_view = mx::tasking::PrefetchListView{descriptor};
        ASSERT_EQ(list_view.size(), 1U);
        ASSERT_EQ(list_view.at(0U), 960U);
    }

    {
        auto list = mx::tasking::PrefetchList{};
        list.add(1024);
        list.add(512U);
        auto descriptor = list.make(true);
        auto list_view = mx::tasking::PrefetchListView{descriptor};
        ASSERT_EQ(list_view.size(), 2U);
        ASSERT_EQ(list_view.at(0U), 512U);
        ASSERT_EQ(list_view.at(1U), 1024U);
    }

    {
        auto list = mx::tasking::PrefetchList{};
        list.add(1024);
        list.add(512U);
        list.add(0U);
        auto descriptor = list.make(true);
        auto list_view = mx::tasking::PrefetchListView{descriptor};
        ASSERT_EQ(list_view.size(), 3U);
        ASSERT_EQ(list_view.at(0U), 0U);
        ASSERT_EQ(list_view.at(1U), 512U);
        ASSERT_EQ(list_view.at(2U), 1024U);
    }

    {
        auto list = mx::tasking::PrefetchList{};
        list.add(1024);
        list.add(512U);
        list.add(128U);
        list.add(123U);
        list.add(0U);
        auto descriptor = list.make(true);
        auto list_view = mx::tasking::PrefetchListView{descriptor};
        ASSERT_EQ(list_view.size(), 5U);
        ASSERT_EQ(list_view.at(0U), 0U);
        ASSERT_EQ(list_view.at(1U), 123U);
        ASSERT_EQ(list_view.at(2U), 128U);
        ASSERT_EQ(list_view.at(3U), 512U);
        ASSERT_EQ(list_view.at(4U), 1024U);
    }
}