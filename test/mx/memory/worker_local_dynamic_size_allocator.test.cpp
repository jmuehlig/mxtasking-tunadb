#include <gtest/gtest.h>
#include <mx/memory/worker_local_dynamic_size_allocator.h>
#include <random>

TEST(MxTasking, WorkerLocalDynamicSizeAllocator)
{
    auto core_set = mx::util::core_set::build(1U);
    auto allocator = mx::memory::dynamic::local::Allocator{core_set};

    // Allocation success
    auto *alloc1 = allocator.allocate(0U, 0U, 64U, sizeof(std::uint32_t));
    EXPECT_NE(alloc1, nullptr);

    // Alignment
    EXPECT_TRUE((std::uintptr_t(alloc1) & 0x3F) == 0U);

    // Allocation success
    auto *alloc2 = allocator.allocate(0U, 0U, 64U, sizeof(std::uint32_t));
    EXPECT_NE(alloc2, nullptr);
    EXPECT_NE(alloc2, alloc1);

    // Alignment
    EXPECT_TRUE((std::uintptr_t(alloc2) & 0x3F) == 0U);

    // is free
    EXPECT_FALSE(allocator.is_free());
    allocator.free(0U, alloc1);
    EXPECT_FALSE(allocator.is_free());
    allocator.free(0U, alloc2);
    EXPECT_TRUE(allocator.is_free());
}

TEST(MxTasking, WorkerLocalDynamicSizeAllocatorFromRemote)
{
    auto core_set = mx::util::core_set::build(2U);
    auto allocator = mx::memory::dynamic::local::Allocator{core_set};

    // Allocation success
    auto *alloc = allocator.allocate(0U, 0U, 64U, sizeof(std::uint32_t));
    EXPECT_NE(alloc, nullptr);

    // is free
    EXPECT_FALSE(allocator.is_free());

    allocator.free(1U, alloc);
    EXPECT_FALSE(allocator.is_free());

    allocator.reset(core_set, false);
    EXPECT_TRUE(allocator.is_free());
}

TEST(MxTasking, WorkerLocalDynamicSizeAllocatorStress)
{
    auto core_set = mx::util::core_set::build(1U);
    auto allocator = mx::memory::dynamic::local::Allocator{core_set};

    auto random_device = std::random_device{};
    auto dist = std::uniform_int_distribution<std::uint32_t>{1024U, 1024U * 1024U * 4U};

    auto allocations = std::vector<void *>{};
    for (auto i = 0U; i < 8000U; ++i)
    {
        allocations.emplace_back(allocator.allocate(0U, 0U, 64U, dist(random_device)));
        EXPECT_NE(allocations.back(), nullptr);
        EXPECT_TRUE((std::uintptr_t(allocations.back()) & 0x3F) == 0U);
    }

    EXPECT_FALSE(allocator.is_free());

    std::shuffle(allocations.begin(), allocations.end(), std::mt19937{random_device()});

    for (auto *allocation : allocations)
    {
        allocator.free(0U, allocation);
    }

    EXPECT_TRUE(allocator.is_free());
}