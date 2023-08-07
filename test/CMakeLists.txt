set(TESTS
    test/mx/memory/alignment_helper.test.cpp
    test/mx/memory/dynamic_size_allocator.test.cpp
    test/mx/memory/worker_local_dynamic_size_allocator.test.cpp
    test/mx/memory/fixed_size_allocator.test.cpp
    test/mx/memory/tagged_ptr.test.cpp

    test/mx/queue/list.test.cpp
    test/mx/queue/mpsc.test.cpp
    test/mx/queue/mpmc.test.cpp

    test/mx/util/aligned_t.test.cpp
    test/mx/util/core_set.test.cpp
    test/mx/util/vector.test.cpp

    test/mx/tasking/prefetching/prefetch_list.cpp

    test/db/topology/physical_schema.test.cpp
    test/db/data/record_view.test.cpp
)

set(TEST_DEPENDENCIES
    src/db/data/value.cpp
    src/db/type/type.cpp
)

add_executable(mxtests test/test.cpp ${TESTS} ${TEST_DEPENDENCIES})
target_link_libraries(mxtests pthread numa atomic mxtasking mxbenchmarking gtest fmt)