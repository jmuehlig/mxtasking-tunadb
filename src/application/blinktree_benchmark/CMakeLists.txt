add_executable(blinktree_benchmark
    src/application/blinktree_benchmark/main.cpp
    src/application/blinktree_benchmark/benchmark.cpp
)
add_dependencies(blinktree_benchmark argparse-external json-external fmt-external perf)
target_link_libraries(blinktree_benchmark pthread numa atomic mxtasking mxbenchmarking perf)

# YCSB targets
add_custom_target(ycsb-a ${CMAKE_SOURCE_DIR}/scripts/generate_ycsb a randint)
add_custom_target(ycsb-c ${CMAKE_SOURCE_DIR}/scripts/generate_ycsb c randint)
add_custom_target(ycsb-binding ${CMAKE_SOURCE_DIR}/scripts/build_ycsb_binding)