add_executable(radix_join_benchmark
    src/application/radix_join_benchmark/main.cpp
    src/application/radix_join_benchmark/benchmark.cpp
)
add_dependencies(radix_join_benchmark argparse-external json-external fmt-external perf)
target_link_libraries(radix_join_benchmark pthread numa atomic mxtasking mxbenchmarking perf fmt)