SET(MX_BENCHMARKING_SRC
    src/benchmark/workload_set.cpp
    src/benchmark/workload.cpp
    src/benchmark/cores.cpp
    src/benchmark/string_util.cpp
)

add_library(mxbenchmarking SHARED ${MX_BENCHMARKING_SRC})
add_dependencies(mxbenchmarking static-vector-external)
