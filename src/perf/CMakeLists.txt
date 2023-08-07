SET(PERF_SRC
    src/perf/counter_description.cpp
    src/perf/counter.cpp
    src/perf/sample.cpp
    src/perf/jit_map.cpp
    src/perf/imc/mmio_range.cpp
    src/perf/imc/dram_bandwidth_monitor.cpp
)

add_library(perf SHARED ${PERF_SRC})