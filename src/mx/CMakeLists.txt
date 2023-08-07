SET(MX_TASKING_SRC
    src/mx/resource/builder.cpp
    src/mx/tasking/scheduler.cpp
    src/mx/tasking/worker.cpp
    src/mx/tasking/task.cpp
    src/mx/tasking/task_squad.cpp
    src/mx/tasking/prefetch_slot.cpp
    src/mx/tasking/profiling/idle_profiler.cpp
    src/mx/tasking/profiling/time.cpp
    src/mx/tasking/profiling/task_tracer.cpp
    src/mx/util/core_set.cpp
    src/mx/util/random.cpp
    src/mx/memory/dynamic_size_allocator.cpp
    src/mx/memory/worker_local_dynamic_size_allocator.cpp
    src/mx/memory/reclamation/epoch_manager.cpp
    src/mx/io/network/server.cpp
    src/mx/system/cpu.cpp
)
add_library(mxtasking SHARED ${MX_TASKING_SRC})
add_dependencies(mxtasking json-external static-vector-external tsl-robin-map-external)
