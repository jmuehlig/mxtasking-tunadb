# Dependencies:
#   - ittapi for sending asm code to Intel VTune
#   - asmjit for machine code generation

ExternalProject_Add(
    ittapi-external
    GIT_REPOSITORY "https://github.com/intel/ittapi.git"
    GIT_TAG "v3.23.0"
    PREFIX "lib/ittapi"
    CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    INSTALL_COMMAND cmake -E echo ""
)

include_directories(lib/asmjit/src lib/ittapi/src/ittapi-external/include)
link_directories(lib/ittapi/src/ittapi-external-build/bin lib/asmjit/src/asmjit-external-build)

SET(FLOUNDER_SRC
    src/flounder/compilation/register_allocator.cpp
    src/flounder/compilation/register_assigner.cpp
    src/flounder/compilation/compiler.cpp
    src/flounder/compilation/translator.cpp
    src/flounder/optimization/optimizer.cpp
    src/flounder/optimization/cycle_estimator.cpp
    src/flounder/optimization/move_unlikely_branches_optimization.cpp
    src/flounder/comparator.cpp
    src/flounder/program.cpp
    src/flounder/lib.cpp
    src/flounder/string.cpp
)

add_library(flounder STATIC ${FLOUNDER_SRC})
add_dependencies(flounder asmjit-external ittapi-external fmt-external static-vector-external)
target_link_libraries(flounder dl asmjit ittnotify)