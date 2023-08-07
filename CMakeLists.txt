cmake_minimum_required(VERSION 3.10)
project(mxtasking-tunadb)

include(ExternalProject)

# Check SSE is available
include(scripts/FindSSE.cmake)
FindSSE()

# Check RTM is available
include(scripts/FindRTM.cmake)
FindRTM()

# Check AVX2 is available
include(scripts/FindAVX2.cmake)
FindAVX2()

# Set compile flags
set(CMAKE_CXX_STANDARD 20)
#set(CMAKE_CXX_CLANG_TIDY "clang-tidy;--extra-arg-before=-std=c++20 --system-headers=0") # TODO: Crashes with c++20
set(CMAKE_CXX_FLAGS "-Werror \
 -Wno-invalid-offsetof -Wcast-qual -Wctor-dtor-privacy -Wdisabled-optimization \
 -Winit-self -Wmissing-declarations -Wmissing-include-dirs -Woverloaded-virtual \
 -Wredundant-decls -Wsign-promo -Wstrict-overflow=5 -Wswitch-default -Wundef \
 -Wno-unused -Wno-uninitialized")
# -Wshadow -Wformat=2 -Wcast-align   # TODO: Breaks asmjit
# -Wold-style-cast      # TODO: Breaks bison generated files

# Set compile flag for x86_64
if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native")
endif()

# Set SSE flag if available
IF(SSE4_2_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -DUSE_SSE2 -fPIC")
ENDIF(SSE4_2_FOUND)

# Set SSE flag if available
IF(AVX2_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -DUSE_AVX2")
ENDIF(AVX2_FOUND)

# Set RTM flag if available
IF(RTM_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mrtm -DUSE_RTM")
ENDIF(RTM_FOUND)

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG")
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE DEBUG)
endif()
message("Building in '${CMAKE_BUILD_TYPE}' mode.")

# Directories for output binaries and libraries
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# External Projects
ExternalProject_Add(
    asmjit-external
    GIT_REPOSITORY "https://github.com/jmuehlig/asmjit"
    GIT_TAG "master"
    PREFIX "lib/asmjit"
    CMAKE_ARGS "-DASMJIT_STATIC=TRUE"
    INSTALL_COMMAND cmake -E echo ""
)
ExternalProject_Add(
    httplib-external
    GIT_REPOSITORY "https://github.com/yhirose/cpp-httplib"
    GIT_TAG "v0.13.3"
    PREFIX "lib/httplib"
    INSTALL_COMMAND cmake -E echo ""
)
ExternalProject_Add(
    fmt-external
    GIT_REPOSITORY "https://github.com/fmtlib/fmt"
    GIT_TAG "10.0.0"
    PREFIX "lib/fmt"
    INSTALL_COMMAND cmake -E echo ""
    CMAKE_ARGS "-DBUILD_SHARED_LIBS=TRUE -DFMT_TEST=FALSE -DFMT_MASTER_PROJECT=OFF"
    BUILD_COMMAND make fmt
)
ExternalProject_Add(
    argparse-external
    GIT_REPOSITORY "https://github.com/p-ranav/argparse"
    GIT_TAG "v2.9"
    PREFIX "lib/argparse"
    INSTALL_COMMAND cmake -E echo ""
)
ExternalProject_Add(
    json-external
    GIT_REPOSITORY "https://github.com/nlohmann/json"
    GIT_TAG "v3.11.2"
    PREFIX "lib/json"
    INSTALL_COMMAND cmake -E echo ""
    BUILD_COMMAND cmake -E echo ""
    CONFIGURE_COMMAND cmake -E echo ""
)
ExternalProject_Add(
    static-vector-external
    GIT_REPOSITORY "https://github.com/jmacheta/static_vector"
    GIT_TAG "fe1abd1"
    PREFIX "lib/static-vector"
    INSTALL_COMMAND cmake -E echo ""
    BUILD_COMMAND cmake -E echo ""
)
ExternalProject_Add(
    libcount-external
    GIT_REPOSITORY "https://github.com/dialtr/libcount.git"
    GIT_TAG "8e49fda"
    PREFIX "lib/count"
    INSTALL_COMMAND cmake -E echo ""
    CONFIGURE_COMMAND cmake -E echo ""
    BUILD_IN_SOURCE true
    BUILD_COMMAND CXX=${CMAKE_CXX_COMPILER} make
)
ExternalProject_Add(
    xxhash-external
    GIT_REPOSITORY "https://github.com/ekpyron/xxhashct.git"
    GIT_TAG "2d1d905"
    PREFIX "lib/xxhash"
    INSTALL_COMMAND cmake -E echo ""
    CONFIGURE_COMMAND cmake -E echo ""
    BUILD_COMMAND cmake -E echo ""
)
ExternalProject_Add(
    spdlog-external
    GIT_REPOSITORY "https://github.com/gabime/spdlog.git"
    GIT_TAG "v1.12.0"
    PREFIX "lib/spdlog"
    INSTALL_COMMAND cmake -E echo ""
    CONFIGURE_COMMANG cmake -E echo ""
)
ExternalProject_Add(
    tsl-robin-map-external
    GIT_REPOSITORY "https://github.com/Tessil/robin-map.git"
    GIT_TAG "851a59e"
    PREFIX "lib/tsl-robin-map"
    INSTALL_COMMAND cmake -E echo ""
    CONFIGURE_COMMAND cmake -E echo ""
    BUILD_COMMAND cmake -E echo ""
)
ExternalProject_Add(
        linenoise-external
        GIT_REPOSITORY "https://github.com/antirez/linenoise.git"
        GIT_TAG "93b2db9"
        PREFIX "lib/linenoise"
        INSTALL_COMMAND cmake -E echo ""
        CONFIGURE_COMMAND cmake -E echo ""
        BUILD_COMMAND cmake -E echo ""
)

# External libraries
find_library(GTEST gtest)
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

# Include folders
include_directories(
    src/
    lib/
    lib/fmt/src/fmt-external/include
    lib/httplib/src/httplib-external
    lib/json/src/json-external/include
    lib/argparse/src/argparse-external/include
    lib/static-vector/src/static-vector-external/include
    lib/count/src/libcount-external/include
    lib/xxhash/src/xxhash-external
    lib/spdlog/src/spdlog-external/include
    lib/tsl-robin-map/src/tsl-robin-map-external/include
    lib/linenoise/src/linenoise-external
    lib/asmjit/src/asmjit-external/src
)

link_directories(lib/fmt/src/fmt-external-build lib/count/src/libcount-external)

# Include source definitions.
include(src/mx/CMakeLists.txt)
include(src/benchmark/CMakeLists.txt)
include(src/flounder/CMakeLists.txt)
include(src/perf/CMakeLists.txt)
include(src/db/CMakeLists.txt)

## Hello World example
include(src/application/hello_world/CMakeLists.txt)

## BLinkTree Benchmark
include(src/application/blinktree_benchmark/CMakeLists.txt)

## TunaDB
include(src/application/tunadb/CMakeLists.txt)

## Radix Join Benchmark
include(src/application/radix_join_benchmark/CMakeLists.txt)

# Print CoreSet
include(src/application/print_core_set/CMakeLists.txt)

# Tests
if (GTEST)
    include(test/CMakeLists.txt)
else()
    message("Library 'gtest' not found. Please install 'libgtest-dev' for support of unit tests.")
endif()
