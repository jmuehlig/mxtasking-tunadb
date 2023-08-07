include(src/db/data/CMakeLists.txt)
include(src/db/execution/CMakeLists.txt)
include(src/db/expression/CMakeLists.txt)
include(src/db/io/CMakeLists.txt)
include(src/db/network/CMakeLists.txt)
include(src/db/parser/CMakeLists.txt)
include(src/db/plan/CMakeLists.txt)
include(src/db/statistic/CMakeLists.txt)
include(src/db/storage/CMakeLists.txt)
include(src/db/type/CMakeLists.txt)
include(src/db/util/CMakeLists.txt)

SET(DB_SRC
    ${DB_DATA_SRC}
    ${DB_EXECUTION_SRC}
    ${DB_EXPRESSION_SRC}
    ${DB_IO_SRC}
    ${DB_NETWORK_SRC}
    ${DB_PARSER_SRC}
    ${DB_PLAN_SRC}
    ${DB_STATISTIC_SRC}
    ${DB_STORAGE_SRC}
    ${DB_TYPE_SRC}
    ${DB_UTIL_SRC}
    lib/linenoise/src/linenoise-external/linenoise.c
)

