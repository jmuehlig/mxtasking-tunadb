add_executable(tunadb
    src/application/tunadb/main.cpp
    src/application/tunadb/udf.cpp
    ${DB_SRC}
    ${FLEX_lexer_OUTPUTS}
    ${BISON_parser_OUTPUTS}
)
add_dependencies(tunadb mxbenchmarking mxtasking perf flounder fmt-external httplib-external argparse-external json-external static-vector-external xxhash-external libcount-external spdlog-external linenoise-external)
target_link_libraries(tunadb pthread numa atomic mxtasking mxbenchmarking perf flounder fmt count)