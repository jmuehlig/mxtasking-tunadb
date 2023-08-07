set(DB_PARSER_SRC
    src/db/parser/driver.cpp
    src/db/parser/sql_parser.cpp
)

find_package(FLEX REQUIRED)
find_package(BISON REQUIRED)

BISON_TARGET(parser
        src/db/parser/parser.yy
        src/db/parser/parser.cpp
        VERBOSE
        COMPILE_FLAGS "--defines --language=C++"
        )

FLEX_TARGET(lexer
        src/db/parser/scanner.ll
        src/db/parser/scanner.cpp
        COMPILE_FLAGS "--c+ -i"
        )
ADD_FLEX_BISON_DEPENDENCY(lexer parser)