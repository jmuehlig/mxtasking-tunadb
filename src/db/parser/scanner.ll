%{
    #include <db/parser/driver.h>
    #include <db/parser/scanner.hpp>
    #include <db/parser/parser.hpp>
    #include <db/parser/location.hh>
    #include <db/type/date.h>
    #include <db/type/bool.h>
    #include <db/type/decimal.h>

    static db::parser::location loc;

    #define YY_USER_ACTION loc.step(); loc.columns(yyleng);

    #undef  YY_DECL
    #define YY_DECL db::parser::Parser::symbol_type db::parser::Scanner::lex(db::parser::Driver &/*driver*/)

    #define yyterminate() return db::parser::Parser::make_END(loc);
%}


%option c++
%option yyclass="db::parser::Scanner"
%option noyywrap


%%
CREATE                              { return Parser::make_CREATE_TK(loc); }
INSERT                              { return Parser::make_INSERT_TK(loc); }
COPY                                { return Parser::make_COPY_TK(loc); }
DELIMITER                           { return Parser::make_DELIMITER_TK(loc); }
TABLE                               { return Parser::make_TABLE_TK(loc); }
TABLES                              { return Parser::make_TABLES_TK(loc); }
IF                                  { return Parser::make_IF_TK(loc); }
NOT                                 { return Parser::make_NOT_TK(loc); }
EXISTS                              { return Parser::make_EXISTS_TK(loc); }
INTO                                { return Parser::make_INTO_TK(loc); }
VALUES                              { return Parser::make_VALUES_TK(loc); }
NULL                                { return Parser::make_NULL_TK(loc); }
"PRIMARY KEY"                       { return Parser::make_PRIMARY_KEY_TK(loc); }
INT|INTEGER                         { return Parser::make_INT_TK(loc); }
BIGINT|BIGINTEGER                   { return Parser::make_BIGINT_TK(loc); }
DATE                                { return Parser::make_DATE_TK(loc); }
DECIMAL                             { return Parser::make_DECIMAL_TK(loc); }
CHAR                                { return Parser::make_CHAR_TK(loc); }
BOOL                                { return Parser::make_BOOL_TK(loc); }
EXPLAIN                             { return Parser::make_EXPLAIN_TK(loc); }
"EXPLAIN TASK GRAPH"                { return Parser::make_EXPLAIN_TASK_GRAPH_TK(loc); }
"EXPLAIN DATA FLOW"                 { return Parser::make_EXPLAIN_DATA_FLOW_GRAPH_TK(loc); }
"EXPLAIN PERFORMANCE"               { return Parser::make_EXPLAIN_PERFORMANCE_TK(loc); }
"EXPLAIN TASK LOAD"                 { return Parser::make_EXPLAIN_TASK_LOAD_TK(loc); }
"EXPLAIN TASK TRACES"               { return Parser::make_EXPLAIN_TASK_TRACES_TK(loc); }
"EXPLAIN FLOUNDER"                  { return Parser::make_EXPLAIN_FLOUNDER_TK(loc); }
"EXPLAIN ASSEMBLY"                  { return Parser::make_EXPLAIN_ASSEMBLY_TK(loc); }
"EXPLAIN ASM"                       { return Parser::make_EXPLAIN_ASSEMBLY_TK(loc); }
"EXPLAIN DRAM BANDWIDTH"            { return Parser::make_EXPLAIN_DRAM_BANDWIDTH_TK(loc); }
"EXPLAIN TIMES"                     { return Parser::make_EXPLAIN_TIMES_TK(loc); }
"SAMPLE ASSEMBLY"                   { return Parser::make_SAMPLE_ASSEMBLY_TK(loc); }
"SAMPLE OPERATORS"                  { return Parser::make_SAMPLE_OPERATORS_TK(loc); }
"SAMPLE MEMORY"                     { return Parser::make_SAMPLE_MEMORY_TK(loc); }
"SAMPLE HISTORICAL MEMORY"          { return Parser::make_SAMPLE_HISTORICAL_MEMORY_TK(loc); }
"WITH FREQ"                         { return Parser::make_WITH_FREQ_TK(loc); }
BRANCHES                            { return Parser::make_BRANCHES_TK(loc); }
"BRANCH MISSES"                     { return Parser::make_BRANCH_MISSES_TK(loc); }
CYCLES                              { return Parser::make_CYCLES_TK(loc); }
INSTRUCTIONS                        { return Parser::make_INSTRUCTIONS_TK(loc); }
"CACHE MISSES"                      { return Parser::make_CACHE_MISSES_TK(loc); }
"CACHE REFERENCES"                  { return Parser::make_CACHE_REFERENCES_TK(loc); }
"STALLS MEM ANY"                    { return Parser::make_STALLS_MEM_ANY_TK(loc); }
"STALLS L3 MISS"                    { return Parser::make_STALLS_L3_MISS_TK(loc); }
"STALLS L2 MISS"                    { return Parser::make_STALLS_L2_MISS_TK(loc); }
"STALLS L1D MISS"                   { return Parser::make_STALLS_L1D_MISS_TK(loc); }
"CYCLES L3 MISS"                    { return Parser::make_CYCLES_L3_MISS_TK(loc); }
"DTLB MISS"                         { return Parser::make_DTLB_MISS_TK(loc); }
"L3 MISS REMOTE"                    { return Parser::make_L3_MISS_REMOTE_TK(loc); }
"FB FULL"                           { return Parser::make_FB_FULL_TK(loc); }
"BACLEARS ANY"                      { return Parser::make_BACLEARS_ANY_TK(loc); }
"LOAD HIT L1D FB"                   { return Parser::make_LOAD_HIT_L1D_FB_TK(loc); }
"MEM LOADS"                         { return Parser::make_MEM_LOADS_TK(loc); }
"MEM STORES"                        { return Parser::make_MEM_STORES_TK(loc); }
"MEM LOAD L1 MISS"                  { return Parser::make_MEM_LOAD_L1_MISS_TK(loc); }
"MEM LOAD L2 MISS"                  { return Parser::make_MEM_LOAD_L2_MISS_TK(loc); }
"MEM LOAD L3 MISS"                  { return Parser::make_MEM_LOAD_L3_MISS_TK(loc); }
SELECT                              { return Parser::make_SELECT_TK(loc); }
FROM                                { return Parser::make_FROM_TK(loc); }
AS                                  { return Parser::make_AS_TK(loc); }
WHERE                               { return Parser::make_WHERE_TK(loc); }
JOIN                                { return Parser::make_JOIN_TK(loc); }
ON                                  { return Parser::make_ON_TK(loc); }
INTERVAL                            { return Parser::make_INTERVAL_TK(loc); }
YEAR                                { return Parser::make_YEAR_TK(loc); }
MONTH                               { return Parser::make_MONTH_TK(loc); }
DAY                                 { return Parser::make_DAY_TK(loc); }
LIMIT                               { return Parser::make_LIMIT_TK(loc); }
OFFSET                              { return Parser::make_OFFSET_TK(loc); }
"GROUP BY"                          { return Parser::make_GROUP_BY_TK(loc); }
"ORDER BY"                          { return Parser::make_ORDER_BY_TK(loc); }
ASC                                 { return Parser::make_ASC_TK(loc); }
DESC                                { return Parser::make_DESC_TK(loc); }
CAST                                { return Parser::make_CAST_TK(loc); }
"LOAD FILE"                         { return Parser::make_LOAD_FILE_TK(loc); }
"IMPORT CSV"                        { return Parser::make_IMPORT_CSV_TK(loc); }
"SEPARATED BY"                      { return Parser::make_SEPARATED_BY_TK(loc); }
STORE                               { return Parser::make_STORE_TK(loc); }
RESTORE                             { return Parser::make_RESTORE_TK(loc); }
STOP                                { return Parser::make_STOP_TK(loc); }
SET                                 { return Parser::make_SET_TK(loc); }
CONFIGURATION|CONFIG                { return Parser::make_CONFIGURATION_TK(loc); }
CORES                               { return Parser::make_CORES_TK(loc); }
"UPDATE STATISTICS"                 { return Parser::make_UPDATE_STATISTICS_TK(loc); }
\(						            { return Parser::make_LEFT_PARENTHESIS_TK(loc); }
\)						            { return Parser::make_RIGHT_PARENTHESIS_TK(loc); }
\,                                  { return Parser::make_COMMA_TK(loc); }
\.                                  { return Parser::make_DOT_TK(loc); }
"="                                 { return Parser::make_EQUALS_TK(loc); }
"<"                                 { return Parser::make_LESSER_TK(loc); }
"<="                                { return Parser::make_LESSER_EQUALS_TK(loc); }
">"                                 { return Parser::make_GREATER_TK(loc); }
">="                                { return Parser::make_GREATER_EQUALS_TK(loc); }
!=|<>                               { return Parser::make_NOT_EQUALS_TK(loc); }
BETWEEN                             { return Parser::make_BETWEEN_TK(loc); }
LIKE                                { return Parser::make_LIKE_TK(loc); }
AND                                 { return Parser::make_AND_TK(loc); }
OR                                  { return Parser::make_OR_TK(loc); }
IN                                  { return Parser::make_IN_TK(loc); }
COUNT                               { return Parser::make_COUNT_TK(loc); }
SUM                                 { return Parser::make_SUM_TK(loc); }
AVG                                 { return Parser::make_AVG_TK(loc); }
MIN                                 { return Parser::make_MIN_TK(loc); }
MAX                                 { return Parser::make_MAX_TK(loc); }
"+"                                 { return Parser::make_PLUS_TK(loc); }
"-"                                 { return Parser::make_MINUS_TK(loc); }
"/"                                 { return Parser::make_DIV_TK(loc); }
"*"                                 { return Parser::make_MUL_TK(loc); }
TRUE                                { return Parser::make_BOOL(true, loc); }
FALSE                               { return Parser::make_BOOL(false, loc); }
CASE                                { return Parser::make_CASE_TK(loc); }
WHEN                                { return Parser::make_WHEN_TK(loc); }
THEN                                { return Parser::make_THEN_TK(loc); }
ELSE                                { return Parser::make_ELSE_TK(loc); }
END                                 { return Parser::make_END_TK(loc); }
\'[0-9]{4}\-[0-9]{2}\-[0-9]{2}\'    { return Parser::make_DATE(db::type::Date::from_string(std::string{yytext}.substr(1, std::strlen(yytext)-2)), loc); }
\'[0-9]+\'                          { return Parser::make_STRING_INTEGER(std::stoll(std::string{yytext}.substr(1, std::strlen(yytext)-2)), loc); }
\'[^\']+\'                          { return Parser::make_STRING(std::string{yytext}.substr(1, std::strlen(yytext)-2), loc); }
[a-zA-Z_][a-zA-Z_0-9]*	            { return Parser::make_REFERENCE(yytext, loc); }
[0-9]+				                { return Parser::make_UNSIGNED_INTEGER(std::stoll(yytext), loc); }
-?[0-9]+				            { return Parser::make_INTEGER(std::stoll(yytext), loc); }
-?[0-9]*\.[0-9]*                    { return Parser::make_DECIMAL(db::type::Decimal::from_string(std::string{yytext}), loc); }
\n                                  { return Parser::make_END(loc); }
<<EOF>>                             { return Parser::make_END(loc); }
.                                   { }

%%

