%skeleton "lalr1.cc"
%require  "3.0"

%defines
%define api.namespace {db::parser}
%define api.parser.class {Parser}
%define api.value.type variant
%define api.token.constructor
%define parse.assert true
%define parse.error verbose
%locations

%param {db::parser::Driver &driver}
%parse-param {db::parser::Scanner &scanner}

%code requires {
    #include <db/parser/node.h>
    #include <utility>
    #include <vector>
    #include <tuple>
    namespace db::parser {
        class Scanner;
        class Driver;
    }

    #define YY_NULLPTR nullptr
}


%{
    #include <cassert>
    #include <iostream>
    #include <memory>
    #include <vector>
    #include <string>
    #include <cstdint>

    #include <db/parser/driver.h>
    #include <db/parser/scanner.hpp>

    #include <db/parser/parser.hpp>
    #include <db/parser/location.hh>
    #include <db/exception/parser_exception.h>
    #include <db/expression/operation_builder.h>

    #undef yylex
    #define yylex scanner.lex
%}

%token <std::string> STRING REFERENCE
%token <std::int64_t> INTEGER STRING_INTEGER
%token <std::uint64_t> UNSIGNED_INTEGER
%token <type::Decimal> DECIMAL
%token <type::Date> DATE
%token <bool> BOOL
%token CREATE_TK INSERT_TK INTO_TK VALUES_TK COPY_TK DELIMITER_TK
%token TABLE_TK TABLES_TK
%token INT_TK BIGINT_TK DATE_TK DECIMAL_TK CHAR_TK BOOL_TK TRUE_TK FALSE_TK
%token IF_TK NOT_TK EXISTS_TK NULL_TK PRIMARY_KEY_TK
%token EXPLAIN_TK EXPLAIN_TASK_GRAPH_TK EXPLAIN_DATA_FLOW_GRAPH_TK EXPLAIN_PERFORMANCE_TK EXPLAIN_TASK_LOAD_TK EXPLAIN_TASK_TRACES_TK
%token EXPLAIN_FLOUNDER_TK EXPLAIN_ASSEMBLY_TK EXPLAIN_DRAM_BANDWIDTH_TK EXPLAIN_TIMES_TK
%token SAMPLE_ASSEMBLY_TK SAMPLE_OPERATORS_TK SAMPLE_MEMORY_TK SAMPLE_HISTORICAL_MEMORY_TK WITH_FREQ_TK
%token BRANCHES_TK BRANCH_MISSES_TK BACLEARS_ANY_TK CYCLES_TK INSTRUCTIONS_TK CACHE_MISSES_TK CACHE_REFERENCES_TK
%token STALLS_MEM_ANY_TK STALLS_L3_MISS_TK STALLS_L2_MISS_TK STALLS_L1D_MISS_TK CYCLES_L3_MISS_TK DTLB_MISS_TK L3_MISS_REMOTE_TK
%token FB_FULL_TK LOAD_HIT_L1D_FB_TK MEM_LOADS_TK MEM_STORES_TK MEM_LOAD_L1_MISS_TK MEM_LOAD_L2_MISS_TK MEM_LOAD_L3_MISS_TK
%token SELECT_TK FROM_TK AS_TK JOIN_TK ON_TK
%token LIMIT_TK OFFSET_TK
%token WHERE_TK EQUALS_TK LESSER_TK LESSER_EQUALS_TK GREATER_TK GREATER_EQUALS_TK NOT_EQUALS_TK AND_TK OR_TK BETWEEN_TK IN_TK LIKE_TK
%token ORDER_BY_TK ASC_TK DESC_TK
%token COUNT_TK SUM_TK AVG_TK MIN_TK MAX_TK GROUP_BY_TK
%token PLUS_TK MINUS_TK DIV_TK MUL_TK CAST_TK
%token CASE_TK WHEN_TK THEN_TK ELSE_TK END_TK
%token LEFT_PARENTHESIS_TK RIGHT_PARENTHESIS_TK
%token COMMA_TK DOT_TK END
%token LOAD_FILE_TK IMPORT_CSV_TK SEPARATED_BY_TK
%token STORE_TK RESTORE_TK
%token STOP_TK
%token CONFIGURATION_TK SET_TK CORES_TK
%token INTERVAL_TK YEAR_TK MONTH_TK DAY_TK
%token UPDATE_STATISTICS_TK

%type <std::unique_ptr<NodeInterface>> query
%type <std::unique_ptr<NodeInterface>> command
%type <std::unique_ptr<NodeInterface>> statement
%type <std::unique_ptr<CreateStatement>> create_statement
%type <std::unique_ptr<InsertStatement>> insert_statement
%type <std::unique_ptr<CopyStatement>> copy_statement
%type <std::string> optional_copy_delimiter
%type <std::unique_ptr<StopCommand>> stop_command
%type <std::unique_ptr<ShowTablesCommand>> show_tables_command
%type <std::unique_ptr<DescribeTableCommand>> describe_table_command
%type <std::unique_ptr<LoadFileCommand>> load_file_command
%type <std::unique_ptr<CopyStatement>> import_csv_command
%type <std::unique_ptr<StoreCommand>> store_command
%type <std::unique_ptr<RestoreCommand>> restore_command
%type <std::unique_ptr<SetCoresCommand>> set_cores_command
%type <std::unique_ptr<GetConfigurationCommand>> get_configuration_command
%type <std::unique_ptr<UpdateStatisticsCommand>> update_statistics_command
%type <std::tuple<expression::Term, type::Type, bool, bool>> column_description
%type <topology::PhysicalSchema> column_description_list
%type <type::Type> type_description
%type <std::vector<std::string>> optional_column_list
%type <std::vector<std::string>> column_list
%type <std::vector<std::vector<data::Value>>> value_list
%type <std::vector<data::Value>> values
%type <std::vector<data::Value>> values_with_parenthesis
%type <data::Value> value
%type <type::Date> date_interval
%type <std::unique_ptr<SelectQuery>> select_statement
%type <std::unique_ptr<SelectQuery>> select_query
%type <std::vector<plan::logical::TableReference>> table_reference_list
%type <plan::logical::TableReference> table_reference
%type <std::optional<std::vector<plan::logical::JoinReference>>> optional_join_reference_list
%type <std::vector<plan::logical::JoinReference>> join_reference_list
%type <plan::logical::JoinReference> join_reference
%type <std::unique_ptr<expression::Operation>> join_predicate
%type <std::unique_ptr<expression::Operation>> join_operand
%type <std::vector<std::unique_ptr<expression::Operation>>> attribute_list
%type <std::unique_ptr<expression::Operation>> attribute
%type <std::unique_ptr<expression::Operation>> aliased_attribute
%type <std::unique_ptr<expression::Operation>> operand
%type <std::unique_ptr<expression::Operation>> operation
%type <std::unique_ptr<expression::Operation>> optional_where
%type <std::vector<expression::Term>> operand_value_list
%type <std::optional<std::vector<expression::Term>>> optional_group_by
%type <std::vector<expression::Term>> group_by_list
%type <expression::Term> group_by_item
%type <std::optional<std::vector<expression::OrderBy>>> optional_order_by
%type <std::vector<expression::OrderBy>> order_by_item_list
%type <expression::OrderBy> order_by_item
%type <std::unique_ptr<expression::Operation>> order_by_reference
%type <std::optional<expression::Limit>> optional_limit
%type <std::vector<std::unique_ptr<expression::Operation>>> when_then_list
%type <std::unique_ptr<expression::Operation>> when_then
%type <std::unique_ptr<expression::Operation>> else
%type <std::string> optional_separated_by
%type <SelectQuery::ExplainLevel> explain
%type <SelectQuery::SampleCounterType> sample_type
%type <std::optional<std::uint64_t>> sample_frequency
%type <std::tuple<SelectQuery::SampleLevel, SelectQuery::SampleCounterType, std::optional<std::uint64_t>>> sample

%type <bool> optional_if_not_exists
%type <bool> optional_nullable
%type <bool> optional_primary_key

%left DOT_TK AND_TK OR_TK PLUS_TK MINUS_TK MUL_TK DIV_TK
%nonassoc EQUALS_TK LESSER_TK LESSER_EQUALS_TK GREATER_TK GREATER_EQUALS_TK NOT_EQUALS_TK

%start query
%%

query:
    statement END { driver.ast(std::move($1)); YYACCEPT; }
    | select_statement END { driver.ast(std::move($1)); YYACCEPT; }
    | command END { driver.ast(std::move($1)); YYACCEPT; }

statement:
    create_statement { $$ = std::move($1); }
    | insert_statement { $$ = std::move($1); }
    | copy_statement { $$ = std::move($1); }

/******************************
 * STATEMENTS
 ******************************/

/** CREATE **/
create_statement:
    CREATE_TK TABLE_TK optional_if_not_exists REFERENCE LEFT_PARENTHESIS_TK column_description_list RIGHT_PARENTHESIS_TK {
        $$ = std::make_unique<CreateStatement>(std::move($4), $3, std::move($6));
    }

column_description:
	REFERENCE type_description optional_nullable optional_primary_key { $$ = std::make_tuple(expression::Term::make_attribute(std::move($1)), $2, $3, $4); }

column_description_list:
	column_description { $$ = topology::PhysicalSchema{}; $$.emplace_back(std::move(std::get<0>($1)), std::move(std::get<1>($1)), std::get<2>($1), std::get<3>($1)); }
	| column_description_list COMMA_TK column_description { $$ = std::move($1); $$.emplace_back(std::move(std::get<0>($3)), std::move(std::get<1>($3)), std::get<2>($3), std::get<3>($3)); }

type_description:
    INT_TK { $$ = type::Type::make_int(); }
    | BIGINT_TK { $$ = type::Type::make_bigint(); }
    | DATE_TK { $$ = type::Type::make_date(); }
    | BOOL_TK { $$ = type::Type::make_bool(); }
    | DECIMAL_TK { $$ = type::Type::make_decimal(16, 2); }
    | DECIMAL_TK LEFT_PARENTHESIS_TK UNSIGNED_INTEGER COMMA_TK UNSIGNED_INTEGER RIGHT_PARENTHESIS_TK { $$ = type::Type::make_decimal($3, $5); }
    | CHAR_TK LEFT_PARENTHESIS_TK UNSIGNED_INTEGER RIGHT_PARENTHESIS_TK { $$ = type::Type::make_char($3); }

optional_if_not_exists:
    IF_TK NOT_TK EXISTS_TK { $$ = true; }
    | { $$ = false; }

optional_nullable:
    NULL_TK { $$ = true; }
    | NOT_TK NULL_TK { $$ = false; }
    | { $$ = false; }

optional_primary_key:
    PRIMARY_KEY_TK { $$ = true; }
    | { $$ = false; }

/** INSERT **/
insert_statement:
    INSERT_TK INTO_TK REFERENCE optional_column_list VALUES_TK value_list {
        $$ = std::make_unique<InsertStatement>(std::move($3), std::move($4), std::move($6));
    }

column_list:
	REFERENCE { $$ = std::vector<std::string>{}; $$.emplace_back(std::move($1)); }
	| column_list COMMA_TK REFERENCE { $$ = std::move($1); $$.emplace_back(std::move($3)); }

optional_column_list:
    LEFT_PARENTHESIS_TK column_list RIGHT_PARENTHESIS_TK { $$ = std::move($2); }
    | { $$ = std::vector<std::string>{}; }

value_list:
	values_with_parenthesis { $$ = std::vector<std::vector<data::Value>>{}; $$.emplace_back(std::move($1)); }
	| value_list COMMA_TK values_with_parenthesis { $$ = std::move($1); $$.emplace_back(std::move($3)); }

values_with_parenthesis:
    LEFT_PARENTHESIS_TK values RIGHT_PARENTHESIS_TK { $$ = std::move($2); }

values:
    value { $$ = std::vector<data::Value>{}; $$.emplace_back(std::move($1)); }
    | values COMMA_TK value { $$ = std::move($1); $$.emplace_back(std::move($3)); }

value:
    STRING { $$ = data::Value{type::Type::make_char($1.length()), data::Value::value_t{std::move($1)}}; }
    | UNSIGNED_INTEGER { $$ = data::Value{type::Type::make_bigint(), data::Value::value_t{std::int64_t($1)}}; }
    | INTEGER { $$ = data::Value{type::Type::make_bigint(), data::Value::value_t{std::int64_t($1)}}; }
    | STRING_INTEGER { $$ = data::Value{type::Type::make_bigint(), data::Value::value_t{std::int64_t($1)}}; }
    | DATE { $$ = data::Value{type::Type::make_date(), data::Value::value_t{$1}}; }
    | DATE_TK DATE { $$ = data::Value{type::Type::make_date(), data::Value::value_t{$2}}; }
    | date_interval { $$ = data::Value{type::Type::make_date(), data::Value::value_t{$1}}; }
    | BOOL { $$ = data::Value{type::Type::make_bool(), data::Value::value_t{$1}}; }
    | DECIMAL { $$ = data::Value{type::Type::make_decimal($1.description()), data::Value::value_t{$1.data()}}; }

date_interval:
    INTERVAL_TK STRING_INTEGER YEAR_TK { $$ = type::Date::make_interval_from_years(std::uint16_t($2)); }
    | INTERVAL_TK STRING_INTEGER MONTH_TK { $$ = type::Date::make_interval_from_months(std::uint16_t($2)); }
    | INTERVAL_TK STRING_INTEGER DAY_TK { $$ = type::Date::make_interval_from_days(std::uint16_t($2)); }

/** COPY **/
copy_statement:
    COPY_TK REFERENCE FROM_TK STRING optional_copy_delimiter
    {
        $$ = std::make_unique<CopyStatement>(std::move($2), std::move($4), std::move($5));
    }

optional_copy_delimiter:
    LEFT_PARENTHESIS_TK DELIMITER_TK STRING RIGHT_PARENTHESIS_TK { $$ = $3; }
    | { $$ = ";"; }

/******************************
 * QUERY
 ******************************/
select_statement:
    select_query { $$ = std::move($1); }
    | explain select_query { $$ = std::move($2); $$->explain_level($1); }
    | sample select_query { $$ = std::move($2); $$->sample(std::get<0>($1), std::get<1>($1), std::get<2>($1));  }

select_query:
    SELECT_TK attribute_list FROM_TK table_reference_list optional_join_reference_list optional_where optional_group_by optional_order_by optional_limit
    {
        $$ = std::make_unique<SelectQuery>(std::move($2), std::move($4), std::move($5), std::move($6), std::move($7), std::move($8), std::move($9));
    }

explain:
    EXPLAIN_TASK_GRAPH_TK { $$ = SelectQuery::ExplainLevel::TaskGraph; }
    | EXPLAIN_DATA_FLOW_GRAPH_TK { $$ = SelectQuery::ExplainLevel::DataFlowGraph; }
    | EXPLAIN_PERFORMANCE_TK { $$ = SelectQuery::ExplainLevel::Performance; }
    | EXPLAIN_TASK_LOAD_TK { $$ = SelectQuery::ExplainLevel::TaskLoad; }
    | EXPLAIN_TASK_TRACES_TK { $$ = SelectQuery::ExplainLevel::TaskTraces; }
    | EXPLAIN_FLOUNDER_TK { $$ = SelectQuery::ExplainLevel::Flounder; }
    | EXPLAIN_ASSEMBLY_TK { $$ = SelectQuery::ExplainLevel::Assembly; }
    | EXPLAIN_DRAM_BANDWIDTH_TK { $$ = SelectQuery::ExplainLevel::DRAMBandwidth; }
    | EXPLAIN_TIMES_TK { $$ = SelectQuery::ExplainLevel::Times; }
    | EXPLAIN_TK { $$ = SelectQuery::ExplainLevel::Plan; }

sample:
    SAMPLE_ASSEMBLY_TK sample_type sample_frequency { $$ = std::make_tuple(SelectQuery::SampleLevel::Assembly, $2, $3); }
    | SAMPLE_OPERATORS_TK sample_type sample_frequency  { $$ = std::make_tuple(SelectQuery::SampleLevel::Operators, $2, $3); }
    | SAMPLE_MEMORY_TK sample_type sample_frequency  { $$ = std::make_tuple(SelectQuery::SampleLevel::Memory, $2, $3); }
    | SAMPLE_HISTORICAL_MEMORY_TK sample_type sample_frequency { $$ = std::make_tuple(SelectQuery::SampleLevel::HistoricalMemory, $2, $3); }

sample_type:
    BRANCHES_TK { $$ = SelectQuery::SampleCounterType::Branches; }
    | BRANCH_MISSES_TK { $$ = SelectQuery::SampleCounterType::BranchMisses; }
    | CYCLES_TK { $$ = SelectQuery::SampleCounterType::Cycles; }
    | INSTRUCTIONS_TK  { $$ = SelectQuery::SampleCounterType::Instructions; }
    | CACHE_MISSES_TK  { $$ = SelectQuery::SampleCounterType::CacheMisses; }
    | CACHE_REFERENCES_TK  { $$ = SelectQuery::SampleCounterType::CacheReferences; }
    | STALLS_MEM_ANY_TK  { $$ = SelectQuery::SampleCounterType::StallsMemAny; }
    | STALLS_L3_MISS_TK  { $$ = SelectQuery::SampleCounterType::StallsL3Miss; }
    | STALLS_L2_MISS_TK  { $$ = SelectQuery::SampleCounterType::StallsL2Miss; }
    | STALLS_L1D_MISS_TK  { $$ = SelectQuery::SampleCounterType::StallsL1DMiss; }
    | CYCLES_L3_MISS_TK { $$ = SelectQuery::SampleCounterType::CyclesL3Miss; }
    | DTLB_MISS_TK { $$ = SelectQuery::SampleCounterType::DTLBMiss; }
    | L3_MISS_REMOTE_TK { $$ = SelectQuery::SampleCounterType::L3MissRemote; }
    | FB_FULL_TK { $$ = SelectQuery::SampleCounterType::FillBufferFull; }
    | LOAD_HIT_L1D_FB_TK { $$ = SelectQuery::SampleCounterType::LoadHitL1DFillBuffer; }
    | BACLEARS_ANY_TK { $$ = SelectQuery::SampleCounterType::BAClearsAny; }
    | MEM_LOADS_TK { $$ = SelectQuery::SampleCounterType::MemRetiredLoads; }
    | MEM_STORES_TK { $$ = SelectQuery::SampleCounterType::MemRetiredStores; }
    | MEM_LOAD_L1_MISS_TK { $$ = SelectQuery::SampleCounterType::MemRetiredLoadL1Miss; }
    | MEM_LOAD_L2_MISS_TK { $$ = SelectQuery::SampleCounterType::MemRetiredLoadL2Miss; }
    | MEM_LOAD_L3_MISS_TK { $$ = SelectQuery::SampleCounterType::MemRetiredLoadL3Miss; }

sample_frequency:
    WITH_FREQ_TK UNSIGNED_INTEGER { $$ = std::make_optional<std::uint64_t>($2); }
    | { $$ = std::nullopt; }

table_reference_list:
	table_reference { $$ = std::vector<plan::logical::TableReference>{}; $$.emplace_back(std::move($1)); }
	| table_reference_list COMMA_TK table_reference { $$ = std::move($1); $$.emplace_back(std::move($3)); }

table_reference:
    REFERENCE { $$ = plan::logical::TableReference{std::move($1)}; }
    | REFERENCE REFERENCE { $$ = plan::logical::TableReference{std::move($1), std::move($2)}; }
    | REFERENCE AS_TK REFERENCE { $$ = plan::logical::TableReference{std::move($1), std::move($3)}; }

join_reference_list:
    join_reference { $$ = std::vector<plan::logical::JoinReference>{}; $$.emplace_back(std::move($1)); }
    | join_reference_list join_reference { $$ = std::move($1); $$.emplace_back(std::move($2)); }

join_reference:
    JOIN_TK table_reference ON_TK join_predicate { $$ = plan::logical::JoinReference{std::move($2), std::move($4)}; }

join_predicate:
    join_operand EQUALS_TK join_operand { $$ = expression::OperationBuilder::make_eq(std::move($1), std::move($3)); }

join_operand:
    REFERENCE { $$ = expression::OperationBuilder::make_attribute(std::move($1)); }
    | REFERENCE DOT_TK REFERENCE { $$ = expression::OperationBuilder::make_attribute(std::move($1), std::move($3)); }

optional_join_reference_list:
    join_reference_list { $$ = std::make_optional(std::move($1)); }
    | { $$ = std::nullopt; }

attribute_list:
	aliased_attribute { $$ = std::vector<std::unique_ptr<expression::Operation>>{}; $$.emplace_back(std::move($1)); }
	| attribute_list COMMA_TK aliased_attribute { $$ = std::move($1); $$.emplace_back(std::move($3)); }

aliased_attribute:
    attribute { $$ = std::move($1); }
    | attribute AS_TK REFERENCE { $$ = std::move($1); $$->alias(std::move($3)); }

attribute:
    REFERENCE { $$ = expression::OperationBuilder::make_attribute(std::move($1)); }
    | MUL_TK { $$ = expression::OperationBuilder::make_attribute("*"); }
    | REFERENCE DOT_TK REFERENCE { $$ = expression::OperationBuilder::make_attribute(std::move($1), std::move($3)); }
    | REFERENCE DOT_TK MUL_TK { $$ = expression::OperationBuilder::make_attribute(std::move($1), "*"); }
    | LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = std::move($2); }
    | attribute PLUS_TK attribute { $$ = expression::OperationBuilder::make_add(std::move($1), std::move($3)); }
    | attribute MINUS_TK attribute { $$ = expression::OperationBuilder::make_sub(std::move($1), std::move($3)); }
    | attribute MUL_TK attribute { $$ = expression::OperationBuilder::make_multiply(std::move($1), std::move($3)); }
    | attribute DIV_TK attribute { $$ = expression::OperationBuilder::make_divide(std::move($1), std::move($3)); }
    | SUM_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::OperationBuilder::make_sum(std::move($3)); }
    | COUNT_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::OperationBuilder::make_count(std::move($3)); }
    | AVG_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::OperationBuilder::make_avg(std::move($3)); }
    | MIN_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::OperationBuilder::make_min(std::move($3)); }
    | MAX_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::OperationBuilder::make_max(std::move($3)); }
    | CAST_TK LEFT_PARENTHESIS_TK attribute AS_TK type_description RIGHT_PARENTHESIS_TK { $$ = std::make_unique<expression::CastOperation>(std::move($3),$5); }
    | CASE_TK when_then_list else END_TK {
        $2.emplace_back(std::move($3));
        $$ = std::make_unique<expression::ListOperation>(expression::Operation::Id::Case, std::move($2));
    }
    | CASE_TK when_then_list END_TK {
        $$ = std::make_unique<expression::ListOperation>(expression::Operation::Id::Case, std::move($2));
    }
    | value { $$ = expression::OperationBuilder::make_value(std::move($1)); }
    | REFERENCE LEFT_PARENTHESIS_TK attribute_list RIGHT_PARENTHESIS_TK {
            $$ = expression::OperationBuilder::make_user_defined_function(std::move($1), std::move($3));
        }

operand:
    REFERENCE { $$ = expression::OperationBuilder::make_attribute(std::move($1)); }
    | REFERENCE DOT_TK REFERENCE { $$ = expression::OperationBuilder::make_attribute(std::move($1), std::move($3)); }
    | value { $$ = expression::OperationBuilder::make_value(std::move($1)); }
    | LEFT_PARENTHESIS_TK operand RIGHT_PARENTHESIS_TK { $$ = std::move($2); }
    | operand PLUS_TK operand { $$ = expression::OperationBuilder::make_add(std::move($1), std::move($3)); }
    | operand MINUS_TK operand { $$ = expression::OperationBuilder::make_sub(std::move($1), std::move($3)); }
    | operand MUL_TK operand { $$ = expression::OperationBuilder::make_multiply(std::move($1), std::move($3)); }
    | operand DIV_TK operand { $$ = expression::OperationBuilder::make_divide(std::move($1), std::move($3)); }

operation:
    operand EQUALS_TK operand { $$ = expression::OperationBuilder::make_eq(std::move($1), std::move($3)); }
    | operand LESSER_EQUALS_TK operand { $$ = expression::OperationBuilder::make_leq(std::move($1), std::move($3)); }
    | operand LESSER_TK operand { $$ = expression::OperationBuilder::make_lt(std::move($1), std::move($3)); }
    | operand GREATER_EQUALS_TK operand { $$ = expression::OperationBuilder::make_geq(std::move($1), std::move($3)); }
    | operand GREATER_TK operand { $$ = expression::OperationBuilder::make_gt(std::move($1), std::move($3)); }
    | operand NOT_EQUALS_TK operand { $$ = expression::OperationBuilder::make_neq(std::move($1), std::move($3)); }
    | operand LIKE_TK STRING { $$ = expression::OperationBuilder::make_like(std::move($1), std::move($3)); }
    | operand BETWEEN_TK operand AND_TK operand
        {   $$ = expression::OperationBuilder::make_between(std::move($1), std::move($3), std::move($5)); }
    | operand IN_TK LEFT_PARENTHESIS_TK operand_value_list RIGHT_PARENTHESIS_TK
        {   $$ = expression::OperationBuilder::make_in(
                std::move($1),
                std::make_unique<expression::NullaryListOperation>(std::move($4))
            );
        }
    | LEFT_PARENTHESIS_TK operation RIGHT_PARENTHESIS_TK { $$ = std::move($2); }
    | operation AND_TK operation { $$ = expression::OperationBuilder::make_and(std::move($1), std::move($3)); }
    | operation OR_TK operation { $$ = expression::OperationBuilder::make_or(std::move($1), std::move($3)); }
    | EXISTS_TK LEFT_PARENTHESIS_TK select_query RIGHT_PARENTHESIS_TK { $$ = expression::OperationBuilder::make_exists(std::move($3)); }

operand_value_list:
    value { $$ = std::vector<expression::Term>{}; $$.emplace_back(expression::Term{std::move($1)}); }
    | operand_value_list COMMA_TK value { $$ = std::move($1); $$.emplace_back(expression::Term{std::move($3)}); }

optional_where:
    WHERE_TK operation { $$ = std::move($2); }
    | { $$ = nullptr; }

optional_group_by:
    GROUP_BY_TK group_by_list { $$ = std::make_optional(std::move($2)); }
    | { $$ = std::nullopt; }

group_by_list:
	group_by_item { $$ = std::vector<expression::Term>{}; $$.emplace_back(std::move($1)); }
	| group_by_list COMMA_TK group_by_item { $$ = std::move($1); $$.emplace_back(std::move($3)); }

group_by_item:
    REFERENCE { $$ = expression::Term::make_attribute(std::move($1)); }
    | REFERENCE DOT_TK REFERENCE { $$ = expression::Term::make_attribute(std::move($1), std::move($3)); }

optional_limit:
    LIMIT_TK UNSIGNED_INTEGER { $$ = std::make_optional(expression::Limit{$2}); }
    | LIMIT_TK UNSIGNED_INTEGER OFFSET_TK UNSIGNED_INTEGER { $$ = std::make_optional(expression::Limit{$2, $4}); }
    | { $$ = std::nullopt; }

optional_order_by:
    ORDER_BY_TK order_by_item_list { $$ = std::make_optional(std::move($2)); }
    | { $$ = std::nullopt; }

order_by_item_list:
	order_by_item { $$ = std::vector<expression::OrderBy>{}; $$.emplace_back(std::move($1)); }
	| order_by_item_list COMMA_TK order_by_item { $$ = std::move($1); $$.emplace_back(std::move($3)); }

order_by_item:
    order_by_reference ASC_TK { $$ = expression::OrderBy{std::move($1), expression::OrderBy::Direction::ASC}; }
    | order_by_reference DESC_TK { $$ = expression::OrderBy{std::move($1), expression::OrderBy::Direction::DESC}; }
    | order_by_reference { $$ = expression::OrderBy{std::move($1)}; }

order_by_reference:
    REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1))); }
    | REFERENCE DOT_TK REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1), std::move($3))); }

when_then_list:
	when_then { $$ = std::vector<std::unique_ptr<expression::Operation>>{}; $$.emplace_back(std::move($1)); }
	| when_then_list COMMA_TK when_then { $$ = std::move($1); $$.emplace_back(std::move($3)); }

when_then:
    WHEN_TK operation THEN_TK attribute { $$ = std::make_unique<expression::BinaryOperation>(expression::Operation::Id::WhenThen, std::move($2), std::move($4)); }

else:
    ELSE_TK attribute { $$ = std::make_unique<expression::UnaryOperation>(expression::Operation::Id::Else, std::move($2)); }

/******************************
 * COMMANDS
 ******************************/
command:
    stop_command { $$ = std::move($1); }
    | show_tables_command { $$ = std::move($1); }
    | describe_table_command { $$ = std::move($1); }
    | load_file_command { $$ = std::move($1); }
    | import_csv_command { $$ = std::move($1); }
    | store_command { $$ = std::move($1); }
    | restore_command { $$ = std::move($1); }
    | get_configuration_command { $$ = std::move($1); }
    | set_cores_command { $$ = std::move($1); }
    | update_statistics_command { $$ = std::move($1); }

stop_command: DOT_TK STOP_TK { $$ = std::make_unique<StopCommand>(); }

show_tables_command: DOT_TK TABLES_TK { $$ = std::make_unique<ShowTablesCommand>(); }

describe_table_command:
    DOT_TK TABLE_TK REFERENCE { $$ = std::make_unique<DescribeTableCommand>(std::move($3)); }

load_file_command: DOT_TK LOAD_FILE_TK STRING { $$ = std::make_unique<LoadFileCommand>(std::move($3)); }

import_csv_command:
    DOT_TK IMPORT_CSV_TK STRING INTO_TK REFERENCE optional_separated_by
    {
        $$ = std::make_unique<CopyStatement>(std::move($5), std::move($3), std::move($6));
    }

get_configuration_command:
    DOT_TK CONFIGURATION_TK
    {
        $$ = std::make_unique<GetConfigurationCommand>();
    }

set_cores_command:
    DOT_TK SET_TK CORES_TK UNSIGNED_INTEGER
    {
        $$ = std::make_unique<SetCoresCommand>($4);
    }

update_statistics_command:
    DOT_TK UPDATE_STATISTICS_TK REFERENCE
    {
        $$ = std::make_unique<UpdateStatisticsCommand>(std::move($3));
    }

optional_separated_by:
    SEPARATED_BY_TK STRING { $$ = $2; }
    | { $$ = ","; }

store_command:
    DOT_TK STORE_TK STRING { $$ = std::make_unique<StoreCommand>(std::move($3)); }

restore_command:
    DOT_TK RESTORE_TK STRING { $$ = std::make_unique<RestoreCommand>(std::move($3)); }

%%

void db::parser::Parser::error(const location_type &/*type*/, const std::string& message)
{
    throw exception::ParserException(message);
}
