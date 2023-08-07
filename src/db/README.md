# Structure and Features of TunaDB

## Code Structure
### Parser
The parser can be found in the [parser](parser) folder. 
It is written using [flex](https://github.com/westes/flex) and [bison](https://www.gnu.org/software/bison/).
A given query is parsed, the tokens are transformed into [statements](parser/node) which will be consumed by the planner.

### Planner
The parser can be found in the [plan/logical](plan/logical) folder.
After parsing the query, the planner will create a canonical query plan and perform some required steps to preprocess the plan for the execution engine.

### Optimizer
The optimizer can be found in the [plan/optimizer](plan/optimizer) folder.
It implements a rule-based optimization based on a flexible and extensible framework, for example, evaluating constant arithmetics (`a.foo > 4+2`, `cast 42 as decimal(4,2)`), predicate pushdown, join re-ordering, and early projection.

### Execution
The execution engine can be found in the [execution](execution) folder which is divided into [interpretation](src/execution/interpretation) and [compilation](src/execution/compilation) engines.
Both engines use [MxTasking](../mx) as an execution framework instead of classic threads transforming the logical plan into a task-graph.
The task-graph can be found in the [plan/physical](plan/physical) folder.

### Catalog, Storage and Typesystem
The catalog can be found in the [topology](topology) folder providing table and schema (logical and physical) implementation.
Tuples are accessed using views and (constant) value implementations which can be found in [data](data).
Types are implemented in the [type](type) folder.
At the moment, TunaDB supports the following types:
* `CHAR` (of fixed lengths)
* `INT` (32 bit)
* `BIGINT` (64 bit)
* `DOUBLE` (with precision and scale)
* `DATE`

## Analyzing Query Execution
TunaDB has several options for explaining and profiling query execution, such as displaying query plans, generated query code, and performance counters during execution.

### Show the Logical Query Plan
    explain select * from <table> where <expression>

### Show the Task Graph
    explain task graph select * from <table> where <expression>

### Show Performance Counters and Execution statistics
    explain performance select * from <table> where <expression>

### Show the emitted FlounderIR
    explain flounder select * from <table> where <expression>

### Show the generated Assembly Code (from FlounderIR)
    explain asm select * from <table> where <expression>

## Statements
* `create table foo (id int, name char(30));`
* `insert into foo [(id, name)] values (1, 'Heinz') [,(2, 'Dieter')];`
* `copy foo from 'path/to/file.csv [(delimiter '|')];` (default delimiter is `;`)

## Commands

| Command                                                    | Description                                       |
|------------------------------------------------------------|---------------------------------------------------|
| `.tables`                                                  | List all tables                                   |
| `.table foo`                                               | Show schema of table `foo`                        |
| `.update statistics foo`                                   | Update statistics of table `foo`                   |
| `.load file 'path/to/file.sql'`                            | Execute all commands of the file `path/to/file.sql` |

## Boot
* The application `bin/tunadb` will start an in-memory database system and connect a command line client.
* To intially load some data, use the `--load` argument: `./bin/tunadb --load sql/load_sf01.sql`.
* For a full list of possible arguments see `./bin/tunadb -h`.
* Most notably arguments:
  * `--execute "<query>"` will execute the given query immediatly, could also be a file, i.e., `--execute sql/queries/tpch/q06.sql`, and can include `explain` keywords (e.g., `explain performance select ...`).
  * `--server-only` will start the server only.
  * `--client-only` will start the (command line) client only.

## Web Console
When using `--web-client` flag on startup, *tunadb* will start a web console at [http://0.0.0.0:9100](http://0.0.0.0:9100).