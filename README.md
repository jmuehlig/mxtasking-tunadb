# TunaDB: A task-based in-memory research DBMS
Tunadb is an research in-memory Database Management System aimed to enhance query compilation, job processing, and data prefetching. 
Through the integration of creative approaches and novel models, this project seeks to create a high-performance and efficient database solution.

**Note**: This is a research project. While TunaDB demonstrates promising capabilities, it is unsuitable for production use. 

## Key Features and Implemented Concepts
### Task-based Architecture
TunaDB adopts our [MxTask](https://github.com/jmuehlig/mxtasking)-based approach to manage query execution efficiently. 
By breaking down large queries into smaller tasks, it optimizes resource efficiency and task parallelism, enabling fast data processing.

### Query Compilation
TunaDB's query compilation process is streamlined by using [FlounderIR](https://github.com/Henning1/resql) as a lightweight intermediate representation to comile queries into executable assembly code. 
It facilitates the translation of high-level inquiries to low-level execution activities by simplifying the query structure.
The shipped implementation of FlounderIR improves on the original implementation by some optimizations (e.g., better register assignment and branch relocation).

### Profiling and Performance Analysis
TunaDB provides powerful profiling features for in-depth performance investigation. 
It supports inlined perf-counter profiling, as well as perf sampling of memory addresses and instructions, offering useful insights into system behavior and identifying potential bottlenecks.

Furthermore, developers can benefit from the seamless integration of third-party applications like [Intel® VTune™](https://www.intel.com/content/www/us/en/developer/tools/oneapi/vtune-profiler.html) and perf.
TunaDB will make compiled query code available for such tools.
This allows for a comprehensive inspection of compiled code, enabling detailed performance evaluations and optimizations to unlock the DBMS's full potential.

## Build Instructions
### Dependencies
Please install the following dependencies
* `cmake` `>= 3.10`
* `clang` `>= 13` (`gcc` is not tested)
* `clang-tidy` `>= 13`
* `libnuma` or `libnuma-dev`
* `bison` 
* `flex`
* `libgtest-dev` for tests in `test/` (optional)

### Building
#### Step 1: Clone the repository

    git clone https://github.com/jmuehlig/mxtasking-tunadb.git

#### Step 2: Generate the `Makefile` using `cmake`

    cmake . -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-15 -DCMAKE_CXX_COMPILER=clang++-15 

#### Step 3: Build `TunaDB`

    make tunadb -j4

The `tunadb` binary will be located in `bin/`.

## Basic Usage
### Starting TunaDB
Calling the binary `./bin/tunadb` will start both the server and a client in a single process.
You can now create tables, insert data, and execute queries, using the client's console.

### Using the web client
TunaDB will start an additional web client that is available for convenient use when the `--web-client` switch is added (`./bin/tunadb --web-client`).
The web client allows to execute queries, show query plans (both logical and physical), show generated FlounderIR and assembly code and profile the execution.
After startup, the web console is available under

    http://0.0.0.0:9100

### Loading initial data
TunaDB can execute one SQL file to initially load data before starting the server. 
Use the `--load <file.sql>` option. 
The given SQL file may 
* create tables (`CREATE TABLE <table> (...)`), 
* copy data from (CSV) files (`COPY <table> FROM '<file>'`), 
* execute further SQL files (`.LOAD FILE '<file.sql>'`),
* and/or update statistics (`.UPDATE STATISTICS <table>`).

#### Loading TPC-H data
If you want to bring the data of the TPC-H benchmark into TunaDB:
* Create a folder `sql/data/tpch`
* Generate all _.tbl_ files and move them into `sql/data/tpch`
* Load the SQL script `sql/load_tpch.sql`:
        
      `./bin/tunadb --load sql/load_tpch.sql`

### Further Commands
See `./bin/tunadb --help` for further options and flags.
More information about the code structure and implemented commands, data types, etc. are given in [src/db/README.md](src/db/README.md).

## Related Publications
* Jan Mühlig, Jens Teubner. *Micro Partitioning: Friendly to the Hardware and the Developer*. DaMoN 2023: 27-34. [Read the Paper](https://doi.org/10.1145/3592980.3595310)
* Henning Funke, Jan Mühlig, Jens Teubner. *Low-latency query compilation*. VLDB J. 31(6): 1171-1184 (2022). [Read the Paper](https://doi.org/10.1007/s00778-022-00741-5) | [See the original Source Code](https://github.com/Henning1/resql)
* Jan Mühlig, Jens Teubner. *MxTasks: How to Make Efficient Synchronization and Prefetching Easy*. SIGMOD Conference 2021: 1331-1344. [Read the Paper](https://doi.org/10.1145/3448016.3457268) | [See the original Source Code](https://github.com/jmuehlig/mxtasking)
* Henning Funke, Jan Mühlig, Jens Teubner. *Efficient generation of machine code for query compilers*. DaMoN 2020: 6:1-6:7. [Read the Paper](https://doi.org/10.1145/3399666.3399925)

## Code Structure
The code is separated in four different branches:
* [`src/application`](src/application) contains stuff of MxTask-based applications ([TunaDB](src/application/tunadb) is one of them). For guidance: Every application should be hold in a separated folder and end up in at least one binary (stored in `bin/`).
* [`src/db`](src/db) contains database-related implementations, such as indices, types, execution engine, etc..
* [`src/mx`](src/mx) includes all stuff for the task-based abstraction `MxTasking`.
* [`src/flounder`](src/flounder) includes the low-latency IR, used for jit compiling operators.
* [`src/perf`](src/flounder) includes an implementation of in-source perf counter and sampling.

## Further Applications
Besides TunaDB, this repository includes further task-based applications used for papers or development.

### B-link-Tree Benchmark
The folder [`src/application/blinktree_benchmark`](src/application/blinktree_benchmark) contains the benchmark code used in our paper _MxTasks: How to Make Efficient Synchronization and Prefetching Easy_.

### Radix Join Benchmark
The folder [`src/application/radix_join_benchmark`](src/application/radix_join_benchmark) contains the benchmark code used in our paper _Micro Partitioning: Friendly to the Hardware and the Developer_.

### Task-based "Hello World"
The folder [`src/application/hello_world`](src/application/hello_world) contains a task-based example for creating and spawning a simple task.

## External Libraries
TunaDB would not be possible without the help of various external libraries.
The used libraries will be downloaded automatically (using `git`) during the build process.
Special thanks to:
* `argparse` ([view on GitHub](https://github.com/p-ranav/argparse)) under MIT license
* `nlohmann json` ([view on GitHub](https://github.com/nlohmann/json)) under MIT license
* `linenoise` ([view on GitHub](https://github.com/antirez/linenoise)) under BSD-2 license
* `cpp-httplib` ([view on GitHub](https://github.com/yhirose/cpp-httplib)) under MIT license
* `asmjit` ([view on GitHub](https://github.com/asmjit/asmjit)) under Zlib license
* `{fmt}` ([view on GitHub](https://github.com/fmtlib/fmt)) under MIT license
* `spdlog` ([view on GitHub](https://github.com/gabime/spdlog)) under MIT license
* `static_vector` ([view on GitHub](https://github.com/jmacheta/static_vector)) under MIT license
* `robin-map` ([view on GitHub](https://github.com/Tessil/robin-map.git)) under MIT license
* `libcount` ([view on GitHub](https://github.com/dialtr/libcount)) under Apache-2.0 license
* `xxhashct` ([view on GitHub](https://github.com/ekpyron/xxhashct)) _published without license_
* `ittapi` ([view on GitHub](https://github.com/intel/ittapi.git)) under GPLv2 and 3-Clause BSD licenses

## Contact
If you have any questions or comments, feel free to contact via mail: [jan.muehlig@tu-dortmund.de](mailto:jan.muehlig@tu-dortmund.de).
