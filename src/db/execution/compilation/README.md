## Interface for Code-Generating Operators
| Method                | Direction         | Description                                                                                                                                                                                                                                                                           |
|-----------------------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `finalization_data()` | `parent -> child` | Some operators need to finalize the node (i.e., emitting the last tile, aggregating sub-partitions etc.). If so, the operator returns the finalization type (single, parallel, reduce) and the data items to finalize (i.e., multiple hash tables). Otherwhise `nullopt` is returned. |
| `request_symbols()`   | `parent -> child` | Insert symbol requests (attributes, calculated results like `id * 7`,...).                                                                                                                                                                                                            |
| `produce()`           | `parent -> child` | Starts code generation. Most operatos will call `produce()` of its child. `Scan` will generate a loop over tuples and call the parents `consume()` for each tuple.                                                                                                                    |
| `consume()`           | `child -> parent` | Generates code consuming individual tuples.                                                                                                                                                                                                                                           |
| `input_data()`        | `parent -> child` | Returns the (persisted) data that will be processed when starting the graph.                                                                                                                                                                                                          |
| `output_provider()`   | `parent -> child` | Sets the output provider for execution and finalization phase. Materialization and Limit operators have to see their children results and set its output providers based on that.                                                                                                     |
| `dependencies()`      | `parent -> child` | Returns an `optional` context with the succeeding operator (when the pipeline is broken) and a list of dependent operators (i.e., build before probe).                                                                                                                                |                                                                                                                                                                                  

## Implemented Operators that support Code Generation
| Operator                | Description                                                                                             | Supports Finalization | Requires Finalization                                     | Output Provider                | Supports Prefetching |
|-------------------------|---------------------------------------------------------------------------------------------------------|-----------------------|-----------------------------------------------------------|--------------------------------|----------------------|
| Scan                    | Creates a loop over all tuples from the given RowTile. Requested symbols are loaded into virtual register. | No                    | No                                                        | -                              | Yes                  |
| Selection               | Filters all tuples and applies `consume()` for all tuples passing the filters.                          | No                    | No                                                        | -                              | No                   |
| Arithmetic              | Applies arithmetic for requested during `consume()`.                                                    | No                    | No                                                        | -                              | No                   |
| Aggregation             | Aggregates all tuples during `consume()`.                                                               | -                     | Yes (averaging results, merging, and emitting aggregates). | Local results per core         | No                   |
| Grouped Aggregation     | Aggregates all tuples during `consume()` using a hash table for groups.                                 | -                     | Yes (averaging results, merging, and emitting aggregates). | Local results per core         | No                   |
| Radix Group Aggregation | Consumes all tuples and inserts into a partition-local hash table. The hash tables are emitted during finalization | - | Yes (emitting all tuples from the hash table).                 | RowTile | Yes                  |
| Partition               | Maps each tuple to a specific task squad (=partition).  | No             | -                     | No                             | Yes, if not first    |
| Materialize Partition   | Mazterializes each tuple to a partition-tile. Emits all full and the last RowTile to the graph.            | No             | Yes (emitting the last RowTile)                              | RowTile per Input per Output core | No                   |
| Radix Join Build        | Consumes all tuples and inserts the into a partition-local hash table.                                  | No                    | No                                                        | SimpleHash Table per core      | Yes                  |
| Radix Join Probe        | Consumes all tuples and probes the (built) hash tablem                                                  | No                    | No                                                        | -                              | Yes                  |
| Limit                   | Applies `consume()` for all tuples passing the limit and offset filters.                                | Yes                   | No                                                        | -                              | No                   |
| Materialization         | Materializes (and emits) all tuples during `consume()`.                                                 | Yes                   | No                                                        | RowTile                           | No                   |