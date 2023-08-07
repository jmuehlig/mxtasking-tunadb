#pragma once
#include "expression_set.h"
#include "symbol_set.h"
#include <db/plan/physical/dataflow_graph.h>
#include <flounder/program.h>
#include <memory>
#include <mx/tasking/annotation.h>
#include <mx/tasking/dataflow/node.h>
#include <optional>
#include <vector>

namespace db::execution::compilation {
/**
 * Since different operators need to handle different outputs, each execution context will provide an instance
 * of an output provider. The output provider will provide an address for the flounder program which is the location
 * where records will be written to.
 *
 * Examples:
 *  -   The build side of the HashJoin operator needs to write tuple into a hash table.
 *      The HashJoin operator will create an instance of an output provider which returns
 *      the address to the hash table.
 *  -   The aggregate operator needs to store local aggregation results for each channel.
 *      Therefore, the aggregate operator creates an instance of an output provider that
 *      returns the local result for a specific channel.
 */
class OutputProviderInterface
{
public:
    constexpr OutputProviderInterface() noexcept = default;
    virtual ~OutputProviderInterface() = default;

    virtual std::uintptr_t get(std::uint16_t worker_id, std::optional<std::reference_wrapper<const RecordToken>> token,
                               mx::tasking::dataflow::EmitterInterface<execution::RecordSet> &graph,
                               mx::tasking::dataflow::NodeInterface<execution::RecordSet> *node) = 0;
};

class CompilationContext
{
public:
    CompilationContext() : _expression_set(_symbol_set) {}
    ~CompilationContext() noexcept = default;

    [[nodiscard]] const SymbolSet &symbols() const noexcept { return _symbol_set; }
    [[nodiscard]] SymbolSet &symbols() noexcept { return _symbol_set; }

    [[nodiscard]] const ExpressionSet &expressions() const noexcept { return _expression_set; }
    [[nodiscard]] ExpressionSet &expressions() noexcept { return _expression_set; }

    [[nodiscard]] flounder::Label label_next_record() const noexcept { return _label_next_record.value(); }
    void label_next_record(std::optional<flounder::Label> label) noexcept { _label_next_record = label; }

    [[nodiscard]] flounder::Label label_scan_end() const noexcept { return _label_scan_end.value(); }
    void label_scan_end(std::optional<flounder::Label> label) noexcept { _label_scan_end = label; }

private:
    SymbolSet _symbol_set;
    ExpressionSet _expression_set;
    std::optional<flounder::Label> _label_next_record{std::nullopt};
    std::optional<flounder::Label> _label_scan_end{std::nullopt};
};
} // namespace db::execution::compilation