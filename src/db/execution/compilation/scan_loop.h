#pragma once
#include "context.h"
#include "scan_access_characteristic.h"
#include <array>
#include <cstdint>
#include <db/topology/physical_schema.h>
#include <flounder/program.h>
#include <flounder/statement.h>
#include <optional>
#include <string>
#include <utility>

namespace db::execution::compilation {
class RowScanLoop
{
public:
    RowScanLoop(flounder::Program &program, CompilationContext &context, flounder::Register begin_vreg,
                flounder::Register end_vreg, std::string &&source_name, const topology::PhysicalSchema &schema,
                bool dematerialize_record);
    ~RowScanLoop();

    [[nodiscard]] flounder::Register record_vreg() const noexcept { return _current_record_vreg; }

private:
    /// Context to (re)set scan end label.
    CompilationContext &_context;

    /// Virtual register pointing out the current record.
    flounder::Register _current_record_vreg;

    /// Virtual register pointing out the end.
    flounder::Register _tile_end_vreg;

    /// For loop to be opened after initializing vregs
    /// and to be closed in destructor.
    std::array<std::byte, sizeof(flounder::ForEach)> _for_loop;
};

class PaxScanLoop
{
public:
    PaxScanLoop(flounder::Program &program, CompilationContext &context, std::string &&source_name,
                const topology::PhysicalSchema &schema, bool dematerialize_record);
    ~PaxScanLoop();

    [[nodiscard]] flounder::Register tile_data_vreg() const noexcept { return _begin_data_vreg; }
    [[nodiscard]] flounder::Register row_index() noexcept
    {
        return reinterpret_cast<flounder::ForRange *>(_for_loop.data())->counter_vreg();
    }

private:
    /// Program to emit code.
    flounder::Program &_program;

    /// Context to (re)set scan end label.
    CompilationContext &_context;

    /// For loop to be opened after initializing vregs
    /// and to be closed in destructor.
    std::array<std::byte, sizeof(flounder::ForRange)> _for_loop;

    /// Vreg holding the base address where the pax records start.
    flounder::Register _begin_data_vreg;

    /// Vreg holding the number of records.
    flounder::Register _size_vreg;
};
} // namespace db::execution::compilation