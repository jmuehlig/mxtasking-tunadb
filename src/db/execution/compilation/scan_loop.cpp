#include "scan_loop.h"
#include "materializer.h"
#include <flounder/statement.h>

using namespace db::execution::compilation;

RowScanLoop::RowScanLoop(flounder::Program &program, CompilationContext &context, flounder::Register begin_vreg,
                         flounder::Register end_vreg, std::string &&source_name, const topology::PhysicalSchema &schema,
                         const bool dematerialize_record)
    : _context(context), _current_record_vreg(begin_vreg), _tile_end_vreg(end_vreg)
{
    const auto record_size = schema.row_size();

    /// Iterate over records.
    {
        auto *for_loop = new (this->_for_loop.data())
            flounder::ForEach{program, this->_current_record_vreg, this->_tile_end_vreg, record_size,
                              fmt::format("row_scan_{}_loop", std::move(source_name))};

        /// Label to jump to the next tuple iteration.
        context.label_next_record(for_loop->step_label());
        context.label_scan_end(for_loop->foot_label());

        if (dematerialize_record)
        {
            /// Load all requested symbols into registers.
            RowMaterializer::load(program, context.symbols(), schema, this->_current_record_vreg, 0U);
        }
    }
}

RowScanLoop::~RowScanLoop()
{
    this->_context.label_scan_end(std::nullopt);
    this->_context.label_next_record(std::nullopt);

    reinterpret_cast<flounder::ForEach *>(this->_for_loop.data())->~ForEach();
}

PaxScanLoop::PaxScanLoop(flounder::Program &program, db::execution::compilation::CompilationContext &context,
                         std::string &&source_name, const topology::PhysicalSchema &schema, bool dematerialize_record)
    : _program(program), _context(context), _begin_data_vreg(program.vreg(fmt::format("{}_tile", source_name))),
      _size_vreg(program.vreg(fmt::format("{}_tile_size", source_name)))
{
    program.arguments() << program.request_vreg64(this->_begin_data_vreg) << program.get_arg0(this->_begin_data_vreg)
                        << program.request_vreg64(this->_size_vreg) << program.get_arg1(this->_size_vreg);

    {
        auto *for_loop = new (this->_for_loop.data()) flounder::ForRange{
            program, 0U, flounder::Operand{this->_size_vreg}, fmt::format("pax_scan_{}_loop", std::move(source_name))};

        /// Label to jump to the next tuple iteration.
        context.label_next_record(for_loop->step_label());
        context.label_scan_end(for_loop->foot_label());

        if (dematerialize_record)
        {
            /// Load all requested symbols into registers.
            PaxMaterializer::load(program, context.symbols(), schema, this->_begin_data_vreg, for_loop->counter_vreg());
        }
    }
}

PaxScanLoop::~PaxScanLoop()
{
    this->_context.label_scan_end(std::nullopt);
    this->_context.label_next_record(std::nullopt);

    reinterpret_cast<flounder::ForRange *>(this->_for_loop.data())->~ForRange();

    this->_program << this->_program.clear(this->_begin_data_vreg) << this->_program.clear(this->_size_vreg);
}