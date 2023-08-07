#include "materialize_operator.h"
#include <db/execution/compilation/materializer.h>
#include <flounder/lib.h>
#include <flounder/statement.h>

using namespace db::execution::compilation;

void MaterializeOperator::produce(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    const auto is_materialize = this->is_materialize(phase);

    if (is_materialize)
    {
        const auto argument_id = phase == GenerationPhase::execution ? 2U : 0U;

        /// Load registers for (1) the graph context (to emit data),
        /// (2) the tile (from the graph context; to store data),
        /// (3) the tile capacity (from the tile),
        /// and (4) the tile size (from the tile; to realize when the tile is full and should emitted).
        /// (1) Graph context.
        this->_emitter_vreg = program.vreg("emitter");
        program.arguments() << program.request_vreg64(this->_emitter_vreg.value())
                            << program.get_argument(argument_id, this->_emitter_vreg.value());

        /// (2) Tile. The tile is a resource::ptr, we need to shift out the info (16bit).
        this->_tile_out_vreg = program.vreg("tile_out");
        this->_tile_out_size_vreg = program.vreg("tile_out_size");

        auto tile_address = program.mem(this->_emitter_vreg.value(), MaterializeEmitter::tile_offset());
        auto tile_size_address = program.mem(this->_tile_out_vreg.value(), data::PaxTile::size_offset());

        program.header()
            /// (2) Get the tile pointer and mask out the information.
            << program.request_vreg64(this->_tile_out_vreg.value())
            << program.mov(this->_tile_out_vreg.value(), tile_address)
            << program.and_(this->_tile_out_vreg.value(), program.constant64(0xFFFFFFFFFFFF))

            /// (3) Read the tile size from the tile pointed by tile_out.
            << program.request_vreg64(this->_tile_out_size_vreg.value())
            << program.mov(this->_tile_out_size_vreg.value(), tile_size_address);
    }

    this->child()->produce(phase, program, context);

    if (is_materialize)
    {
        auto context_guard = flounder::ContextGuard{program, "Materialize"};

        /// Emit the record set, if it contains any record.
        {
            auto if_tile_is_not_empty =
                flounder::If{program,
                             flounder::IsGreater{flounder::Operand{this->_tile_out_size_vreg.value()},
                                                 flounder::Operand{program.constant8(0)}},
                             "if_tile_is_not_empty"};

            /// Write the tile size.
            program << program.mov(program.mem(this->_tile_out_vreg.value(), data::PaxTile::size_offset()),
                                   this->_tile_out_size_vreg.value());

            /// Emit the data to the graph.
            flounder::FunctionCall(program, std::uintptr_t(&MaterializeEmitter::emit))
                .call({flounder::Operand{this->_emitter_vreg.value()}});
        }

        program << program.clear(this->_tile_out_size_vreg.value()) << program.clear(this->_tile_out_vreg.value())
                << program.clear(this->_emitter_vreg.value());
    }
}

void MaterializeOperator::consume(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    const auto is_materialize = this->is_materialize(phase);

    if (is_materialize)
    {
        auto context_guard = flounder::ContextGuard{program, "Materialize"};

        /// Materialize the record.
        PaxMaterializer::materialize(program, context.symbols(), this->_schema, this->_tile_out_vreg.value(),
                                     this->_tile_out_size_vreg.value());
        context.symbols().release(program, this->_schema.terms());

        /// Increase pointer for the next record and size.
        program << program.add(this->_tile_out_size_vreg.value(), program.constant8(1));

        /// Emit the record set, if it is full.
        {
            auto if_tile_is_full = flounder::If{
                program,
                flounder::IsGreaterEquals{flounder::Operand{this->_tile_out_size_vreg.value()},
                                          flounder::Operand{program.constant32(config::tuples_per_tile())}, false},
                "if_tile_is_full"};

            /// Write the tile size.
            program << program.mov(program.mem(this->_tile_out_vreg.value(), data::PaxTile::size_offset()),
                                   this->_tile_out_size_vreg.value());

            /// Emit the data to the graph.
            flounder::FunctionCall(program, std::uintptr_t(&MaterializeEmitter::emit), this->_tile_out_vreg.value())
                .call({flounder::Operand{this->_emitter_vreg.value()}});

            /// Reset the tile registers because emit() will create a new tile.
            program << program.xor_(this->_tile_out_size_vreg.value(), this->_tile_out_size_vreg.value());
        }
    }
}

void MaterializeOperator::request_symbols(const GenerationPhase phase, SymbolSet &symbols)
{
    /**
     * Materialization becomes active when
     *  a) The child has no finalization pass and we are in execution phase
     *  or b) The child has a finalization pass and we are in the finalization phase.
     */
    if (this->is_materialize(phase))
    {
        symbols.request(this->_schema.terms());
    }

    this->child()->request_symbols(phase, symbols);
}

std::unique_ptr<OutputProviderInterface> MaterializeOperator::output_provider(const GenerationPhase phase)
{
    if (this->is_materialize(phase))
    {
        return std::make_unique<TileOutputProvider>(mx::tasking::runtime::workers(),
                                                    topology::PhysicalSchema{this->_schema});
    }

    return this->child()->output_provider(phase);
}
