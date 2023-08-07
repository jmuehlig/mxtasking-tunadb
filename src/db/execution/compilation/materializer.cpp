#include "materializer.h"
#include <db/data/pax_tile.h>
#include <flounder/lib.h>

using namespace db::execution::compilation;

bool Materializer::is_materialize_with_pointer(const type::Type type) noexcept
{
    /// Only chars need to be materialized with a pointer.
    if (type != type::Id::CHAR)
    {
        return false;
    }

    /// Chars of length 1, 2, 4, and 8 will fit into a register.
    const auto length = type.char_description().length();
    return (length == 1U || length == 2U || length == 4U) == false;
}

void RowMaterializer::materialize(flounder::Program &program, SymbolSet &symbols,
                                  const topology::PhysicalSchema &schema, flounder::Register record_address,
                                  const std::uint32_t offset)
{
    for (auto index = 0U; index < schema.size(); ++index)
    {
        const auto &term = schema.term(index);
        auto symbol_vreg = symbols.get(term);

        RowMaterializer::materialize(program, schema.type(index), offset + schema.row_offset(index), symbol_vreg,
                                     record_address);
    }
}

void RowMaterializer::materialize(flounder::Program &program, const topology::PhysicalSchema &schema,
                                  const expression::Term &term, flounder::Register value,
                                  flounder::Register record_address)
{
    const auto index = schema.index_include_alias(term);
    if (index.has_value())
    {
        RowMaterializer::materialize(program, schema.type(index.value()), schema.row_offset(index.value()), value,
                                     record_address);
    }
}

void RowMaterializer::materialize(flounder::Program &program, const type::Type type, const std::uint32_t offset,
                                  flounder::Register value, flounder::Register record_address)
{
    if (RowMaterializer::is_materialize_with_pointer(type))
    {
        /// Chars have to be copied since only the address is stored in the symbol reg.
        /// Use the record_address virtual register as the destination by temporary
        /// increasing to the char location.
        //        const auto has_offset = offset > 0U;
        //        if (has_offset)
        //        {
        //            program << program.add(record_address, program.constant32(offset));
        //        }

        /// Copy the char to the real attribute address.
        flounder::Lib::memcpy(program, record_address, offset, value, type.char_description().length());

        /// Reset the record_address vreg.
        //        if (has_offset)
        //        {
        //            program << program.sub(record_address, program.constant32(offset));
        //        }
    }
    else
    {
        const auto target_address = program.mem(record_address, offset, type.register_width());
        program << program.mov(target_address, value);
    }
}

void RowMaterializer::load(flounder::Program &program, SymbolSet &symbols, const topology::PhysicalSchema &schema,
                           flounder::Register record_address, const std::uint32_t offset)
{
    for (auto index = 0U; index < schema.size(); ++index)
    {
        const auto &term = schema.term(index);
        if (symbols.is_requested(term) && symbols.is_set(term) == false)
        {
            const auto type = schema.type(index);

            std::ignore =
                RowMaterializer::load(program, symbols, term, type, offset + schema.row_offset(index), record_address);
        }
    }
}

flounder::Register RowMaterializer::load(flounder::Program &program, SymbolSet &symbols, const expression::Term &term,
                                         const type::Type type, const std::uint16_t offset,
                                         flounder::Register record_address)
{
    /// Create and request a virtual register for the symbol.
    auto symbol_vreg = program.vreg(SymbolSet::make_vreg_name(term));
    auto register_width = type.register_width();
    const auto materialize_with_pointer = RowMaterializer::is_materialize_with_pointer(type);

    program << program.request_vreg(symbol_vreg, register_width);

    if (materialize_with_pointer)
    {
        /// For chars, only the address is stored in the register; not the value itself.
        program << program.lea(symbol_vreg, program.mem(record_address, offset));
    }
    else
    {
        /// Other values are loaded directly into the register.
        program << program.mov(symbol_vreg, program.mem(record_address, offset));
    }

    /// Load the virtual register containing the value into the symbol set.
    if (symbols.is_requested(term))
    {
        symbols.set(term, symbol_vreg);
    }

    return symbol_vreg;
}

std::optional<flounder::Register> RowMaterializer::load(flounder::Program &program, SymbolSet &symbols,
                                                        const expression::Term &term,
                                                        const topology::PhysicalSchema &schema,
                                                        flounder::Register record_address)
{
    const auto index = schema.index(term);
    if (index.has_value() == false)
    {
        return std::nullopt;
    }

    const auto type = schema.type(index.value());
    const auto offset = schema.row_offset(index.value());

    return RowMaterializer::load(program, symbols, term, type, offset, record_address);
}

void PaxMaterializer::load(flounder::Program &program, db::execution::compilation::SymbolSet &symbols,
                           const topology::PhysicalSchema &schema, flounder::Register tile_data_address,
                           flounder::Register row_index)
{
    for (auto index = 0U; index < schema.size(); ++index)
    {
        const auto &term = schema.term(index);
        PaxMaterializer::load(program, symbols, schema, term, tile_data_address, row_index);
    }
}

void PaxMaterializer::load(flounder::Program &program, db::execution::compilation::SymbolSet &symbols,
                           const topology::PhysicalSchema &schema, const expression::Term &term,
                           flounder::Register tile_data_address, flounder::Register row_index)
{
    if (symbols.is_requested(term) && symbols.is_set(term) == false)
    {
        const auto index = schema.index_include_alias(term);
        const auto type = schema.type(index.value());
        const auto pax_offset = schema.pax_offset(index.value());

        std::ignore = PaxMaterializer::load(program, symbols, term, type, pax_offset, tile_data_address, row_index);
    }
}

flounder::Register PaxMaterializer::load(flounder::Program &program, db::execution::compilation::SymbolSet &symbols,
                                         const expression::Term &term, type::Type type, std::uint64_t offset,
                                         flounder::Register tile_data_address, flounder::Register row_index)
{
    /// Create and request a virtual register for the symbol.
    auto symbol_vreg = program.vreg(SymbolSet::make_vreg_name(term));
    auto register_width = type.register_width();
    const auto materialize_with_pointer = PaxMaterializer::is_materialize_with_pointer(type);

    program << program.request_vreg(symbol_vreg, register_width);

    if (materialize_with_pointer)
    {
        /// For chars, only the address is stored in the register; not the value itself.
        /// addr = tile_address + offset + (row_index * size)
        program << program.mov(symbol_vreg, row_index) << program.imul(symbol_vreg, program.constant32(type.size()))
                << program.lea(symbol_vreg, program.mem(symbol_vreg, tile_data_address, offset));
    }
    else
    {
        /// Other values are loaded directly into the register.
        /// [base_vreg + type.size() * row_index_vreg + pax_offset]
        auto source = program.mem(tile_data_address, row_index, type.size(), offset);
        program << program.mov(symbol_vreg, source);
    }

    /// Load the virtual register containing the value into the symbol set.
    if (symbols.is_requested(term))
    {
        symbols.set(term, symbol_vreg);
    }

    return symbol_vreg;
}

void PaxMaterializer::materialize(flounder::Program &program, db::execution::compilation::SymbolSet &symbols,
                                  const topology::PhysicalSchema &schema, flounder::Register tile_address,
                                  flounder::Register record_index)
{
    for (auto index = 0U; index < schema.size(); ++index)
    {
        const auto &term = schema.term(index);
        auto symbol_vreg = symbols.get(term);

        PaxMaterializer::materialize(program, schema.type(index), schema.pax_offset(index), symbol_vreg, tile_address,
                                     record_index);
    }
}

void PaxMaterializer::materialize(flounder::Program &program, type::Type type, std::uint16_t offset,
                                  flounder::Register value, flounder::Register tile_address,
                                  flounder::Register row_index)
{
    if (RowMaterializer::is_materialize_with_pointer(type))
    {
        auto target_address = program.vreg("pax_tile_pointer_out");
        program << program.request_vreg64(target_address) << program.mov(target_address, row_index)
                << program.imul(target_address, program.constant32(type.size()))
                << program.add(target_address, program.constant32(offset + sizeof(data::PaxTile)))
                << program.add(target_address, tile_address);

        /// Copy the char to the real attribute address.
        flounder::Lib::memcpy(program, target_address, value, type.char_description().length());

        program << program.clear(target_address);
    }
    else
    {
        auto target_address = program.mem(tile_address, row_index, type.size(), offset + sizeof(data::PaxTile));
        program << program.mov(target_address, value);
    }
}