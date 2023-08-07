#pragma once

#include "symbol_set.h"
#include <db/topology/physical_schema.h>
#include <flounder/program.h>
#include <functional>
#include <optional>

namespace db::execution::compilation {
class Materializer
{
public:
    constexpr Materializer() noexcept = default;
    virtual ~Materializer() noexcept = default;

    /**
     * Checks if materialization of a specific type should be done using pointers.
     * This is the case for CHAR types that do not fit into a register.
     *
     * @param type Type to check.
     * @return True, if materialization should be handled by using a pointer.
     */
    [[nodiscard]] static bool is_materialize_with_pointer(type::Type type) noexcept;
};

class RowMaterializer final : public Materializer
{
public:
    constexpr RowMaterializer() noexcept = default;
    ~RowMaterializer() noexcept override = default;

    /**
     * Materializes all values that are available in
     * the symbol set and requested by the schema to
     * the given record.
     *
     * @param program Program to allocate instruction nodes.
     * @param symbols List of symbols containing the virtual registers where the values are loaded.
     * @param schema Schema of the record that should be materialized.
     * @param record_address Address where the values are written to.
     * @param offset Offset to the record address.
     */
    static void materialize(flounder::Program &program, SymbolSet &symbols, const topology::PhysicalSchema &schema,
                            flounder::Register record_address, std::uint32_t offset);

    static void materialize(flounder::Program &program, const topology::PhysicalSchema &schema,
                            const expression::Term &term, flounder::Register value, flounder::Register record_address);

    /**
     * Dematerializes all values that are included in
     * the given schema and requested by the given symbols.
     * The virtual registers containing the values are stored
     * in the symbol set.
     *
     * @param program Program to allocate instruction nodes.
     * @param symbols Symbol set to load requested terms and store dematerialized virtual registers.
     * @param schema Schema of the given record.
     * @param record_address Virtual register containing the address of the record that should be dematerialized.
     * @param offset Offset of the record address.
     */
    static void load(flounder::Program &program, SymbolSet &symbols, const topology::PhysicalSchema &schema,
                     flounder::Register record_address, const std::uint32_t offset);

    /**
     * Dematerializes the given term.
     * If the term is requested by the given symbols, the virtual register
     * will also be loaded to the symbol set.
     *
     * @param program Program to allocate instruction nodes.
     * @param symbols Symbol set to load requested terms and store dematerialized virtual registers.
     * @param term Term that should be dematerialized.
     * @param schema Schema of the record.
     * @param record_address Virtual register containing the address of the record that should be dematerialized.
     * @return Virtual register holding the value.
     */
    static std::optional<flounder::Register> load(flounder::Program &program, SymbolSet &symbols,
                                                  const expression::Term &term, const topology::PhysicalSchema &schema,
                                                  flounder::Register record_address);

    /**
     * Creates a memory access to access the the value stored at the given index in the given schema.
     *
     * @param program Program to allocate nodes.
     * @param record_address Record to access.
     * @param schema Schema of the record.
     * @param index Index within the schema.
     * @return Memory address to access the value.
     */
    static flounder::MemoryAddress access(flounder::Program &program, flounder::Register record_address,
                                          const topology::PhysicalSchema &schema, const std::uint16_t index)
    {
        return access(program, record_address, 0U, schema, index);
    }

    /**
     * Creates a memory access to access the the value stored at the given index in the given schema.
     *
     * @param program Program to allocate nodes.
     * @param record_address Record to access.
     * @param offset Offset to the record address.
     * @param schema Schema of the record.
     * @param index Index within the schema.
     * @return Memory address to access the value.
     */
    static flounder::MemoryAddress access(flounder::Program &program, flounder::Register record_address,
                                          const std::uint32_t offset, const topology::PhysicalSchema &schema,
                                          std::uint16_t index)
    {
        return program.mem(record_address, offset + schema.row_offset(index), schema.type(index).register_width());
    }

private:
    static void materialize(flounder::Program &program, type::Type type, std::uint32_t offset, flounder::Register value,
                            flounder::Register record_address);

    /**
     * Dematerializes the given term.
     * If the term is requested by the given symbols, the virtual register
     * will also be loaded to the symbol set.
     *
     * @param program Program to allocate instruction nodes.
     * @param symbols Symbol set to load requested terms and store dematerialized virtual registers.
     * @param term Term that should be dematerialized.
     * @param type Type of the value.
     * @param offset Offset of the value within the records schema.
     * @param record_address Virtual register containing the address of the record that should be dematerialized.
     * @return Virtual register holding the value.
     */
    static flounder::Register load(flounder::Program &program, SymbolSet &symbols, const expression::Term &term,
                                   type::Type type, std::uint16_t offset, flounder::Register record_address);
};

class PaxMaterializer final : public Materializer
{
public:
    constexpr PaxMaterializer() noexcept = default;
    ~PaxMaterializer() noexcept override = default;

    /**
     * Materializes all values that are available in
     * the symbol set and requested by the schema to
     * the given record.
     *
     * @param program Program to allocate instruction nodes.
     * @param symbols List of symbols containing the virtual registers where the values are loaded.
     * @param schema Schema of the record that should be materialized.
     * @param tile_address Address of the tile where the data is written to.
     * @param record_index Index of the record to write.
     */
    static void materialize(flounder::Program &program, SymbolSet &symbols, const topology::PhysicalSchema &schema,
                            flounder::Register tile_address, flounder::Register record_index);

    /**
     * Dematerializes all values that are included in
     * the given schema and requested by the given symbols.
     * The virtual registers containing the values are stored
     * in the symbol set.
     *
     * @param program Program to allocate instruction nodes.
     * @param symbols Symbol set to load requested terms and store dematerialized virtual registers.
     * @param schema Schema of the given record.
     * @param tile_data_address Virtual register containing the address of the tile containing multiple rows organized
     * in a PAX schema.
     * @param row_index Virtual register containing the index of the row that should be dematerialized.
     */
    static void load(flounder::Program &program, SymbolSet &symbols, const topology::PhysicalSchema &schema,
                     flounder::Register tile_data_address, flounder::Register row_index);

    static void load(flounder::Program &program, SymbolSet &symbols, const topology::PhysicalSchema &schema,
                     const expression::Term &term, flounder::Register tile_data_address, flounder::Register row_index);

    /**
     * Dematerializes the given term.
     * If the term is requested by the given symbols, the virtual register
     * will also be loaded to the symbol set.
     *
     * @param program Program to allocate instruction nodes.
     * @param symbols Symbol set to load requested terms and store dematerialized virtual registers.
     * @param term Term that should be dematerialized.
     * @param type Type of the value.
     * @param offset Offset of the column within the tile.
     * @param tile_data_address Virtual register containing the address of the tile containing multiple rows organized
     * in a PAX schema.
     * @param row_index Virtual register containing the index of the row that should be dematerialized.
     * @return Virtual register holding the value.
     */
    static flounder::Register load(flounder::Program &program, SymbolSet &symbols, const expression::Term &term,
                                   type::Type type, std::uint64_t offset, flounder::Register tile_data_address,
                                   flounder::Register row_index);

private:
    /**
     * Materializes a single value to the tile.
     *
     * @param program Program to allocate instruction nodes.
     * @param type Type of the value to materialize.
     * @param offset Offset of the column.
     * @param value Register holding the value.
     * @param tile_address Address of the tile.
     * @param row_index Index of the row to write.
     */
    static void materialize(flounder::Program &program, type::Type type, std::uint16_t offset, flounder::Register value,
                            flounder::Register tile_address, flounder::Register row_index);
};
} // namespace db::execution::compilation