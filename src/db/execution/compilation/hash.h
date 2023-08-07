#pragma once

#include "materializer.h"
#include <db/type/type.h>
#include <flounder/program.h>
#include <flounder/statement.h>
#include <fmt/core.h>

namespace db::execution::compilation {
class SimpleHash
{
public:
    constexpr SimpleHash() noexcept = default;
    ~SimpleHash() noexcept = default;

    /**
     * Calculates a hash for the value in the given virtual register.
     * The value will not be overridden.
     *
     * @param program Program to emit code.
     * @param type Type of the value in the given virtual register.
     * @param value_vreg Virtual register holding the value to hash.
     * @return 64bit Virtual register containing the hash.
     */
    [[nodiscard]] flounder::Register emit(flounder::Program &program, type::Type type, flounder::Register value_vreg);

private:
    void emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);

    void emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);

    void emit_char(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg,
                   std::int32_t length);
};

class RadixHash
{
public:
    explicit RadixHash(const std::uint8_t num_bits) noexcept : _num_bits(num_bits) {}
    ~RadixHash() noexcept = default;

    /**
     * Calculates a hash for the value in the given virtual register.
     * The value will not be overridden.
     *
     * @param program Program to emit code.
     * @param type Type of the value in the given virtual register.
     * @param value_vreg Virtual register holding the value to hash.
     * @return 64bit Virtual register containing the hash.
     */
    [[nodiscard]] flounder::Register emit(flounder::Program &program, type::Type type, flounder::Register value_vreg);

private:
    const std::uint8_t _num_bits;
};

class MurmurHash
{
public:
    constexpr MurmurHash(const std::uint64_t seed) noexcept : _seed(seed) {}
    ~MurmurHash() noexcept = default;

    /**
     * Calculates a hash for the value in the given virtual register.
     * The value will not be overridden.
     *
     * @param program Program to emit code.
     * @param type Type of the value in the given virtual register.
     * @param value_vreg Virtual register holding the value to hash.
     * @return 64bit Virtual register containing the hash.
     */
    [[nodiscard]] flounder::Register emit(flounder::Program &program, type::Type type, flounder::Register value_vreg);

private:
    const std::uint64_t _seed;

    void emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);

    void emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);

    void emit_char(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg,
                   std::int32_t length);
};

class GoldenRatioHash
{
public:
    constexpr GoldenRatioHash() noexcept = default;
    ~GoldenRatioHash() noexcept = default;

    /**
     * Calculates a hash for the value in the given virtual register.
     * The value will not be overridden.
     *
     * @param program Program to emit code.
     * @param type Type of the value in the given virtual register.
     * @param value_vreg Virtual register holding the value to hash.
     * @return 64bit Virtual register containing the hash.
     */
    [[nodiscard]] flounder::Register emit(flounder::Program &program, type::Type type, flounder::Register value_vreg);

private:
    void emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);

    void emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);

    void emit_char(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg,
                   std::int32_t length);
};

class FNV1Hash
{
public:
    constexpr FNV1Hash() noexcept = default;
    ~FNV1Hash() noexcept = default;

    /**
     * Calculates a hash for the value in the given virtual register.
     * The value will not be overridden.
     *
     * @param program Program to emit code.
     * @param type Type of the value in the given virtual register.
     * @param value_vreg Virtual register holding the value to hash.
     * @return 64bit Virtual register containing the hash.
     */
    [[nodiscard]] flounder::Register emit(flounder::Program &program, type::Type type, flounder::Register value_vreg);

private:
    void emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);

    void emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);
};

class CRC32Hash
{
public:
    constexpr CRC32Hash() noexcept = default;
    ~CRC32Hash() noexcept = default;

    /**
     * Calculates a hash for the value in the given virtual register.
     * The value will not be overridden.
     *
     * @param program Program to emit code.
     * @param type Type of the value in the given virtual register.
     * @param value_vreg Virtual register holding the value to hash.
     * @return 64bit Virtual register containing the hash.
     */
    [[nodiscard]] flounder::Register emit(flounder::Program &program, type::Type type, flounder::Register value_vreg);

private:
    void emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);

    void emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg);
};

class HashCombine
{
public:
    static void emit(flounder::Program &program, flounder::Register vreg_a, flounder::Register vreg_b);
};
} // namespace db::execution::compilation