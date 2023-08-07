#include "hash.h"
#include <db/exception/execution_exception.h>

using namespace db::execution::compilation;

void HashCombine::emit(flounder::Program &program, flounder::Register vreg_a, flounder::Register vreg_b)
{
    /// Used from Noisepage (https://github.com/cmu-db/noisepage/blob/master/src/include/common/hash_util.h#L164)

    /// static constexpr auto k_mul = uint64_t(0x9ddfea08eb382d69);
    //    auto k_mul = program.constant64(0x9ddfea08eb382d69);
    //    auto shifted_vreg = program.vreg("tmp_hashcombine_shift");
    //
    //    program
    //        /// hash_t first = (a ^ b) * k_mul;
    //        << program.xor_(vreg_a, vreg_b)
    //        << program.imul(vreg_a, k_mul)
    //
    //        /// first ^= (first >> 47u);
    //        << program.request_vreg64(shifted_vreg) << program.mov(shifted_vreg, vreg_a)
    //        << program.shr(shifted_vreg, program.constant8(47))
    //        << program.xor_(vreg_a, shifted_vreg)
    //
    //        /// hash_t b = (second_hash ^ a) * k_mul;
    //        << program.xor_(vreg_b, vreg_a)
    //        << program.imul(vreg_b, k_mul)
    //
    //        /// b ^= (b >> 47u);
    //        << program.mov(shifted_vreg, vreg_b) << program.shr(shifted_vreg, program.constant8(47))
    //        << program.xor_(vreg_b, shifted_vreg)
    //        << program.clear(shifted_vreg)
    //
    //        /// b *= k_mul;
    //        << program.imul(vreg_b, k_mul)
    //
    //        /// return b;
    //        << program.mov(vreg_a, vreg_b);

    /// Boost::hash_combine 64bit version (see
    /// https://stackoverflow.com/questions/5889238/why-is-xor-the-default-way-to-combine-hashes) lhs ^= rhs +
    /// 0x517cc1b727220a95 + (lhs << 6) + (lhs >> 2);
    auto lhs_shl_vreg = program.vreg("lhs_shl");
    auto lhs_shr_vreg = program.vreg("lhs_shr");
    program << program.request_vreg64(lhs_shl_vreg) << program.request_vreg64(lhs_shr_vreg)
            << program.mov(lhs_shl_vreg, vreg_a) << program.mov(lhs_shr_vreg, vreg_a)
            << program.shl(lhs_shl_vreg, program.constant8(6)) << program.shr(lhs_shr_vreg, program.constant8(2))
            << program.add(vreg_b, program.constant64(0x517cc1b727220a95)) << program.add(vreg_b, lhs_shl_vreg)
            << program.add(vreg_b, lhs_shr_vreg) << program.clear(lhs_shl_vreg) << program.clear(lhs_shr_vreg)
            << program.xor_(vreg_a, vreg_b);
}

flounder::Register SimpleHash::emit(flounder::Program &program, const type::Type type, flounder::Register value_vreg)
{
    auto hash_vreg = program.vreg(fmt::format("simple_hash_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(hash_vreg);

    switch (type.register_width())
    {
    case flounder::RegisterWidth::r8:
    case flounder::RegisterWidth::r16:
    case flounder::RegisterWidth::r32: {
        SimpleHash::emit32(program, hash_vreg, value_vreg);
        break;
    }
    case flounder::RegisterWidth::r64: {
        if (RowMaterializer::is_materialize_with_pointer(type) == false)
        {
            this->emit64(program, hash_vreg, value_vreg);
        }
        else
        {
            this->emit_char(program, hash_vreg, value_vreg, type.char_description().length());
        }
    }
    }
    return hash_vreg;
}

void SimpleHash::emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    /// According to:
    /// https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key

    /**
     * u32 hash(u32 x) {
     *   x = ((x >> 16) ^ x) * 0x45d9f3b;
     *   x = ((x >> 16) ^ x) * 0x45d9f3b;
     *   x = (x >> 16) ^ x;
     *   return x;
     * }
     */

    program << program.mov(hash_vreg, value_vreg);

    auto tmp = program.vreg(fmt::format("tmp_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(tmp)
            /// x = ((x >> 16) ^ x) * 0x45d9f3b;
            << program.mov(tmp, hash_vreg) << program.shr(hash_vreg, program.constant8(16))
            << program.xor_(hash_vreg, tmp)
            << program.imul(hash_vreg, program.constant32(0x45d9f3b))
            /// x = ((x >> 16) ^ x) * 0x45d9f3b;
            << program.mov(tmp, hash_vreg) << program.shr(hash_vreg, program.constant8(16))
            << program.xor_(hash_vreg, tmp)
            << program.imul(hash_vreg, program.constant32(0x45d9f3b))
            /// x = (x >> 16) ^ x
            << program.mov(tmp, hash_vreg) << program.shr(hash_vreg, program.constant8(16))
            << program.xor_(hash_vreg, tmp) << program.clear(tmp);
}

void SimpleHash::emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    /// According to:
    /// https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
    /**
     * u64 hash(u64 x) {
     *   x = (x ^ (x >> 30)) * u64(0xbf58476d1ce4e5b9);
     *   x = (x ^ (x >> 27)) * u64(0x94d049bb133111eb);
     *   x = x ^ (x >> 31);
     *   return x;
     * }
     */

    program << program.mov(hash_vreg, value_vreg);

    auto tmp = program.vreg(fmt::format("tmp_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(tmp)
            /// x = (x ^ (x >> 30)) * u64(0xbf58476d1ce4e5b9);
            << program.mov(tmp, value_vreg) << program.shr(tmp, program.constant8(30)) << program.xor_(hash_vreg, tmp)
            << program.imul(hash_vreg, program.constant64(0xbf58476d1ce4e5b9))
            /// x = (x ^ (x >> 27)) * u64(0x94d049bb133111eb);
            << program.mov(tmp, hash_vreg) << program.shr(tmp, program.constant8(27)) << program.xor_(hash_vreg, tmp)
            << program.imul(hash_vreg, program.constant64(0x94d049bb133111eb))
            /// x = x ^ (x >> 31);
            << program.mov(tmp, hash_vreg) << program.shr(tmp, program.constant8(31)) << program.xor_(hash_vreg, tmp)
            << program.clear(tmp);
}

void SimpleHash::emit_char(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg,
                           const std::int32_t length)
{
    // According to: https://cp-algorithms.com/string/string-hashing.html
    auto m_const = program.constant64(0x3b9aca09);
    program << program.xor_(hash_vreg, hash_vreg);

    auto p_pow = program.vreg("p_pow");
    program << program.request_vreg64(p_pow) << program.mov(p_pow, program.constant32(0xc9a0));

    {
        auto char_loop = flounder::ForRange{program, 0U, std::uint32_t(length),
                                            fmt::format("hash_loop_{}", value_vreg.virtual_name().value())};

        auto char_value = program.vreg(fmt::format("char_{}", value_vreg.virtual_name().value()));

        /// Load char.
        program << program.request_vreg64(char_value) << program.xor_(char_value, char_value)
                << program.mov(char_value,
                               program.mem(value_vreg, char_loop.counter_vreg(), flounder::RegisterWidth::r8));

        {
            auto if_is_end = flounder::If{
                program, flounder::IsEquals{flounder::Operand{char_value}, flounder::Operand{program.constant8(0)}},
                "if_string_ends"};
            program << program.jmp(char_loop.foot_label());
        }

        program << program.imul(char_value, p_pow) << program.add(hash_vreg, char_value) << program.imul(p_pow, p_pow)
                << program.clear(char_value);
    }

    program << program.clear(p_pow) << program.fmod(hash_vreg, m_const);
}

flounder::Register RadixHash::emit(flounder::Program &program, type::Type type, flounder::Register value_vreg)
{
    auto hash_vreg = program.vreg(fmt::format("radix_hash_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(hash_vreg);

    if (RowMaterializer::is_materialize_with_pointer(type) == false)
    {
        program << program.mov(hash_vreg, value_vreg) << program.shr(hash_vreg, program.constant8(this->_num_bits));
    }
    else
    {
        throw exception::NotImplementedException{"Hashing pointer with radix hash."};
    }

    return hash_vreg;
}

flounder::Register MurmurHash::emit(flounder::Program &program, const type::Type type, flounder::Register value_vreg)
{
    auto hash_vreg = program.vreg(fmt::format("murmur_hash_{}_{}", value_vreg.virtual_name().value(), this->_seed));
    program << program.request_vreg64(hash_vreg);

    switch (type.register_width())
    {
    case flounder::RegisterWidth::r8:
    case flounder::RegisterWidth::r16:
    case flounder::RegisterWidth::r32: {
        MurmurHash::emit32(program, hash_vreg, value_vreg);
        break;
    }
    case flounder::RegisterWidth::r64:
        if (RowMaterializer::is_materialize_with_pointer(type) == false)
        {
            MurmurHash::emit64(program, hash_vreg, value_vreg);
        }
        else
        {
            MurmurHash::emit_char(program, hash_vreg, value_vreg, type.char_description().length());
        }
        break;
    }
    return hash_vreg;
}

void MurmurHash::emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    /// https://en.wikipedia.org/wiki/MurmurHash
    auto tmp = program.vreg(fmt::format("tmp_{}", value_vreg.virtual_name().value()));
    program << program.mov(hash_vreg, value_vreg);
    if (this->_seed > 0U)
    {
        program << program.xor_(hash_vreg, program.constant32(std::int32_t(this->_seed)));
    }
    program << program.imul(hash_vreg, program.constant32(0xcc9e2d51)) << program.request_vreg64(tmp)
            << program.mov(tmp, hash_vreg) << program.shl(hash_vreg, program.constant8(15))
            << program.shr(tmp, program.constant8(17)) << program.or_(hash_vreg, tmp)
            << program.imul(hash_vreg, program.constant32(0x1b873593)) << program.clear(tmp);
}

void MurmurHash::emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    /// https://lemire.me/blog/2018/08/15/fast-strongly-universal-64-bit-hashing-everywhere/
    auto tmp = program.vreg(fmt::format("tmp_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(tmp) << program.mov(hash_vreg, value_vreg);

    if (this->_seed > 0U)
    {
        program << program.xor_(hash_vreg, program.constant64(this->_seed));
    }

    program << program.mov(tmp, value_vreg) << program.shr(tmp, program.constant8(33)) << program.xor_(hash_vreg, tmp)
            << program.imul(hash_vreg, program.constant64(0xff51afd7ed558ccd)) << program.xor_(hash_vreg, tmp)
            << program.imul(hash_vreg, program.constant64(0xc4ceb9fe1a85ec53)) << program.xor_(hash_vreg, tmp)
            << program.clear(tmp);
}

void MurmurHash::emit_char(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg,
                           const std::int32_t length)
{
    // According to: https://cp-algorithms.com/string/string-hashing.html
    auto m_const = program.constant64(0x3b9aca09 ^ this->_seed);
    program << program.xor_(hash_vreg, hash_vreg);

    auto p_pow = program.vreg("p_pow");
    program << program.request_vreg64(p_pow) << program.mov(p_pow, program.constant32(0xc9a0));

    {
        auto char_loop = flounder::ForRange{program, 0U, std::uint32_t(length),
                                            fmt::format("hash_loop_{}", value_vreg.virtual_name().value())};

        auto char_value = program.vreg(fmt::format("char_{}", value_vreg.virtual_name().value()));

        /// Load char.
        program << program.request_vreg64(char_value) << program.xor_(char_value, char_value)
                << program.mov(char_value,
                               program.mem(value_vreg, char_loop.counter_vreg(), flounder::RegisterWidth::r8));

        {
            auto if_is_end = flounder::If{
                program, flounder::IsEquals{flounder::Operand{char_value}, flounder::Operand{program.constant8(0)}},
                "if_string_ends"};
            program << program.jmp(char_loop.foot_label());
        }

        program << program.imul(char_value, p_pow) << program.add(hash_vreg, char_value) << program.imul(p_pow, p_pow)
                << program.clear(char_value);
    }

    program << program.clear(p_pow) << program.fmod(hash_vreg, m_const);
}

flounder::Register GoldenRatioHash::emit(flounder::Program &program, const type::Type type,
                                         flounder::Register value_vreg)
{
    auto hash_vreg = program.vreg(fmt::format("golden_ratio_hash_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(hash_vreg);

    switch (type.register_width())
    {
    case flounder::RegisterWidth::r8:
    case flounder::RegisterWidth::r16:
    case flounder::RegisterWidth::r32: {
        this->emit32(program, hash_vreg, value_vreg);
        break;
    }
    case flounder::RegisterWidth::r64:
        if (RowMaterializer::is_materialize_with_pointer(type) == false)
        {
            this->emit64(program, hash_vreg, value_vreg);
        }
        else
        {
            throw exception::NotImplementedException{"Char Hash for Golden Ratio Hash"};
        }
        break;
    }
    return hash_vreg;
}

void GoldenRatioHash::emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    auto upper_hash_vreg = program.vreg(fmt::format("upper_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(upper_hash_vreg) << program.xor_(hash_vreg, hash_vreg)
            << program.xor_(upper_hash_vreg, upper_hash_vreg) << program.mov(hash_vreg, value_vreg)
            << program.imul(hash_vreg, program.constant32(0x9e3779b9)) << program.xor_(upper_hash_vreg, hash_vreg)
            << program.mov(hash_vreg, value_vreg) << program.shr(hash_vreg, program.constant8(16))
            << program.imul(hash_vreg, program.constant32(0x9e3779b9)) << program.xor_(hash_vreg, upper_hash_vreg)
            << program.clear(upper_hash_vreg);
}

void GoldenRatioHash::emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    auto golden_constant_vreg = program.vreg(fmt::format("golden_constant_vreg_{}", value_vreg.virtual_name().value()));
    auto upper_hash_vreg = program.vreg(fmt::format("upper_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(golden_constant_vreg) << program.request_vreg64(upper_hash_vreg)
            << program.mov(golden_constant_vreg, program.constant64(0x9e3779b97f4a7c13))
            << program.xor_(hash_vreg, hash_vreg) << program.xor_(upper_hash_vreg, upper_hash_vreg)
            << program.mov(hash_vreg, value_vreg) << program.imul(hash_vreg, golden_constant_vreg)
            << program.xor_(upper_hash_vreg, hash_vreg) << program.mov(hash_vreg, value_vreg)
            << program.shr(hash_vreg, program.constant8(32)) << program.imul(hash_vreg, golden_constant_vreg)
            << program.xor_(hash_vreg, upper_hash_vreg) << program.clear(golden_constant_vreg)
            << program.clear(upper_hash_vreg);
}

flounder::Register FNV1Hash::emit(flounder::Program &program, type::Type type, flounder::Register value_vreg)
{
    auto hash_vreg = program.vreg(fmt::format("fnv1_hash_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(hash_vreg);

    switch (type.register_width())
    {
    case flounder::RegisterWidth::r8:
    case flounder::RegisterWidth::r16:
    case flounder::RegisterWidth::r32: {
        FNV1Hash::emit32(program, hash_vreg, value_vreg);
        break;
    }
    case flounder::RegisterWidth::r64: {
        FNV1Hash::emit64(program, hash_vreg, value_vreg);
        break;
    }
    }
    return hash_vreg;
}

void FNV1Hash::emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    program << program.mov(hash_vreg, program.constant32(0x811c9dc5));

    auto tmp = program.vreg(fmt::format("tmp_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(tmp) << program.xor_(tmp, tmp);
    for (auto i = 0U; i < sizeof(std::int32_t); ++i)
    {
        program << program.mov(tmp, value_vreg);
        if (i > 0U)
        {
            program << program.shr(tmp, program.constant8(i * 8U));
        }
        program << program.and_(tmp, program.constant8(std::numeric_limits<std::int8_t>::max()))
                << program.xor_(hash_vreg, tmp) << program.imul(hash_vreg, program.constant32(0x01000193));
    }
    program << program.clear(tmp);
}

void FNV1Hash::emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    program << program.mov(hash_vreg, program.constant64(0xcbf29ce484222325));

    auto tmp = program.vreg(fmt::format("tmp_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(tmp);
    for (auto i = 0U; i < sizeof(std::int64_t); ++i)
    {
        program << program.mov(tmp, value_vreg);
        if (i > 0U)
        {
            program << program.shr(tmp, program.constant8(i * 8U));
        }
        program << program.and_(tmp, program.constant8(std::numeric_limits<std::int8_t>::max()))
                << program.xor_(hash_vreg, tmp) << program.imul(hash_vreg, program.constant64(0x00000100000001B3));
    }
    program << program.clear(tmp);
}

flounder::Register CRC32Hash::emit(flounder::Program &program, type::Type type, flounder::Register value_vreg)
{
    /// See
    /// https://github.com/cmu-db/noisepage/blob/c2635d3360dd24a9f7a094b4b8bcd131d99f2d4b/src/include/common/hash_util.h

    auto hash_vreg = program.vreg(fmt::format("crc32_hash_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(hash_vreg);

    switch (type.register_width())
    {
    case flounder::RegisterWidth::r8:
    case flounder::RegisterWidth::r16:
    case flounder::RegisterWidth::r32: {
        CRC32Hash::emit32(program, hash_vreg, value_vreg);
        break;
    }
    case flounder::RegisterWidth::r64: {
        CRC32Hash::emit64(program, hash_vreg, value_vreg);
        break;
    }
    }
    return hash_vreg;
}

void CRC32Hash::emit32(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    auto value_as_64_vreg = program.vreg(fmt::format("tmp_32_64_{}", value_vreg.virtual_name().value()));
    program << program.request_vreg64(value_as_64_vreg) << program.mov(value_as_64_vreg, value_vreg);
    CRC32Hash::emit64(program, hash_vreg, value_as_64_vreg);
    program << program.clear(value_as_64_vreg);
}

void CRC32Hash::emit64(flounder::Program &program, flounder::Register hash_vreg, flounder::Register value_vreg)
{
    auto lower_vreg = program.vreg("crc32_hash_lower");
    program << program.request_vreg64(lower_vreg) << program.mov(lower_vreg, program.constant32(0xB56B4A9))
            << program.crc32(lower_vreg, value_vreg) << program.mov(hash_vreg, program.constant32(0x04c11db7))
            << program.crc32(hash_vreg, value_vreg) << program.shl(hash_vreg, program.constant8(32))
            << program.or_(hash_vreg, lower_vreg) << program.clear(lower_vreg)
            << program.imul(hash_vreg, program.constant64(0x2545f4914f6cdd1dULL));
}