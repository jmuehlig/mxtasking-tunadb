#include <cassert>
#include <flounder/exception.h>
#include <flounder/statement.h>
#include <flounder/string.h>

using namespace flounder;

Register String::is_equals(Program &program, std::string &&name, Descriptor left, Descriptor right)
{
    /// One of the both strings should be a pointer.
    /// Otherwise, it can (and should!) be compared by
    /// "normal" register compare.
    /// Strings with 1,2, or 4 byte are inlined, i.e.
    /// CHAR(4) == "ABCD" -> 32bit reg == 32bit reg
    assert(left.is_pointer() || right.is_pointer());

    /// One of both strings could be a pointer
    /// while the other is an inlined value. In this case,
    /// We can load the pointer into a register and compare
    /// to the inlined value, i.e.
    /// CHAR(10) == "ABCD" -> mov 32bit CHAR(10) into reg, reg == 32bit reg["ABCD"]
    if (left.is_pointer() ^ right.is_pointer())
    {
        return String::is_equals_inlined_and_pointer(program, std::move(name), left, right);
    }

    /// Strings stored in the database my be variable size,
    /// the size is only a max-value (i.e., CHAR(10) = "ABCD\0\0\0...").
    /// When comparing with fixed values, i.e., for selection, we can compare
    /// with bride registers.
    if (left.is_fixed_size() || right.is_fixed_size())
    {
        if (left.is_fixed_size() && right.is_fixed_size() == false)
        {
            return String::is_equals_fixed_and_variable_size(program, std::move(name), left, right);
        }

        if (right.is_fixed_size() && left.is_fixed_size() == false)
        {
            return String::is_equals_fixed_and_variable_size(program, std::move(name), right, left);
        }

        /// If both are fixed size, this is not implemented.
        assert(false);
        return Register{0U, RegisterWidth::r64};
    }

    /// Compare to pointer strings byte by byte.
    const auto min_size = std::min(left.size(), right.size());

    auto result_vreg = program.vreg(fmt::format("streq_{}_result", name));

    auto unequals_label = program.label(fmt::format("streq_{}_ne", name));
    auto equals_label = program.label(fmt::format("streq_{}_eq", name));
    auto end_label = program.label(fmt::format("streq_{}_end", name));

    program << program.request_vreg8(result_vreg);
    {
        auto loop = ForRange{program, 0U, min_size};

        auto left_operand =
            left.data().is_reg()
                ? program.mem(left.data().reg(), loop.counter_vreg(), left.offset().value_or(0), RegisterWidth::r8)
                : program.mem(program.constant64(left.data().constant().value_as_int64() + left.offset().value_or(0)),
                              loop.counter_vreg(), RegisterWidth::r8);
        auto right_operand =
            right.data().is_reg()
                ? program.mem(right.data().reg(), loop.counter_vreg(), right.offset().value_or(0), RegisterWidth::r8)
                : program.mem(program.constant64(right.data().constant().value_as_int64() + right.offset().value_or(0)),
                              loop.counter_vreg(), RegisterWidth::r8);

        /// Compare left[i] and right[i].
        /// If they are not equals, return false.
        program << program.mov(result_vreg, left_operand) << program.cmp(result_vreg, right_operand)
                << program.jne(unequals_label)

                /// If both are equals, test if they are '\0'
                /// (we only need to test one of them since both are equals).
                << program.test(result_vreg, result_vreg) << program.jz(equals_label);
    }

    /// If left > right: test if the left[min_size] == '\0'
    if (left.size() > right.size())
    {
        auto left_operand =
            left.data().is_reg()
                ? program.mem(left.data().reg(), min_size + left.offset().value_or(0), RegisterWidth::r8)
                : program.mem(program.constant64(left.data().constant().value_as_int64() + min_size +
                                                 left.offset().value_or(0)),
                              RegisterWidth::r8);

        program << program.mov(result_vreg, left_operand) << program.test(result_vreg, result_vreg)
                << program.jz(equals_label);
    }
    /// If right > left: test if the right[min_size] == '\0'
    else if (right.size() > left.size())
    {
        auto right_operand =
            right.data().is_reg()
                ? program.mem(right.data().reg(), min_size + right.offset().value_or(0), RegisterWidth::r8)
                : program.mem(program.constant64(right.data().constant().value_as_int64() + min_size +
                                                 right.offset().value_or(0)),
                              RegisterWidth::r8);

        program << program.mov(result_vreg, right_operand) << program.test(result_vreg, result_vreg)
                << program.jz(equals_label);
    }
    /// Both have the same length, if we did not jump to unequals_label, both are equals.
    else
    {
        program << program.jmp(equals_label);
    }

    program
        /// not equals: set result_vreg to 0 and jump to end
        << program.section(unequals_label) << program.xor_(result_vreg, result_vreg)
        << program.jmp(end_label)

        /// equals: set result_vreg to 1
        << program.section(equals_label) << program.mov(result_vreg, program.constant8(1))
        << program.section(end_label);

    return result_vreg;
}

flounder::Register String::is_equals_inlined_and_pointer(Program &program, std::string &&name, Descriptor left,
                                                         Descriptor right)
{
    /// Only one string is a pointer.
    assert(left.is_pointer() && right.is_pointer() == false);
    assert(left.size() != right.size());

    const auto inlined_size = left.is_pointer() ? right.size() : left.size();
    const auto outlined_size = left.is_pointer() ? left.size() : right.size();

    auto left_operand = left.data();
    auto right_operand = right.data();
    const auto register_width = static_cast<RegisterWidth>(inlined_size * 8U);

    /// Load the data pointed out by the pointer into a register of the inlined size.
    if (left.is_pointer())
    {
        left_operand =
            left.data().is_reg()
                ? program.mem(left.data().reg(), left.offset().value_or(0), register_width)
                : program.mem(program.constant64(left.data().constant().value_as_int64() + left.offset().value_or(0)),
                              register_width);
    }
    else if (right.is_pointer())
    {
        right_operand =
            right.data().is_reg()
                ? program.mem(right.data().reg(), right.offset().value_or(0), register_width)
                : program.mem(program.constant64(right.data().constant().value_as_int64() + right.offset().value_or(0)),
                              register_width);
    }

    auto result_vreg = program.vreg(fmt::format("streq_{}_result", name));
    auto unequals_label = program.label(fmt::format("streq_{}_ne", name));
    auto equals_label = program.label(fmt::format("streq_{}_eq", name));
    auto end_label = program.label(fmt::format("streq_{}_end", name));

    program << program.request_vreg(result_vreg, register_width) << program.cmp(left_operand, right_operand)
            << program.jne(unequals_label);

    /// Actually, we only support when the pointer points to data
    /// that is larger than the inlined size, i.e., (inlined("ABCD") == "ABCDEFG").
    /// They can not be equal, because then both would be inlined (or both not)
    /// and compared by integer comparison (or normal string comparison respectively).
    if (outlined_size > inlined_size)
    {
        /// Check the outlined operand (which is the larger)
        /// if it ends, too.

        /// Both have the same size, jump to equals.
        if (left.size() == right.size())
        {
            program << program.jmp(equals_label);
        }
        else
        {
            /// We need a 8bit register to load the larger byte to test for '\0'.
            /// However, if the result_vreg is 8bit, we can reuse it.
            auto access_last_byte_vreg =
                register_width == RegisterWidth::r8 ? result_vreg : program.vreg(fmt::format("streq_{}_last", name));
            if (register_width != RegisterWidth::r8)
            {
                program << program.request_vreg8(access_last_byte_vreg);
            }

            auto outlined_operand = std::optional<MemoryAddress>{std::nullopt};
            /// Left is larger.
            if (left.size() > right.size())
            {
                if (left.data().is_reg())
                {
                    outlined_operand =
                        program.mem(left.data().reg(), right.size() + left.offset().value_or(0), RegisterWidth::r8);
                }
                else
                {
                    outlined_operand = program.mem(program.constant64(left.data().constant().value_as_int64() +
                                                                      right.size() + left.offset().value_or(0)),
                                                   RegisterWidth::r8);
                }
            }

            /// Right is larger.
            else
            {
                if (right.data().is_reg())
                {
                    outlined_operand =
                        program.mem(right.data().reg(), left.size() + right.offset().value_or(0), RegisterWidth::r8);
                }
                else
                {
                    outlined_operand = program.mem(program.constant64(right.data().constant().value_as_int64() +
                                                                      left.size() + right.offset().value_or(0)),
                                                   RegisterWidth::r8);
                }
            }

            program << program.mov(access_last_byte_vreg, outlined_operand.value())
                    << program.test(access_last_byte_vreg, access_last_byte_vreg);

            /// Clear the reg used for comparing the last byte,
            /// if we did not reuse the result_vreg.
            if (access_last_byte_vreg != result_vreg)
            {
                program << program.clear(access_last_byte_vreg);
            }

            program << program.je(equals_label);
        }
    }
    else
    {
        throw NotImplementedException{"streq of inlined and outlined with outlined_size < inlined_size"};
    }

    program
        /// not equals: set result_vreg to 0 and jump to end
        << program.section(unequals_label) << program.xor_(result_vreg, result_vreg)
        << program.jmp(end_label)

        /// equals: set result_vreg to 1
        << program.section(equals_label) << program.mov(result_vreg, program.constant8(1))
        << program.section(end_label);

    return result_vreg;
}

flounder::Register String::is_equals_fixed_and_variable_size(Program &program, std::string &&name,
                                                             Descriptor fixed_size_descriptor,
                                                             Descriptor variable_size_descriptor)
{
    /// We assume, that the variable size is always larger or equal to the
    /// fixed size.
    assert(variable_size_descriptor.size() >= fixed_size_descriptor.size());

    auto unequals_label = program.label(fmt::format("streq_{}_ne", name));
    auto equals_label = program.label(fmt::format("streq_{}_eq", name));
    auto end_label = program.label(fmt::format("streq_{}_end", name));

    auto result_vreg = program.vreg(fmt::format("streq_{}_result", name));
    program << program.request_vreg8(result_vreg);

    auto byte_compared = 0UL;

    /// Compare as many 8byte at once.
    {
        const auto count_comparisons = fixed_size_descriptor.size() / 8U;
        if (count_comparisons > 0U)
        {
            auto compare_vreg = program.vreg(fmt::format("streq8_{}", name));
            program << program.request_vreg64(compare_vreg);
            for (auto i = 0U; i < count_comparisons; ++i)
            {
                const auto fixed_offset = byte_compared + fixed_size_descriptor.offset().value_or(0U);
                auto left_operand =
                    String::access(program, fixed_size_descriptor.data(), fixed_offset, RegisterWidth::r64);

                auto variable_offset = byte_compared + variable_size_descriptor.offset().value_or(0U);
                auto right_operand =
                    String::access(program, variable_size_descriptor.data(), variable_offset, RegisterWidth::r64);

                program << program.mov(compare_vreg, left_operand) << program.cmp(compare_vreg, right_operand)
                        << program.jne(unequals_label);

                byte_compared += 8U;
            }
            program << program.clear(compare_vreg);
        }
    }

    /// Compare as many 4byte at once.
    {
        const auto count_comparisons = (fixed_size_descriptor.size() - byte_compared) / 4U;
        if (count_comparisons > 0U)
        {
            auto compare_vreg = program.vreg(fmt::format("streq4_{}", name));
            program << program.request_vreg32(compare_vreg);
            for (auto i = 0U; i < count_comparisons; ++i)
            {
                const auto fixed_offset = byte_compared + fixed_size_descriptor.offset().value_or(0U);
                auto left_operand =
                    String::access(program, fixed_size_descriptor.data(), fixed_offset, RegisterWidth::r32);

                auto variable_offset = byte_compared + variable_size_descriptor.offset().value_or(0U);
                auto right_operand =
                    String::access(program, variable_size_descriptor.data(), variable_offset, RegisterWidth::r32);

                program << program.mov(compare_vreg, left_operand) << program.cmp(compare_vreg, right_operand)
                        << program.jne(unequals_label);

                byte_compared += 4U;
            }
            program << program.clear(compare_vreg);
        }
    }

    /// Compare as many 2byte at once.
    {
        const auto count_comparisons = (fixed_size_descriptor.size() - byte_compared) / 2U;
        if (count_comparisons > 0U)
        {
            auto compare_vreg = program.vreg(fmt::format("streq2_{}", name));
            program << program.request_vreg16(compare_vreg);
            for (auto i = 0U; i < count_comparisons; ++i)
            {
                const auto fixed_offset = byte_compared + fixed_size_descriptor.offset().value_or(0U);
                auto left_operand =
                    String::access(program, fixed_size_descriptor.data(), fixed_offset, RegisterWidth::r16);

                auto variable_offset = byte_compared + variable_size_descriptor.offset().value_or(0U);
                auto right_operand =
                    String::access(program, variable_size_descriptor.data(), variable_offset, RegisterWidth::r16);

                program << program.mov(compare_vreg, left_operand) << program.cmp(compare_vreg, right_operand)
                        << program.jne(unequals_label);

                byte_compared += 2U;
            }
            program << program.clear(compare_vreg);
        }
    }

    /// Compare as many byte-wise at once.
    {
        const auto count_comparisons = fixed_size_descriptor.size() - byte_compared;
        if (count_comparisons > 0U)
        {
            auto compare_vreg = program.vreg(fmt::format("streq1_{}", name));
            program << program.request_vreg8(compare_vreg);
            for (auto i = 0U; i < count_comparisons; ++i)
            {
                const auto fixed_offset = byte_compared + fixed_size_descriptor.offset().value_or(0U);
                auto left_operand =
                    String::access(program, fixed_size_descriptor.data(), fixed_offset, RegisterWidth::r8);

                auto variable_offset = byte_compared + variable_size_descriptor.offset().value_or(0U);
                auto right_operand =
                    String::access(program, variable_size_descriptor.data(), variable_offset, RegisterWidth::r8);

                program << program.mov(compare_vreg, left_operand) << program.cmp(compare_vreg, right_operand)
                        << program.jne(unequals_label);

                ++byte_compared;
            }
            program << program.clear(compare_vreg);
        }
    }

    if (variable_size_descriptor.size() > fixed_size_descriptor.size())
    {
        auto variable_offset = fixed_size_descriptor.size() + variable_size_descriptor.offset().value_or(0U);
        auto variable_operand =
            String::access(program, variable_size_descriptor.data(), variable_offset, RegisterWidth::r8);

        program << program.mov(result_vreg, variable_operand) << program.test(result_vreg, result_vreg)
                << program.jz(equals_label);
    }
    else
    {
        program << program.jmp(equals_label);
    }

    program
        /// not equals: set result_vreg to 0 and jump to end
        << program.section(unequals_label) << program.xor_(result_vreg, result_vreg)
        << program.jmp(end_label)

        /// equals: set result_vreg to 1
        << program.section(equals_label) << program.mov(result_vreg, program.constant8(1))
        << program.section(end_label);

    return result_vreg;
}