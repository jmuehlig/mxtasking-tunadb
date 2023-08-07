#pragma once

#include "annotation.h"
#include "exception.h"
#include "instruction_set.h"
#include <algorithm>
#include <flounder/compilation/compilate.h>
#include <flounder/ir/instructions.h>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace flounder {
class Program
{
public:
    explicit Program()
        : _blocks({InstructionSet{"Arguments", 32U}, InstructionSet{"Header", 64U}, InstructionSet{"Body", 4096U}})
    {
    }
    Program(Program &&) noexcept = default;
    Program(const Program &) = delete;

    ~Program()
    {
        for (auto data : _data)
        {
            std::free(reinterpret_cast<void *>(data));
        }
    }

    Program &operator=(Program &&) noexcept = default;
    Program &operator=(const Program &) = delete;

    [[nodiscard]] std::uint64_t next_id() noexcept { return _increment_identifier++; }
    [[nodiscard]] std::uint64_t size() noexcept
    {
        return std::accumulate(_blocks.begin(), _blocks.end(), 0UL,
                               [](const auto size, const auto &block) { return size + block.size(); });
    }

    //    void annotate(std::unique_ptr<Annotation> &&annotation) { _annotations.emplace_back(std::move(annotation)); }
    //    [[nodiscard]] const std::vector<std::unique_ptr<Annotation>> &annotations() const noexcept { return
    //    _annotations; }

    [[nodiscard]] InstructionSet &arguments() noexcept { return _blocks[0U]; }
    [[nodiscard]] const InstructionSet &arguments() const noexcept { return _blocks[0U]; }
    [[nodiscard]] InstructionSet &header() noexcept { return _blocks[1U]; }
    [[nodiscard]] const InstructionSet &header() const noexcept { return _blocks[1U]; }
    [[nodiscard]] InstructionSet &body() noexcept { return _blocks[2U]; }
    [[nodiscard]] const InstructionSet &body() const noexcept { return _blocks[2U]; }

    /**
     * Inserts the given instruction at the end of the active code.
     *
     * @param instruction Instruction to insert.
     * @return The program.
     */
    template <typename T>
    requires is_instruction<T> Program &operator<<(T &&instruction)
    {
        body() << std::move(instruction);
        return *this;
    }

    /**
     * Inserts the given instruction at the end of the active code.
     *
     * @param instruction Instruction to insert.
     * @return The program.
     */
    Program &operator<<(Instruction &&instruction)
    {
        body() << std::move(instruction);
        return *this;
    }

    /**
     * Inserts the given instruction at the end of the active code.
     *
     * @param instruction Instruction to insert.
     * @return The program.
     */
    template <typename T>
    requires is_instruction<T> Program &operator<<(T &instruction)
    {
        body() << instruction;
        return *this;
    }

    /**
     * Inserts the given code at the end of the active code.
     *
     * @param code Code to insert.
     * @return The program.
     */
    Program &operator<<(InstructionSet &&code)
    {
        body() << std::move(code);
        return *this;
    }

    [[nodiscard]] void *data(const std::size_t size)
    {
        auto *memory = std::aligned_alloc(64U, size);
        _data.emplace_back(std::uintptr_t(memory));
        return memory;
    }

    [[nodiscard]] Register vreg(std::string &&name, const bool is_accessed_frequently)
    {
        if (auto iterator = _virtual_register_names.find(name); iterator != _virtual_register_names.end())
        {
            return Register{std::string_view{*iterator}, is_accessed_frequently};
        }

        auto [iterator, _] = _virtual_register_names.insert(std::move(name));
        return Register{std::string_view{*iterator}, is_accessed_frequently};
    }

    [[nodiscard]] Register vreg(std::string &&name) { return vreg(std::move(name), true); }

    //    [[nodiscard]] Register mreg(const RegisterWidth width, const std::uint8_t register_id)
    //    {
    //        return Register{register_id, width};
    //    }

    [[nodiscard]] Register mreg(const RegisterWidth width, const RegisterSignType sign_type,
                                const std::uint8_t register_id)
    {
        return Register{register_id, width, sign_type};
    }

    //    [[nodiscard]] Register mreg8(const std::uint8_t register_id) { return mreg(RegisterWidth::r8, register_id); }
    //
    //    [[nodiscard]] Register mreg16(const std::uint8_t register_id) { return mreg(RegisterWidth::r16, register_id);
    //    }
    //
    //    [[nodiscard]] Register mreg32(const std::uint8_t register_id) { return mreg(RegisterWidth::r32, register_id);
    //    }

    [[nodiscard]] Register mreg64(const std::uint8_t register_id)
    {
        return mreg(RegisterWidth::r64, RegisterSignType::Signed, register_id);
    }

    [[nodiscard]] Label label(std::string &&name)
    {
        if (auto iterator = _label_names.find(name); iterator != _label_names.end())
        {
            return Label{std::string_view{*iterator}};
        }

        auto [iterator, _] = _label_names.insert(std::move(name));
        return Label{std::string_view{*iterator}};
    }

    [[nodiscard]] Constant constant64(const std::int64_t value)
    {
        if (value <= std::numeric_limits<std::int32_t>::max())
        {
            return constant32(value);
        }
        return Constant{value};
    }

    [[nodiscard]] Constant constant32(const std::int32_t value) { return Constant{value}; }

    [[nodiscard]] Constant constant16(const std::int16_t value) { return Constant{value}; }

    [[nodiscard]] Constant constant8(const std::int8_t value) { return Constant{value}; }

    [[nodiscard]] Constant address(const std::uintptr_t address) { return Constant{address}; }

    template <typename T> [[nodiscard]] Constant address(T *address) { return this->address(std::uintptr_t(address)); }

    [[nodiscard]] MemoryAddress mem(Register reg) { return MemoryAddress{reg, 0}; }

    [[nodiscard]] MemoryAddress mem(Register reg, const RegisterWidth width) { return mem(reg, 0, width); }

    [[nodiscard]] MemoryAddress mem(const std::int64_t address) { return MemoryAddress{constant64(address)}; }

    [[nodiscard]] MemoryAddress mem(Register reg, const std::int32_t offset) { return MemoryAddress{reg, offset}; }

    [[nodiscard]] MemoryAddress mem(Register reg, const std::int32_t offset, const RegisterWidth width)
    {
        return MemoryAddress{reg, offset, width};
    }

    [[nodiscard]] MemoryAddress mem(Constant base, Register index)
    {
        return MemoryAddress{base, index, 0U, 0, std::nullopt};
    }

    [[nodiscard]] MemoryAddress mem(Constant base, Register index, const RegisterWidth width)
    {
        return MemoryAddress{base, index, 0U, 0, width};
    }

    [[nodiscard]] MemoryAddress mem(Constant base, RegisterWidth width)
    {
        return MemoryAddress{base, std::nullopt, 0U, 0, width};
    }

    [[nodiscard]] MemoryAddress mem(Register reg, Register index, const std::int32_t displacement,
                                    const RegisterWidth width)
    {
        return MemoryAddress{reg, index, 0, displacement, width};
    }

    [[nodiscard]] MemoryAddress mem(Register reg, Register index, const std::uint8_t scale,
                                    const std::int32_t displacement)
    {
        return MemoryAddress{reg, index, scale, displacement, std::nullopt};
    }

    [[nodiscard]] MemoryAddress mem(Register reg, Register index, const std::uint8_t scale,
                                    const std::int32_t displacement, const RegisterWidth width)
    {
        return MemoryAddress{reg, index, scale, displacement, width};
    }

    [[nodiscard]] MemoryAddress mem(Register base, Register index)
    {
        return MemoryAddress{base, index, 0U, 0, std::nullopt};
    }

    [[nodiscard]] MemoryAddress mem(Register base, Register index, const RegisterWidth width)
    {
        return MemoryAddress{base, index, 0U, 0, width};
    }

    [[nodiscard]] MemoryAddress mem(Register base, Register index, const std::int32_t displacement)
    {
        return MemoryAddress{base, index, 0U, displacement, std::nullopt};
    }

    [[nodiscard]] VregInstruction request_vreg8(Register vreg)
    {
        return VregInstruction{vreg, RegisterWidth::r8, RegisterSignType::Signed};
    }

    [[nodiscard]] VregInstruction request_vreg16(Register vreg)
    {
        return VregInstruction{vreg, RegisterWidth::r16, RegisterSignType::Signed};
    }

    [[nodiscard]] VregInstruction request_vreg32(Register vreg)
    {
        return VregInstruction{vreg, RegisterWidth::r32, RegisterSignType::Signed};
    }

    [[nodiscard]] VregInstruction request_vreg64(Register vreg)
    {
        return VregInstruction{vreg, RegisterWidth::r64, RegisterSignType::Signed};
    }

    [[nodiscard]] VregInstruction request_vreg8u(Register vreg)
    {
        return VregInstruction{vreg, RegisterWidth::r8, RegisterSignType::Unsigned};
    }

    [[nodiscard]] VregInstruction request_vreg16u(Register vreg)
    {
        return VregInstruction{vreg, RegisterWidth::r16, RegisterSignType::Unsigned};
    }

    [[nodiscard]] VregInstruction request_vreg32u(Register vreg)
    {
        return VregInstruction{vreg, RegisterWidth::r32, RegisterSignType::Unsigned};
    }

    [[nodiscard]] VregInstruction request_vreg64u(Register vreg)
    {
        return VregInstruction{vreg, RegisterWidth::r64, RegisterSignType::Unsigned};
    }

    [[nodiscard]] VregInstruction request_vreg(Register vreg, const RegisterWidth width)
    {
        return VregInstruction{vreg, width, RegisterSignType::Signed};
    }

    [[nodiscard]] VregInstruction request_vreg(Register vreg, const RegisterWidth width,
                                               const RegisterSignType sign_type)
    {
        return VregInstruction{vreg, width, sign_type};
    }

    [[nodiscard]] ClearInstruction clear(Register vreg) { return ClearInstruction{vreg}; }

    [[nodiscard]] FcallInstruction fcall(const std::uintptr_t function) { return FcallInstruction{function}; }

    [[nodiscard]] FcallInstruction fcall(const std::uintptr_t function, Register return_register)
    {
        return FcallInstruction{return_register, function};
    }

    [[nodiscard]] CallInstruction call(const std::uintptr_t function) { return CallInstruction{function}; }

    [[nodiscard]] GetArgumentInstruction get_argument(const std::uint8_t argument_number, Register reg)
    {
        return GetArgumentInstruction{argument_number, reg};
    }

    [[nodiscard]] GetArgumentInstruction get_arg0(Register reg) { return this->get_argument(0U, reg); }
    [[nodiscard]] GetArgumentInstruction get_arg1(Register reg) { return this->get_argument(1U, reg); }
    [[nodiscard]] GetArgumentInstruction get_arg2(Register reg) { return this->get_argument(2U, reg); }
    [[nodiscard]] GetArgumentInstruction get_arg3(Register reg) { return this->get_argument(3U, reg); }
    [[nodiscard]] GetArgumentInstruction get_arg4(Register reg) { return this->get_argument(4U, reg); }

    [[nodiscard]] PushInstruction push(Register mreg) { return PushInstruction{mreg}; }

    [[nodiscard]] PopInstruction pop(Register mreg) { return PopInstruction{mreg}; }

    [[nodiscard]] MovInstruction mov(Operand destination, Operand source)
    {
        return MovInstruction{destination, source};
    }

    [[nodiscard]] MovInstruction mov(Operand destination, Constant source) { return mov(destination, Operand{source}); }

    [[nodiscard]] MovInstruction mov(Register destination, Operand source) { return mov(Operand{destination}, source); }

    [[nodiscard]] MovInstruction mov(Operand destination, Register source) { return mov(destination, Operand{source}); }

    [[nodiscard]] MovInstruction mov(Register destination, Register source)
    {
        return mov(Operand{destination}, Operand{source});
    }

    [[nodiscard]] MovInstruction mov(Register destination, MemoryAddress source)
    {
        return mov(Operand{destination}, Operand{source});
    }

    [[nodiscard]] MovInstruction mov(Register destination, Constant source)
    {
        return mov(Operand{destination}, Operand{source});
    }

    [[nodiscard]] MovInstruction mov(MemoryAddress destination, Register source)
    {
        return mov(Operand{destination}, Operand{source});
    }

    [[nodiscard]] MovInstruction mov(MemoryAddress destination, Constant source)
    {
        return mov(Operand{destination}, Operand{source});
    }

    [[nodiscard]] CmovleInstruction cmovle(Register destination, Operand source)
    {
        return CmovleInstruction{Operand{destination}, source};
    }

    [[nodiscard]] CmovleInstruction cmovle(Register destination, Register source)
    {
        return cmovle(destination, Operand{source});
    }

    [[nodiscard]] CmovleInstruction cmovle(Register destination, MemoryAddress source)
    {
        return cmovle(destination, Operand{source});
    }

    [[nodiscard]] CmovgeInstruction cmovge(Register destination, Operand source)
    {
        return CmovgeInstruction{Operand{destination}, source};
    }

    [[nodiscard]] CmovgeInstruction cmovge(Register destination, Register source)
    {
        return cmovge(destination, Operand{source});
    }

    [[nodiscard]] CmovgeInstruction cmovge(Register destination, MemoryAddress source)
    {
        return cmovge(destination, Operand{source});
    }

    [[nodiscard]] LeaInstruction lea(Register destination, MemoryAddress source)
    {
        return LeaInstruction{Operand{destination}, Operand{source}};
    }

    [[nodiscard]] RetInstruction ret() { return RetInstruction{}; }

    [[nodiscard]] NopInstruction nop() { return NopInstruction{}; }

    [[nodiscard]] CqoInstruction cqo() { return CqoInstruction{}; }

    [[nodiscard]] SectionInstruction section(Label label) { return SectionInstruction{label}; }

    [[nodiscard]] CommentInstruction comment(std::string &&comment) { return CommentInstruction{std::move(comment)}; }

    [[nodiscard]] ContextBeginInstruction context_begin(std::string &&name)
    {
        return ContextBeginInstruction{std::move(name)};
    }

    [[nodiscard]] ContextEndInstruction context_end(std::string &&name)
    {
        return ContextEndInstruction{std::move(name)};
    }

    [[nodiscard]] BranchBeginInstruction begin_branch(const std::uint64_t branch_id)
    {
        return BranchBeginInstruction{branch_id};
    }

    [[nodiscard]] BranchEndInstruction end_branch() { return BranchEndInstruction{}; }

    [[nodiscard]] CmpInstruction cmp(Operand left, Operand right) { return CmpInstruction{left, right}; }
    [[nodiscard]] CmpInstruction cmp(Operand left, Operand right, const bool is_likely)
    {
        return CmpInstruction{left, right, is_likely};
    }
    [[nodiscard]] CmpInstruction cmp(Register left, Register right) { return cmp(Operand{left}, Operand{right}); }
    [[nodiscard]] CmpInstruction cmp(Register left, MemoryAddress right) { return cmp(Operand{left}, Operand{right}); }
    [[nodiscard]] CmpInstruction cmp(Register left, Constant right) { return cmp(Operand{left}, Operand{right}); }
    [[nodiscard]] CmpInstruction cmp(MemoryAddress left, Register right) { return cmp(Operand{left}, Operand{right}); }
    [[nodiscard]] CmpInstruction cmp(MemoryAddress left, Constant right) { return cmp(Operand{left}, Operand{right}); }

    [[nodiscard]] TestInstruction test(Operand left, Operand right) { return TestInstruction{left, right}; }
    [[nodiscard]] TestInstruction test(Register left, Register right) { return test(Operand{left}, Operand{right}); }
    [[nodiscard]] TestInstruction test(MemoryAddress left, Register right)
    {
        return test(Operand{left}, Operand{right});
    }
    [[nodiscard]] TestInstruction test(Register reg, Constant right) { return test(Operand{reg}, Operand{right}); }
    [[nodiscard]] TestInstruction test(MemoryAddress mem, Constant right) { return test(Operand{mem}, Operand{right}); }

    [[nodiscard]] JumpInstruction jmp(Label label) { return JumpInstruction{JumpInstruction::JMP, label}; }
    [[nodiscard]] JumpInstruction je(Label label) { return JumpInstruction{JumpInstruction::JE, label}; }
    [[nodiscard]] JumpInstruction jne(Label label) { return JumpInstruction{JumpInstruction::JNE, label}; }
    [[nodiscard]] JumpInstruction jl(Label label) { return JumpInstruction{JumpInstruction::JL, label}; }
    [[nodiscard]] JumpInstruction jle(Label label) { return JumpInstruction{JumpInstruction::JLE, label}; }
    [[nodiscard]] JumpInstruction jg(Label label) { return JumpInstruction{JumpInstruction::JG, label}; }
    [[nodiscard]] JumpInstruction jge(Label label) { return JumpInstruction{JumpInstruction::JGE, label}; }
    [[nodiscard]] JumpInstruction ja(Label label) { return JumpInstruction{JumpInstruction::JA, label}; }
    [[nodiscard]] JumpInstruction jae(Label label) { return JumpInstruction{JumpInstruction::JAE, label}; }
    [[nodiscard]] JumpInstruction jb(Label label) { return JumpInstruction{JumpInstruction::JB, label}; }
    [[nodiscard]] JumpInstruction jbe(Label label) { return JumpInstruction{JumpInstruction::JBE, label}; }
    [[nodiscard]] JumpInstruction jz(Label label) { return JumpInstruction{JumpInstruction::JZ, label}; }
    [[nodiscard]] JumpInstruction jnz(Label label) { return JumpInstruction{JumpInstruction::JNZ, label}; }

    [[nodiscard]] SetneInstruction setne(Operand operand) { return SetneInstruction{operand}; }
    [[nodiscard]] SetneInstruction setne(Register reg) { return setne(Operand{reg}); }

    [[nodiscard]] SeteInstruction sete(Operand operand) { return SeteInstruction{operand}; }
    [[nodiscard]] SeteInstruction sete(Register reg) { return sete(Operand{reg}); }

    [[nodiscard]] IdivInstruction idiv(Operand operand) { return IdivInstruction{operand}; }
    [[nodiscard]] IdivInstruction idiv(Register reg) { return idiv(Operand{reg}); }
    [[nodiscard]] IdivInstruction idiv(MemoryAddress mem) { return idiv(Operand{mem}); }

    [[nodiscard]] IncInstruction inc(Operand operand) { return IncInstruction{operand}; }
    [[nodiscard]] IncInstruction inc(Register reg) { return inc(Operand{reg}); }
    [[nodiscard]] IncInstruction inc(MemoryAddress mem) { return inc(Operand{mem}); }

    [[nodiscard]] DecInstruction dec(Operand operand) { return DecInstruction{operand}; }
    [[nodiscard]] DecInstruction dec(Register reg) { return dec(Operand{reg}); }
    [[nodiscard]] DecInstruction dec(MemoryAddress mem) { return dec(Operand{mem}); }

    [[nodiscard]] PrefetchInstruction prefetch(MemoryAddress mem) { return PrefetchInstruction{Operand{mem}}; }

    [[nodiscard]] SetReturnArgumentInstruction set_return(Operand operand)
    {
        return SetReturnArgumentInstruction{operand};
    }

    [[nodiscard]] SetReturnArgumentInstruction set_return(Register reg) { return set_return(Operand{reg}); }

    [[nodiscard]] AddInstruction add(Operand left, Operand right) { return AddInstruction{left, right}; }
    [[nodiscard]] AddInstruction add(Register left, Register right) { return add(Operand{left}, Operand{right}); }
    [[nodiscard]] AddInstruction add(Register left, MemoryAddress right) { return add(Operand{left}, Operand{right}); }
    [[nodiscard]] AddInstruction add(Register left, Constant right) { return add(Operand{left}, Operand{right}); }
    [[nodiscard]] AddInstruction add(MemoryAddress left, Register right) { return add(Operand{left}, Operand{right}); }
    [[nodiscard]] AddInstruction add(MemoryAddress left, Constant right) { return add(Operand{left}, Operand{right}); }

    [[nodiscard]] XaddInstruction xadd(Operand left, Operand right, const bool is_locked = false)
    {
        return XaddInstruction{left, right, is_locked};
    }
    [[nodiscard]] XaddInstruction xadd(Register left, Register right, const bool is_locked = false)
    {
        return xadd(Operand{left}, Operand{right}, is_locked);
    }
    [[nodiscard]] XaddInstruction xadd(Register left, MemoryAddress right, const bool is_locked = false)
    {
        return xadd(Operand{left}, Operand{right}, is_locked);
    }
    [[nodiscard]] XaddInstruction xadd(Register left, Constant right, const bool is_locked = false)
    {
        return xadd(Operand{left}, Operand{right}, is_locked);
    }
    [[nodiscard]] XaddInstruction xadd(MemoryAddress left, Register right, const bool is_locked = false)
    {
        return xadd(Operand{left}, Operand{right}, is_locked);
    }
    [[nodiscard]] XaddInstruction xadd(MemoryAddress left, Constant right, const bool is_locked = false)
    {
        return xadd(Operand{left}, Operand{right}, is_locked);
    }

    [[nodiscard]] SubInstruction sub(Operand left, Operand right) { return SubInstruction{left, right}; }
    [[nodiscard]] SubInstruction sub(Register left, Register right) { return sub(Operand{left}, Operand{right}); }
    [[nodiscard]] SubInstruction sub(Register left, MemoryAddress right) { return sub(Operand{left}, Operand{right}); }
    [[nodiscard]] SubInstruction sub(Register left, Constant right) { return sub(Operand{left}, Operand{right}); }
    [[nodiscard]] SubInstruction sub(MemoryAddress left, Register right) { return sub(Operand{left}, Operand{right}); }
    [[nodiscard]] SubInstruction sub(MemoryAddress left, Constant right) { return sub(Operand{left}, Operand{right}); }

    [[nodiscard]] ImulInstruction imul(Operand left, Operand right) { return ImulInstruction{left, right}; }
    [[nodiscard]] ImulInstruction imul(Register left, Register right) { return imul(Operand{left}, Operand{right}); }
    [[nodiscard]] ImulInstruction imul(Register left, MemoryAddress right)
    {
        return imul(Operand{left}, Operand{right});
    }
    [[nodiscard]] ImulInstruction imul(Register left, Constant right) { return imul(Operand{left}, Operand{right}); }
    [[nodiscard]] ImulInstruction imul(MemoryAddress left, Register right)
    {
        return imul(Operand{left}, Operand{right});
    }
    [[nodiscard]] ImulInstruction imul(MemoryAddress left, Constant right)
    {
        return imul(Operand{left}, Operand{right});
    }

    [[nodiscard]] AndInstruction and_(Operand left, Operand right) { return AndInstruction{left, right}; }
    [[nodiscard]] AndInstruction and_(Register left, Register right) { return and_(Operand{left}, Operand{right}); }
    [[nodiscard]] AndInstruction and_(Register left, MemoryAddress right)
    {
        return and_(Operand{left}, Operand{right});
    }
    [[nodiscard]] AndInstruction and_(Register left, Constant right) { return and_(Operand{left}, Operand{right}); }
    [[nodiscard]] AndInstruction and_(MemoryAddress left, Register right)
    {
        return and_(Operand{left}, Operand{right});
    }
    [[nodiscard]] AndInstruction and_(MemoryAddress left, Constant right)
    {
        return and_(Operand{left}, Operand{right});
    }

    [[nodiscard]] OrInstruction or_(Operand left, Operand right) { return OrInstruction{left, right}; }
    [[nodiscard]] OrInstruction or_(Register left, Register right) { return or_(Operand{left}, Operand{right}); }
    [[nodiscard]] OrInstruction or_(Register left, MemoryAddress right) { return or_(Operand{left}, Operand{right}); }
    [[nodiscard]] OrInstruction or_(Register left, Constant right) { return or_(Operand{left}, Operand{right}); }
    [[nodiscard]] OrInstruction or_(MemoryAddress left, Register right) { return or_(Operand{left}, Operand{right}); }
    [[nodiscard]] OrInstruction or_(MemoryAddress left, Constant right) { return or_(Operand{left}, Operand{right}); }

    [[nodiscard]] XorInstruction xor_(Operand left, Operand right) { return XorInstruction{left, right}; }
    [[nodiscard]] XorInstruction xor_(Register left, Register right) { return xor_(Operand{left}, Operand{right}); }
    [[nodiscard]] XorInstruction xor_(Register left, MemoryAddress right)
    {
        return xor_(Operand{left}, Operand{right});
    }
    [[nodiscard]] XorInstruction xor_(Register left, Constant right) { return xor_(Operand{left}, Operand{right}); }
    [[nodiscard]] XorInstruction xor_(MemoryAddress left, Register right)
    {
        return xor_(Operand{left}, Operand{right});
    }
    [[nodiscard]] XorInstruction xor_(MemoryAddress left, Constant right)
    {
        return xor_(Operand{left}, Operand{right});
    }

    [[nodiscard]] ShlInstruction shl(Operand left, Operand right) { return ShlInstruction{left, right}; }
    [[nodiscard]] ShlInstruction shl(Register left, Constant right) { return shl(Operand{left}, Operand{right}); }
    [[nodiscard]] ShlInstruction shl(Register left, Register right) { return shl(Operand{left}, Operand{right}); }

    [[nodiscard]] ShrInstruction shr(Operand left, Operand right) { return ShrInstruction{left, right}; }
    [[nodiscard]] ShrInstruction shr(Register left, Constant right) { return shr(Operand{left}, Operand{right}); }
    [[nodiscard]] ShrInstruction shr(Register left, Register right) { return shr(Operand{left}, Operand{right}); }

    [[nodiscard]] Crc32Instruction crc32(Operand left, Operand right) { return Crc32Instruction{left, right}; }
    [[nodiscard]] Crc32Instruction crc32(Register left, Register right) { return crc32(Operand{left}, Operand{right}); }
    [[nodiscard]] Crc32Instruction crc32(Register left, MemoryAddress right)
    {
        return crc32(Operand{left}, Operand{right});
    }

    [[nodiscard]] FdivInstruction fdiv(Operand first, Operand second, Operand third)
    {
        return FdivInstruction{first, second, third};
    }

    [[nodiscard]] FdivInstruction fdiv(Operand left, Operand right) { return fdiv(left, left, right); }

    [[nodiscard]] FdivInstruction fdiv(Register left, Register right) { return fdiv(Operand{left}, Operand{right}); }

    [[nodiscard]] FdivInstruction fdiv(Register left, Constant right) { return fdiv(Operand{left}, Operand{right}); }

    [[nodiscard]] FmodInstruction fmod(Operand first, Operand second, Operand third)
    {
        return FmodInstruction{first, second, third};
    }

    [[nodiscard]] FmodInstruction fmod(Operand left, Operand right) { return fmod(left, left, right); }

    [[nodiscard]] FmodInstruction fmod(Register left, Constant right) { return fmod(Operand{left}, Operand{right}); }

    [[nodiscard]] AlignInstruction align(const std::uint8_t alignment) { return AlignInstruction{alignment}; }

    [[nodiscard]] std::vector<std::string> code() const noexcept;

private:
    /// Counter to increase for unique identifier names.
    std::uint64_t _increment_identifier{0U};

    /// Blocks of the program: Argument Handler (0), Header (1), body (2).
    std::array<InstructionSet, 3U> _blocks;

    /// Names of the virtual registers.
    std::unordered_set<std::string> _virtual_register_names;

    /// Names of the labels.
    std::unordered_set<std::string> _label_names;

    /// List of annotations.
    //    std::vector<std::unique_ptr<Annotation>> _annotations;

    /// Data needed at runtime, allocated just in time.
    std::vector<std::uintptr_t> _data;

    bool _is_optimize{true};
};

class ContextGuard
{
public:
    ContextGuard(Program &program, std::string &&name) : _program(program), _name(std::move(name))
    {
        program << program.context_begin(std::string{_name});
    }

    ~ContextGuard() { _program << _program.context_end(std::move(_name)); }

private:
    Program &_program;
    std::string _name;
};
} // namespace flounder