#pragma once

#include "statement.h"
#include <fmt/core.h>
#include <string>

namespace flounder {

class Debug
{
public:
    static void print_ptr(Program &program, Register vreg)
    {
        program << program.comment(fmt::format("Debug::print_ptr({})", vreg.virtual_name().value()));
        std::ignore = FunctionCall{program, std::uintptr_t(&Debug::cout_ptr)}.call({Operand{vreg}});
    }

    static void print_int64(Program &program, Register vreg)
    {
        program << program.comment(fmt::format("Debug::print_int64_t({})", vreg.virtual_name().value()));
        std::ignore = FunctionCall{program, std::uintptr_t(&Debug::cout_int64)}.call({Operand{vreg}});
    }

    static void print_int32(Program &program, Register vreg)
    {
        program << program.comment(fmt::format("Debug::print_int32_t({})", vreg.virtual_name().value()));
        std::ignore = FunctionCall{program, std::uintptr_t(&Debug::cout_int32)}.call({Operand{vreg}});
    }

    static void print_int8(Program &program, Register vreg)
    {
        program << program.comment(fmt::format("Debug::print_int8_t({})", vreg.virtual_name().value()));
        std::ignore = FunctionCall{program, std::uintptr_t(&Debug::cout_int8)}.call({Operand{vreg}});
    }

    static void print_string(Program &program, Register vreg)
    {
        program << program.comment(fmt::format("Debug::print_string({})", vreg.virtual_name().value()));
        std::ignore = FunctionCall{program, std::uintptr_t(&Debug::cout_string)}.call({Operand{vreg}});
    }

    static void say_hello(Program &program)
    {
        program << program.comment("Debug::say_hello");
        std::ignore = FunctionCall{program, std::uintptr_t(&Debug::cout_hello)}.call();
    }

private:
    __attribute__((noinline)) static void cout_ptr(const std::uintptr_t value)
    {
        std::cout << fmt::format("[Flounder DEBUG] {:d}", value) << std::endl;
    }

    __attribute__((noinline)) static void cout_int64(const std::int64_t value)
    {
        std::cout << fmt::format("[Flounder DEBUG] {:d}", value) << std::endl;
    }

    __attribute__((noinline)) static void cout_int32(const std::int32_t value)
    {
        std::cout << fmt::format("[Flounder DEBUG] {:d}", std::int32_t(value)) << std::endl;
    }

    __attribute__((noinline)) static void cout_int8(const std::int64_t value)
    {
        std::cout << fmt::format("[Flounder DEBUG] {:d}", std::int8_t(value)) << std::endl;
    }

    __attribute__((noinline)) static void cout_hello() { std::cout << "[Flounder DEBUG] Hello :-)" << std::endl; }

    __attribute__((noinline)) static void cout_string(std::uintptr_t str_address)
    {
        std::cout << "[Flounder DEBUG] " << reinterpret_cast<char *>(str_address) << std::endl;
    }
};
} // namespace flounder