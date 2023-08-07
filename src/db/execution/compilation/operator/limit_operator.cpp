#include "limit_operator.h"
#include <flounder/comparator.h>
#include <flounder/debug.h>

using namespace db::execution::compilation;

void LimitOperator::produce(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    if (phase == GenerationPhase::prefetching)
    {
        this->child()->produce(phase, program, context);
        return;
    }

    auto context_guard = flounder::ContextGuard{program, "Limit"};

    const auto is_use_limit = this->is_use_limit(phase);

    if (is_use_limit)
    {
        /// Register for offset.
        if (this->_limit.offset() > 0U)
        {
            auto *offset = reinterpret_cast<std::uint64_t *>(program.data(sizeof(std::uint64_t)));
            *offset = 0U;

            this->_offset_address_vreg = program.vreg("offset_address");
            program.header() << program.request_vreg64(this->_offset_address_vreg.value())
                             << program.mov(this->_offset_address_vreg.value(),
                                            program.address(std::uintptr_t(offset)));
        }

        /// Register for limit.
        auto *limit = reinterpret_cast<std::uint64_t *>(program.data(sizeof(std::uint64_t)));
        *limit = 0U;
        this->_limit_address_vreg = program.vreg("limit_address");
        program.header() << program.request_vreg64(this->_limit_address_vreg.value())
                         << program.mov(this->_limit_address_vreg.value(), program.address(std::uintptr_t(limit)));
    }

    this->child()->produce(phase, program, context);

    if (is_use_limit)
    {
        if (this->_limit.offset() > 0U)
        {
            program << program.clear(this->_offset_address_vreg.value());
        }

        program << program.clear(this->_limit_address_vreg.value());
    }
}

void LimitOperator::consume(const GenerationPhase phase, flounder::Program &program, CompilationContext &context)
{
    if (phase == GenerationPhase::prefetching)
    {
        this->parent()->consume(phase, program, context);
        return;
    }

    auto context_guard = flounder::ContextGuard{program, "Limit"};

    const auto is_use_limit = this->is_use_limit(phase);

    if (is_use_limit)
    {
        /**
         * Increment offset and limit counter.
         * Generate instruction:
         *  - If offset not enough: jmp to after parent.consume()
         *  - If limit already fulfilled: jmp to after parent.consume()
         *      - TODO: Stop graph
         */

        if (this->_limit.offset() > 0U)
        {
            auto offset_address = program.mem(this->_offset_address_vreg.value());
            auto offset_vreg = program.vreg("offset");
            program << program.request_vreg64(offset_vreg) << program.mov(offset_vreg, program.constant8(1))
                    << program.xadd(offset_address, offset_vreg, true)
                    << program.cmp(offset_vreg, program.constant64(this->_limit.offset() - 1U))
                    << program.jle(context.label_next_record()) << program.clear(offset_vreg);
        }

        auto limit_address = program.mem(this->_limit_address_vreg.value());
        auto limit_vreg = program.vreg("limit");
        program << program.request_vreg64(limit_vreg) << program.mov(limit_vreg, program.constant8(1))
                << program.xadd(limit_address, limit_vreg, true)
                << program.cmp(limit_vreg, program.constant64(this->_limit.limit() - 1U))
                << program.jg(context.label_scan_end()) << program.clear(limit_vreg);

        program << program.begin_branch(0);
    }

    this->parent()->consume(phase, program, context);

    if (is_use_limit)
    {
        program << program.end_branch();
    }
}