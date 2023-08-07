#pragma once

#include <cstdint>
#include <mx/tasking/task.h>

namespace db::execution::compilation {
class ZeroOutBloomFilterTask final : public mx::tasking::TaskInterface
{
public:
    constexpr ZeroOutBloomFilterTask(void *begin, const std::size_t length) noexcept : _begin(begin), _length(length) {}

    ~ZeroOutBloomFilterTask() noexcept override = default;

    mx::tasking::TaskResult execute(const std::uint16_t /*worker_id*/) override
    {
        std::memset(_begin, '\0', _length);
        return mx::tasking::TaskResult::make_remove();
    }

private:
    void *_begin;
    const std::size_t _length;
};
} // namespace db::execution::compilation