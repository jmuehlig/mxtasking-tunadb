#pragma once

#include <mx/tasking/task.h>

namespace db::index::blinktree {
template <typename K, typename V, class L> class Task : public mx::tasking::TaskInterface
{
public:
    constexpr static inline auto TRACE_ID = 0b1000011000000000000;

    constexpr Task(const K key, L &listener) : _listener(listener), _key(key) {}
    ~Task() override = default;

protected:
    L &_listener;
    K _key;
};
} // namespace db::index::blinktree