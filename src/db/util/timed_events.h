#pragma once
#include <chrono>
#include <mx/synchronization/spinlock.h>
#include <string>
#include <utility>
#include <vector>

namespace db::util {
class TimedEvents
{
public:
    using event_t = std::pair<std::chrono::steady_clock::time_point, std::string>;

    TimedEvents() { _latch.unlock(); }

    ~TimedEvents() = default;

    void emplace_back(std::string &&event_name)
    {
        const auto now = std::chrono::steady_clock::now();

        _latch.lock();
        const auto event_iterator = std::find_if(_events.begin(), _events.end(), [&event_name](const auto &event) {
            return std::get<1>(event) == event_name;
        });
        if (event_iterator == _events.end())
        {
            _events.emplace_back(now, std::move(event_name));
        }
        _latch.unlock();
    }

    [[nodiscard]] const std::vector<event_t> &events() const noexcept { return _events; }

    [[nodiscard]] std::vector<std::pair<std::uint64_t, std::string>> normalized(
        const std::chrono::steady_clock::time_point start)
    {
        auto normalized_events = std::vector<std::pair<std::uint64_t, std::string>>{};
        std::transform(
            _events.begin(), _events.end(), std::back_inserter(normalized_events), [start](const auto &event) {
                const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::get<0>(event) - start);
                return std::make_pair(duration.count(), std::get<1>(event));
            });

        return normalized_events;
    }

private:
    mx::synchronization::Spinlock _latch;
    std::vector<event_t> _events;
};
} // namespace db::util