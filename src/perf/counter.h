#pragma once
#include "counter_description.h"
#include <algorithm>
#include <array>
#include <linux/perf_event.h>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace perf {
/**
 * Represents a Linux Performance Counter.
 */
class Counter
{
public:
    struct read_format
    {
        std::uint64_t value{0U};
        std::uint64_t time_enabled{0U};
        std::uint64_t time_running{0U};
    };

    Counter(std::string &&name, std::uint64_t type, std::uint64_t event_id, std::optional<std::uint64_t> msr_value);

    Counter(std::string &&name, std::uint64_t type, std::uint64_t event_id)
        : Counter(std::move(name), type, event_id, std::nullopt)
    {
    }

    explicit Counter(const CounterDescription &description)
        : Counter(std::string{description.name()}, description.type(), description.event_id(), description.msr_value())
    {
    }

    Counter(Counter &&) noexcept = default;
    ~Counter() = default;

    /**
     * Calls perf_open to open the file descriptor for this performance counter.
     *
     * @return True, when the syscall was successful.
     */
    [[nodiscard]] bool open();

    /**
     * Closes the opened file descriptor for the counter.
     */
    void close();

    /**
     * Enables and reads the current value of the performance counter.
     *
     * @return True, when the read was successful.
     */
    [[nodiscard]] bool start();

    /**
     * Disables and reads the current value of the performance counter.
     *
     * @return True, when the read was successful.
     */
    [[nodiscard]] bool stop();

    /**
     * @return The value of the performance counter (end - start).
     */
    [[nodiscard]] double get() const;

    /**
     * Calculates the difference between start and end.
     *
     * @param start Start value of the counter.
     * @param end End value of the counter.
     * @return The value of the performance counter between start and end.
     */
    [[nodiscard]] static double get(const read_format &start, const read_format &end) noexcept;

    /**
     * @return Current value of the counter.
     */
    [[nodiscard]] read_format now() const;

    /**
     * @return The name of the performance counter.
     */
    [[nodiscard]] const std::string &name() const { return _name; }

    explicit operator const std::string &() const { return name(); }

    bool operator==(const std::string &name) const { return _name == name; }

private:
    /// Name of the counter (can be anything).
    std::string _name;

    /// File descriptor of the opened counter.
    std::int32_t _file_descriptor = -1;

    /// Perf event to read from.
    perf_event_attr _perf_event_attribute;

    /// Value of the counter on start().
    read_format _start_value{};

    /// Value of the counter after stop().
    read_format _end_value{};

    /**
     * Reads the current value into the given value.
     *
     * @param value Value to read current state into.
     * @return True, if the read operation was successful.
     */
    [[nodiscard]] bool read(read_format &value) const;
};

/**
 * Holds a set of performance counter and starts/stops them together.
 */
class alignas(64) CounterManager
{
public:
    CounterManager() noexcept = default;
    CounterManager(const std::vector<CounterDescription> &counters);
    CounterManager(CounterManager &&) noexcept = default;
    ~CounterManager();

    CounterManager &operator=(CounterManager &&) noexcept = default;

    /**
     * Add the given counter to the environment.
     *
     * @param counter_description Description of the counter.
     */
    void add(const CounterDescription &counter_description);

    /**
     * Opens all counter.
     *
     * @return True, if all counter could be opened.
     */
    [[nodiscard]] bool open();

    /**
     * Cloeses all counter.
     */
    void close();

    /**
     * Start all added counters.
     */
    void start();

    /**
     * Stop all counters.
     */
    void stop();

    [[nodiscard]] double operator[](const CounterDescription &counter_description) const
    {
        return this->operator[](counter_description.name());
    }

    [[nodiscard]] double operator[](const std::string &name) const
    {
        auto counter_iterator = std::find(_counters.begin(), _counters.end(), name);
        if (counter_iterator != _counters.end())
        {
            return counter_iterator->get();
        }

        return 0.0;
    }

    [[nodiscard]] const std::vector<Counter> &counters() const noexcept { return _counters; }
    [[nodiscard]] std::vector<Counter> &counters() noexcept { return _counters; }

private:
    std::vector<Counter> _counters;
};

class alignas(64) GroupCounter
{
public:
    constexpr static inline auto MAX_MEMBERS = 5U;

    struct read_format
    {
        struct value
        {
            std::uint64_t value;
            std::uint64_t id;
        };

        std::uint64_t count_members;
        std::uint64_t time_enabled{0U};
        std::uint64_t time_running{0U};
        std::array<value, MAX_MEMBERS> values;
    };

    class Member
    {
    public:
        explicit Member(const CounterDescription &description) noexcept : _description(description) {}

        Member(Member &&) noexcept = default;

        ~Member() noexcept = default;

        Member &operator=(Member &&) noexcept = default;

        [[nodiscard]] const CounterDescription &description() const noexcept { return _description; }
        [[nodiscard]] perf_event_attr &event_attribute() noexcept { return _event_attribute; }
        [[nodiscard]] std::uint64_t &id() noexcept { return _id; }
        [[nodiscard]] std::uint64_t id() const noexcept { return _id; }
        [[nodiscard]] std::int32_t file_descriptor() const noexcept { return _file_descriptor; }
        [[nodiscard]] bool is_open() const noexcept { return _file_descriptor > -1; }

        Member &operator=(const std::int32_t file_descriptor) noexcept
        {
            _file_descriptor = file_descriptor;
            return *this;
        }

    private:
        CounterDescription _description;
        perf_event_attr _event_attribute;
        std::uint64_t _id{0U};
        std::int32_t _file_descriptor{-1};
    };

    explicit GroupCounter(const std::vector<CounterDescription> &members)
    {
        _members.reserve(members.size());
        for (const auto &member : members)
        {
            _members.emplace_back(Member{member});
        }
    }

    GroupCounter(GroupCounter &&) noexcept = default;
    ~GroupCounter() = default;

    GroupCounter &operator=(GroupCounter &&) noexcept = default;

    /**
     * Calls perf_open to open the file descriptor for this performance counter.
     *
     * @return True, when the syscall was successful.
     */
    [[nodiscard]] bool open();

    /**
     * Closes the opened file descriptor for the counter.
     */
    void close();

    /**
     * Enables and reads the current value of the performance counter.
     *
     * @return True, when the read was successful.
     */
    [[nodiscard]] bool start();

    /**
     * Disables and reads the current value of the performance counter.
     *
     * @return True, when the read was successful.
     */
    [[nodiscard]] bool stop();

    /**
     * @return The value of the performance counter (end - start).
     */
    [[nodiscard]] double get(const std::string &name) const;

    /**
     * @return All counter values.
     */
    [[nodiscard]] std::unordered_map<std::string, double> get() const;

private:
    /// Group members and their file descriptors.
    std::vector<Member> _members;

    /// Value of the counter on start().
    read_format _start_value{};

    /// Value of the counter after stop().
    read_format _end_value{};

    /**
     * @return The file descriptor of the leader or -1 if empty.
     */
    [[nodiscard]] std::int32_t leader_file_descriptor() const noexcept
    {
        return _members.empty() == false ? _members[0U].file_descriptor() : -1;
    }

    [[nodiscard]] static double get_value_for_id(const read_format &values, const std::uint64_t id) noexcept
    {
        for (auto i = 0U; i < values.count_members; ++i)
        {
            if (values.values[i].id == id)
            {
                return values.values[i].value;
            }
        }

        return 0;
    }
};
} // namespace perf