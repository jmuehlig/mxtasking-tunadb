#pragma once

#include "counter_description.h"
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <linux/perf_event.h>
#include <numeric>
#include <unordered_map>
#include <utility>
#include <vector>

namespace perf {
class AggregatedSamples
{
public:
    explicit AggregatedSamples(std::unordered_map<std::uintptr_t, std::uint64_t> &&samples,
                               std::unordered_map<std::uintptr_t, std::vector<std::uintptr_t>> &&callchains) noexcept
        : _samples(samples), _callchains(std::move(callchains))
    {
        _count_samples =
            std::accumulate(_samples.begin(), _samples.end(), 0U,
                            [](const auto sum, const auto &sample_entry) { return sum + sample_entry.second; });
    }

    ~AggregatedSamples() = default;

    [[nodiscard]] std::uint64_t count() const noexcept { return _count_samples; }
    [[nodiscard]] std::pair<std::uint64_t, float> count_and_percentage(
        const std::uintptr_t instruction_address) const noexcept
    {
        if (_count_samples == 0U)
        {
            return std::make_pair(0U, .0);
        }

        auto count = 0UL;

        /// Direct samples.
        if (auto iterator = _samples.find(instruction_address); iterator != _samples.end())
        {
            count += iterator->second;
        }

        /// Callchain samples.
        for (const auto &[callchain_top_insutrction_address, callchain] : _callchains)
        {
            if (auto callchain_iterator = std::find(callchain.begin(), callchain.end(), instruction_address);
                callchain_iterator != callchain.end())
            {
                if (auto iterator = _samples.find(callchain_top_insutrction_address); iterator != _samples.end())
                {
                    std::cout << instruction_address << " is in callchain of " << callchain_top_insutrction_address
                              << std::endl;
                    count += iterator->second;
                }
            }
        }

        if (count > 0U)
        {
            return std::make_pair(count, 100.0 / _count_samples * count);
        }

        return std::make_pair(0U, .0);
    }

    [[nodiscard]] const std::unordered_map<std::uintptr_t, std::uint64_t> &samples() const noexcept { return _samples; }

    void insert(AggregatedSamples &&other)
    {
        _samples.insert(other._samples.begin(), other._samples.end());
        _callchains.merge(other._callchains);
        _count_samples += other._count_samples;
    }

private:
    std::unordered_map<std::uintptr_t, std::uint64_t> _samples;
    std::unordered_map<std::uintptr_t, std::vector<std::uintptr_t>> _callchains;
    std::uint64_t _count_samples;
};

class HistoricalSamples
{
public:
    explicit HistoricalSamples(std::vector<std::pair<std::uint64_t, std::uintptr_t>> &&samples) noexcept
        : _samples(samples)
    {
    }

    ~HistoricalSamples() = default;

    [[nodiscard]] const std::vector<std::pair<std::uint64_t, std::uintptr_t>> &samples() const noexcept
    {
        return _samples;
    }
    [[nodiscard]] std::vector<std::pair<std::uint64_t, std::uintptr_t>> &samples() noexcept { return _samples; }

    void insert(HistoricalSamples &&other)
    {
        std::move(other._samples.begin(), other._samples.end(), std::back_inserter(_samples));
    }

private:
    std::vector<std::pair<std::uint64_t, std::uintptr_t>> _samples;
};

class alignas(64) Sample
{
public:
    enum Type : std::uint64_t
    {
        Instruction = PERF_SAMPLE_IP,
        Address = PERF_SAMPLE_ADDR,
        PhysicalAddress = PERF_SAMPLE_PHYS_ADDR,
        Time = PERF_SAMPLE_TIME,
        Callchain = PERF_SAMPLE_CALLCHAIN,
    };

    Sample(std::uint64_t type, std::uint64_t event_id, std::uint64_t sample_type, std::uint64_t sample_frequency);
    Sample(const CounterDescription &description, const std::uint64_t sample_type, const std::uint64_t sample_frequency)
        : Sample(description.type(), description.event_id(), sample_type, sample_frequency)
    {
    }
    Sample(Sample &&other) noexcept
        : _file_descriptor(other._file_descriptor), _buffer(other._buffer), _buffer_size(other._buffer_size)
    {
        std::memcpy(&_perf_event_attribute, &other._perf_event_attribute, sizeof(perf_event_attr));
    }

    ~Sample() = default;

    Sample &operator=(Sample &&other) noexcept
    {
        _sample_type_flags = other._sample_type_flags;
        _file_descriptor = std::exchange(other._file_descriptor, -1);
        _buffer = std::exchange(other._buffer, nullptr);
        _buffer_size = other._buffer_size;
        std::memcpy(&_perf_event_attribute, &other._perf_event_attribute, sizeof(perf_event_attr));

        return *this;
    }

    /**
     * Calls perf_open to open the file descriptor for this performance counter.
     *
     * @return True, when the syscall was successful.
     */
    [[nodiscard]] bool open();

    /**
     * Enables and reads the current value of the performance counter.
     */
    void start() const;

    /**
     * Disables and reads the current value of the performance counter.
     */
    void stop() const;

    /**
     * @return List of aggregated samples (value -> count).
     */
    AggregatedSamples aggregate();

    /**
     * @return List of historical samples (time, value).
     */
    HistoricalSamples get();

    /**
     * Closes the sample.
     */
    void close();

    [[nodiscard]] bool is_historical() const noexcept
    {
        return static_cast<bool>(_sample_type_flags & Sample::Type::Time);
    }
    [[nodiscard]] bool is_callchain() const noexcept
    {
        return static_cast<bool>(_sample_type_flags & Sample::Type::Callchain);
    }

    [[nodiscard]] std::pair<std::uintptr_t, std::uintptr_t> buffer_range() const noexcept { return _buffer_range; }

private:
    /// Flag for historical (or otherwise aggregated) data.
    std::uint64_t _sample_type_flags;

    /// File descriptor of the opened counter.
    std::int32_t _file_descriptor = -1;

    std::int32_t _error{0U};

    /// Perf event used for opening the counter.
    perf_event_attr _perf_event_attribute;

    /// Pointer to the (mmaped) buffer where the samples are stored.
    void *_buffer{nullptr};

    /// Size of the buffer (8192 pages for the samples + 1 page for the buffer metadata).
    std::size_t _buffer_size{4096U * (8192U + 1U)};

    /// Range of the buffer, needed for memory tagging.
    std::pair<std::uintptr_t, std::uintptr_t> _buffer_range{0U, 0U};

    /**
     * Reads the data from the buffer and calls the callback for every sample.
     *
     * @param callback Callback to call for every sample.
     */
    void read(std::function<void(perf_event_header *, void *)> &&callback);
};

class SampleManager
{
public:
private:
};
} // namespace perf