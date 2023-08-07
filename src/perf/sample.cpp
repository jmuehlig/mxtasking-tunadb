#include "sample.h"
#include <asm/unistd.h>
#include <cerrno>
#include <cstring>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <unistd.h>

using namespace perf;

Sample::Sample(const std::uint64_t type, const std::uint64_t event_id, const std::uint64_t sample_type,
               const std::uint64_t sample_frequency)
    : _sample_type_flags(sample_type)
{
    std::memset(&this->_perf_event_attribute, 0, sizeof(perf_event_attr));
    this->_perf_event_attribute.type = type;
    this->_perf_event_attribute.size = sizeof(perf_event_attr);
    this->_perf_event_attribute.config = event_id;
    this->_perf_event_attribute.sample_type = sample_type;
    this->_perf_event_attribute.sample_freq = sample_frequency;
    this->_perf_event_attribute.freq = 1;
    this->_perf_event_attribute.disabled = true;
    this->_perf_event_attribute.mmap = 1U;

    if (static_cast<bool>(sample_type & Type::Address) || static_cast<bool>(sample_type & Type::PhysicalAddress))
    {
        this->_perf_event_attribute.precise_ip = 2U;
    }
    else if (static_cast<bool>(sample_type & Type::Instruction))
    {
        this->_perf_event_attribute.precise_ip = 0U;
    }
}

bool Sample::open()
{
    this->_file_descriptor = syscall(__NR_perf_event_open, &this->_perf_event_attribute, 0, -1, -1, 0);
    if (this->_file_descriptor < 0)
    {
        this->_error = errno;
        return false;
    }

    this->_buffer = ::mmap(nullptr, this->_buffer_size, PROT_READ | PROT_WRITE, MAP_SHARED, this->_file_descriptor, 0);

    return this->_buffer != MAP_FAILED;
}

void Sample::start() const
{
    ioctl(this->_file_descriptor, PERF_EVENT_IOC_RESET, 0);
    ioctl(this->_file_descriptor, PERF_EVENT_IOC_ENABLE, 0);
}

void Sample::stop() const
{
    ioctl(this->_file_descriptor, PERF_EVENT_IOC_DISABLE, 0);
}

#include <iostream>
AggregatedSamples Sample::aggregate()
{
    /// Map of aggregated samples (instruction -> count).
    auto values = std::unordered_map<std::uintptr_t, std::uint64_t>{};

    /// Callchains
    auto callchains = std::unordered_map<std::uintptr_t, std::vector<std::uintptr_t>>{};

    this->read(
        [&values, &callchains, is_callchain = this->is_callchain()](perf_event_header *event_header, void *event) {
            const auto sampled_value = std::uintptr_t(*reinterpret_cast<std::uint64_t *>(event));

            /// Aggregate (count).
            if (auto values_iterator = values.find(sampled_value); values_iterator != values.end())
            {
                values_iterator->second += 1U;
            }
            else
            {
                values.insert(std::make_pair(sampled_value, 1U));
            }

            if (is_callchain)
            {
                auto *callchain = reinterpret_cast<std::uint64_t *>(event) + 1U;
                const auto count_stack_size = callchain[0U];

                if (count_stack_size > 0U)
                {
                    auto ips = std::vector<std::uintptr_t>{};
                    ips.reserve(count_stack_size);

                    for (auto i = 1U; i <= count_stack_size; ++i)
                    {
                        ips.emplace_back(callchain[i]);
                    }

                    if (auto values_iterator = callchains.find(sampled_value); values_iterator != callchains.end())
                    {
                        std::move(ips.begin(), ips.end(), std::back_inserter(values_iterator->second));
                    }
                    else
                    {
                        callchains.insert(std::make_pair(sampled_value, std::move(ips)));
                    }
                }
            }
        });

    return AggregatedSamples{std::move(values), std::move(callchains)};
}

HistoricalSamples Sample::get()
{
    /// Map of aggregated samples (instruction -> count).
    auto values = std::vector<std::pair<std::uint64_t, std::uintptr_t>>{};
    values.reserve(1 << 13U);

    this->read([&values](perf_event_header * /*event_header*/, void *event) {
        const auto time = *reinterpret_cast<std::uint64_t *>(event);
        const auto value =
            std::uintptr_t(*reinterpret_cast<std::uint64_t *>(std::uintptr_t(event) + sizeof(std::uint64_t)));

        values.emplace_back(time, value);
    });

    return HistoricalSamples{std::move(values)};
}

void Sample::read(std::function<void(perf_event_header *, void *)> &&callback)
{
    auto *mmap_page = reinterpret_cast<perf_event_mmap_page *>(this->_buffer);

    /// When the ringbuffer is empty or already read, there is nothing to do.
    if (mmap_page->data_tail >= mmap_page->data_head)
    {
        return;
    }

    /// The buffer starts at page 1 (from 0).
    auto iterator = std::uintptr_t(this->_buffer) + 4096U;

    /// data_head is the size (in bytes) of the samples.
    const auto end = iterator + mmap_page->data_head;

    /// Remember the range for memory tracing.
    this->_buffer_range = std::make_pair(std::uintptr_t(this->_buffer), end);

    while (iterator < end)
    {
        auto *event_header = reinterpret_cast<perf_event_header *>(iterator);

        if (event_header->type == PERF_RECORD_SAMPLE && static_cast<bool>(event_header->misc & PERF_RECORD_MISC_USER))
        {
            auto *event = reinterpret_cast<void *>(event_header + 1U);
            callback(event_header, event);
        }

        /// Go to the next sample.
        iterator += event_header->size;
    }
}

void Sample::close()
{
    munmap(this->_buffer, this->_buffer_size);
    ::close(this->_file_descriptor);
}