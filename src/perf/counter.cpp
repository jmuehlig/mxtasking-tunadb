#include "counter.h"
#include <asm/unistd.h>
#include <cstring>
#include <sys/ioctl.h>
#include <tuple>
#include <unistd.h>
#include <utility>

using namespace perf;

Counter::Counter(std::string &&name, const std::uint64_t type, const std::uint64_t event_id,
                 const std::optional<std::uint64_t> msr_value)
    : _name(std::move(name))
{
    std::memset(&this->_perf_event_attribute, 0, sizeof(perf_event_attr));
    this->_perf_event_attribute.type = type;
    this->_perf_event_attribute.size = sizeof(perf_event_attr);
    this->_perf_event_attribute.config = event_id;
    if (msr_value.has_value())
    {
        this->_perf_event_attribute.config1 = msr_value.value();
        this->_perf_event_attribute.sample_period = 2000;
    }
    this->_perf_event_attribute.disabled = true;
    this->_perf_event_attribute.inherit = 1;
    this->_perf_event_attribute.exclude_kernel = false;
    this->_perf_event_attribute.exclude_hv = false;
    this->_perf_event_attribute.exclude_idle = false;
    this->_perf_event_attribute.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
}

bool Counter::open()
{
    this->_file_descriptor = syscall(__NR_perf_event_open, &this->_perf_event_attribute, 0, -1, -1, 0);
    return this->_file_descriptor >= 0;
}

void Counter::close()
{
    if (this->_file_descriptor > -1)
    {
        ::close(std::exchange(this->_file_descriptor, -1));
    }
}

bool Counter::start()
{
    ioctl(this->_file_descriptor, PERF_EVENT_IOC_RESET, 0);
    ioctl(this->_file_descriptor, PERF_EVENT_IOC_ENABLE, 0);
    return this->read(this->_start_value);
}

bool Counter::stop()
{
    const auto is_read_successful = this->read(this->_end_value);
    ioctl(this->_file_descriptor, PERF_EVENT_IOC_DISABLE, 0);
    return is_read_successful;
}

bool Counter::read(read_format &value) const
{
    return ::read(this->_file_descriptor, &value, sizeof(read_format)) == sizeof(read_format);
}

double Counter::get(const read_format &start, const read_format &end) noexcept
{
    const auto multiplexing_correction =
        double(end.time_enabled - start.time_enabled) / double(end.time_running - start.time_running);
    return double(end.value - start.value) * multiplexing_correction;
}

double Counter::get() const
{
    return Counter::get(this->_start_value, this->_end_value);
}

Counter::read_format Counter::now() const
{
    read_format now_;
    this->read(now_);
    return now_;
}

CounterManager::CounterManager(const std::vector<CounterDescription> &counters)
{
    for (const auto &counter : counters)
    {
        this->add(counter);
    }
}

CounterManager::~CounterManager()
{
    close();
}

void CounterManager::add(const CounterDescription &counter_description)
{
    this->_counters.emplace_back(counter_description);
}

bool CounterManager::open()
{
    for (auto &counter : this->_counters)
    {
        if (counter.open() == false)
        {
            return false;
        }
    }

    return true;
}

void CounterManager::close()
{
    for (auto &counter : this->_counters)
    {
        counter.close();
    }
}

void CounterManager::start()
{
    for (auto &counter : this->_counters)
    {
        std::ignore = counter.start();
    }
}

void CounterManager::stop()
{
    for (auto &counter : this->_counters)
    {
        std::ignore = counter.stop();
    }
}

bool GroupCounter::open()
{
    auto leader_file_descriptor = std::int32_t{-1};
    for (auto &member : this->_members)
    {
        const auto is_leader = leader_file_descriptor == -1;

        std::memset(&member.event_attribute(), 0, sizeof(perf_event_attr));
        member.event_attribute().type = member.description().type();
        member.event_attribute().size = sizeof(perf_event_attr);
        member.event_attribute().config = member.description().event_id();
        member.event_attribute().disabled = is_leader;

        if (is_leader)
        {
            member.event_attribute().read_format =
                PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING | PERF_FORMAT_GROUP | PERF_FORMAT_ID;
        }
        else
        {
            member.event_attribute().read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
        }

        member = syscall(__NR_perf_event_open, &member.event_attribute(), 0, -1, leader_file_descriptor, 0);
        if (is_leader)
        {
            leader_file_descriptor = member.file_descriptor();
        }

        if (member.is_open() == false)
        {
            return false;
        }

        ::ioctl(member.file_descriptor(), PERF_EVENT_IOC_ID, &member.id());
    }

    return true;
}

void GroupCounter::close()
{
    for (auto &member : this->_members)
    {
        if (member.is_open())
        {
            ::close(member.file_descriptor());
            member = -1;
        }
    }
}

bool GroupCounter::start()
{
    if (this->_members.empty() == false)
    {
        ioctl(this->leader_file_descriptor(), PERF_EVENT_IOC_RESET, 0);
        ioctl(this->leader_file_descriptor(), PERF_EVENT_IOC_ENABLE, 0);

        const auto read_size = ::read(this->leader_file_descriptor(), &this->_start_value, sizeof(read_format));
        return read_size > 0U;
    }

    return false;
}

bool GroupCounter::stop()
{
    if (this->_members.empty() == false)
    {
        const auto read_size = ::read(this->leader_file_descriptor(), &this->_end_value, sizeof(read_format));
        ioctl(this->leader_file_descriptor(), PERF_EVENT_IOC_DISABLE, 0);
        return read_size > 0U;
    }

    return false;
}

double GroupCounter::get(const std::string &name) const
{
    const auto multiplexing_correction = double(this->_end_value.time_enabled - this->_start_value.time_enabled) /
                                         double(this->_end_value.time_running - this->_start_value.time_running);

    for (auto i = 0U; i < this->_members.size(); ++i)
    {
        if (this->_members[i].description().name() == name)
        {
            const auto start_value = this->_start_value.values[i].value;
            const auto end_value = this->_end_value.values[i].value;

            return double(end_value - start_value) * multiplexing_correction;
        }
    }

    return 0;
}

std::unordered_map<std::string, double> GroupCounter::get() const
{
    auto values = std::unordered_map<std::string, double>{};
    values.reserve(this->_members.size());

    const auto multiplexing_correction = double(this->_end_value.time_enabled - this->_start_value.time_enabled) /
                                         double(this->_end_value.time_running - this->_start_value.time_running);

    for (const auto &member : this->_members)
    {
        const auto start_value = GroupCounter::get_value_for_id(this->_start_value, member.id());
        const auto end_value = GroupCounter::get_value_for_id(this->_end_value, member.id());

        const auto value = double(end_value - start_value) * multiplexing_correction;

        values.insert(std::make_pair(member.description().name(), value));
    }

    return values;
}