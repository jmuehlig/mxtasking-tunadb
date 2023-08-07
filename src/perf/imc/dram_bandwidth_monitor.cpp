#include "dram_bandwidth_monitor.h"

using namespace perf;

void DRAMBandwidthMonitor::start()
{
    _samples.clear();

    this->_is_running = true;

    this->_sample_thread = std::thread([&is_running = this->_is_running, &imc = this->_imc_controller,
                                        &sample_period = this->_sample_period_us, &samples = this->_samples] {
        while (is_running)
        {
            const auto now = std::chrono::steady_clock::now();
            const auto reads = imc.dram_data_reads();
            const auto writes = imc.dram_data_writes();
            samples.emplace_back(now, reads, writes);

            std::this_thread::sleep_for(std::chrono::microseconds(sample_period));
        }
    });
}

std::vector<DRAMBandwidthMonitor::BandwithSample> DRAMBandwidthMonitor::stop(
    const std::chrono::steady_clock::time_point start)
{
    this->_is_running = false;

    /// Wait for sample thread to complete.
    this->_sample_thread.join();

    auto samples = std::vector<DRAMBandwidthMonitor::BandwithSample>{};
    samples.reserve(this->_samples.size());

    if (this->_samples.empty() == false)
    {
        for (auto i = 1U; i < this->_samples.size(); ++i)
        {
            const auto &sample = this->_samples[i];
            const auto &last_sample = this->_samples[i - 1U];

            const auto timestamp = std::get<0>(sample);
            const auto last_timestmap = std::get<0>(last_sample);
            const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(timestamp - last_timestmap);

            const auto reads = std::get<1>(sample);
            const auto last_reads = std::get<1>(last_sample);
            const auto read_gb_per_s = DRAMBandwidthMonitor::gb_per_second(last_reads, reads, duration);

            const auto writes = std::get<2>(sample);
            const auto last_writes = std::get<2>(last_sample);
            const auto written_gb_per_s = DRAMBandwidthMonitor::gb_per_second(last_writes, writes, duration);

            const auto local_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(timestamp - start);
            samples.emplace_back(std::uint64_t(local_timestamp.count()), read_gb_per_s, written_gb_per_s);
        }
    }

    return samples;
}

double DRAMBandwidthMonitor::gb_per_second(std::uint32_t last, std::uint32_t now, std::chrono::nanoseconds duration)
{
    auto cache_lines = now - last;
    if (now < last)
    {
        cache_lines = std::numeric_limits<std::uint32_t>::max() - last + now;
    }
    return (cache_lines * 64U) / (1024.0 * 1024.0 * 1024.0) / duration.count() * 1000000000.0;
}