#pragma once
#include "imc_controller.h"
#include <chrono>
#include <thread>
#include <tuple>
#include <vector>

namespace perf {
class DRAMBandwidthMonitor
{
public:
    class BandwithSample
    {
    public:
        constexpr BandwithSample(const std::uint64_t timestamp, const double read_gb_per_second,
                                 const double write_gb_per_second) noexcept
            : _timestamp(timestamp), _read_gb_per_second(read_gb_per_second), _write_gb_per_second(write_gb_per_second)
        {
        }

        ~BandwithSample() noexcept = default;

        [[nodiscard]] std::uint64_t timestamp() const noexcept { return _timestamp; }
        [[nodiscard]] double read_gb_per_second() const noexcept { return _read_gb_per_second; }
        [[nodiscard]] double write_gb_per_second() const noexcept { return _write_gb_per_second; }
        [[nodiscard]] double gb_per_second() const noexcept { return _read_gb_per_second + _write_gb_per_second; }

    private:
        std::uint64_t _timestamp;
        double _read_gb_per_second;
        double _write_gb_per_second;
    };

    DRAMBandwidthMonitor(const std::uint32_t sample_period_us) : _sample_period_us(sample_period_us)
    {
        _samples.reserve(5U * 1000 * 1000 / sample_period_us);
    }
    ~DRAMBandwidthMonitor() = default;

    void start();
    [[nodiscard]] std::vector<BandwithSample> stop(std::chrono::steady_clock::time_point start);

private:
    IMCController _imc_controller;
    const std::uint32_t _sample_period_us;
    bool _is_running{false};
    std::thread _sample_thread;

    std::vector<std::tuple<std::chrono::steady_clock::time_point, std::uint32_t, std::uint32_t>> _samples;

    [[nodiscard]] static double gb_per_second(std::uint32_t last, std::uint32_t now, std::chrono::nanoseconds duration);
};
} // namespace perf