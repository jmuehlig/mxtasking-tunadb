#pragma once
#include "program.h"
#include <flounder/annotations.h>
#include <flounder/compilation/compiler.h>
#include <perf/counter.h>

namespace db::execution::compilation {
class ProfileAggregator
{
public:
    constexpr ProfileAggregator() noexcept = default;
    ~ProfileAggregator() noexcept = default;

    void add(const std::uint32_t sampled_records, const float value) noexcept
    {
        _sampled_records += sampled_records;
        _sampled_value += value;
    }

    void clear()
    {
        _sampled_records = 0U;
        _sampled_value = 0;
    }

    [[nodiscard]] float value() const noexcept { return _sampled_value / _sampled_records; }

    [[nodiscard]] std::uint32_t sampled_records() const noexcept { return _sampled_records; }

private:
    /// Number of profiled records.
    std::uint32_t _sampled_records{0U};

    /// Summed profiling value.
    float _sampled_value{0};
};

class ProfileGuidedOptimizer
{
public:
    ProfileGuidedOptimizer(MultiversionProgram &multiversion_program, const perf::Counter &performance_counter,
                           flounder::Compiler &compiler)
        : _is_optimizing(multiversion_program.capacity() > 0U), _performance_counter(performance_counter),
          _scores(multiversion_program.capacity() + 1U, 0), _program(multiversion_program), _compiler(compiler)
    {
    }

    [[nodiscard]] bool is_optimizing() const noexcept { return _is_optimizing; }

    void start_profiling() { _profiling_start = _performance_counter.now(); }

    void end_profiling(std::uint32_t count_profiled_tuples);

private:
    /// Flag if PGO is in process.
    bool _is_optimizing;

    /// Aggregator for current profile.
    ProfileAggregator _aggregator;

    /// Counter to measure performance during optimizing.
    const perf::Counter &_performance_counter;

    /// List of scores for each version.
    std::vector<double> _scores;

    /// Number of the profiled version of the program.
    std::uint32_t _current_version{0U};

    /// Value of the performance counter at the start of the sample.
    perf::Counter::read_format _profiling_start;

    /// Program with versions of the executable and flounder code.
    MultiversionProgram &_program;

    /// Annotation including prefetch distance, record size, etc..
    //    flounder::ScanPrefetchAnnotation *_prefetch_annotation{nullptr};

    /// Compiler to recompile into executable.
    flounder::Compiler &_compiler;

    /**
     * Optimizes the given program.
     *
     * @param program Program to optimize.
     */
    void optimize(flounder::Program &program);

    /**
     * Examines the best executable and updates the callback to the best one.
     */
    void apply_best_version();
};
} // namespace db::execution::compilation