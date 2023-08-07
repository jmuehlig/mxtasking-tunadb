# Performance Counter

This package includes a more user-friendly wrapper for the linux perf interface, which is used to collect performance counter statistics and samples.

## Simple Perfomance Counters
Counters are defined in [src/perf/counter_description.h](counter_description.h) (most of them are particularly defined for Intel's _Cascade Lake_ architecture but can be adapted with minor changes).
Access to the counters can be gained by creating single or grouped counters (see [src/perf/counter.h](counter.h)).

Example usage:

    auto group_counter = perf::GroupCounter{
        perf::CounterDescription::CYCLES, 
        perf::CounterDescription::INSTRUCTIONS};
    group_counter.open();
    group_counter.start();
    /// do some work here...
    group_counter.stop();

    for (auto &[name, value] : group_counter.get())
    {
        std::cout << "Counter " << name << " = " 
            << value << std::endl;
    }

## Sampling
Perf can also be instructed to sample values (e.g., instructions, memory addresses) when a specific event happens.
The [src/perf/sample.h](sample.h) contains a wrapper to sample those values using performance counters as an event.

Example usage:

    /// Create a sampler that samples instruction addresses when experiencing cache misses, sample once per ms.
    auto sample = perf::Sample{perf::CounterDescription::CACHE_MISSES, perf::Sample::Type::Instruction, 1000};

    sample.open();
    sample.start();
    /// do some work here...
    sample.stop();

    /// Get the number of cache misses per sampled instruction.
    auto samples = sample.aggregate();
    for (auto &[instr, value] : samples.samples())
    {
        std::cout << "Instruction " << instr << " = " 
            << value << std::endl;
    }

## Library
The performance counters, as shipped with TunaDB, can be used as a standalone library.
Use `make perf` to build and include the library ([lib/libperf.so](lib/libperf.so)) and header files to your project.
