#pragma once
#include "executable.h"
#include <jitprofiling.h>
#include <perf/jit_map.h>
#include <string>

namespace flounder {
class PerfJitMap
{
public:
    PerfJitMap() = default;
    ~PerfJitMap()
    {
        if (_jit_map.empty() == false)
        {
            _jit_map.write();
        }

        if (_jit_dump.empty() == false)
        {
            _jit_dump.write();
        }
    }

    void make_visible(const Executable &executable, std::string &&name)
    {
        _jit_map.emplace_back(perf::SymbolEntry{executable.base(), executable.code_size(), std::string(name)});
        _jit_dump.emplace_back(perf::SymbolEntry{executable.base(), executable.code_size(), std::move(name)});
    }

private:
    perf::JITMap _jit_map;
    perf::JITDump _jit_dump;
};

class VTuneJitAPI
{
public:
    [[nodiscard]] static bool is_sampling() { return iJIT_IsProfilingActive() == iJIT_SAMPLING_ON; }

    static void make_visible(const Executable &executable, std::string &&name)
    {
        auto vtune_method = iJIT_Method_Load_V2{};
        vtune_method.method_id = iJIT_GetNewMethodID();
        vtune_method.method_name = name.data();
        vtune_method.method_load_address = reinterpret_cast<void *>(executable.base());
        vtune_method.method_size = executable.code_size();

        const auto is_registered =
            iJIT_NotifyEvent(iJVM_EVENT_TYPE_METHOD_LOAD_FINISHED_V2, static_cast<void *>(&vtune_method));

        if (VTuneJitAPI::is_sampling())
        {
            assert(is_registered);
        }
    }
};
} // namespace flounder