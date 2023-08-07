#pragma once

#include <chrono>
#include <db/io/query_result.h>
#include <mx/tasking/profiling/task_tracer.h>
#include <mx/tasking/profiling/time.h>
#include <string>

namespace db::network {
class ServerResponse
{
public:
    enum Type
    {
        Success,             /// Result of inserts, updates, configurations, ...
        Error,               /// Error occured while query processing (i.e., cachted exception)
        GetConfiguration,    /// Specific configuration request
        QueryResult,         /// Time and records of a SELECT query
        LogicalPlan,         /// Shows the query plan
        TaskGraph,           /// Shows the task graph
        DataflowGraph,       /// Shows the task graph including count of emitted data between nodes
        PerformanceCounter,  /// List of hardware- and software performance counters sampled during query execution
        TaskLoad,            /// TODO: Not supported anymore!
        TaskTrace,           /// List of which task executed when
        FlounderCode,        /// Generated flounder code
        AssemblyCode,        /// Generated flounder code compiled to assembly
        SampleAssembly,      /// Assembly code with sampled instruction counters for a specific performance event
        SampleOperators,     /// Operators with samples instruction counters for a specific performance event
        SampleMemory,        /// Memory addresses sampled
        SampleMemoryHistory, /// Memory Traces
        DRAMBandwidth,       /// Sampled DRAM bandwith (needs root)
        Times,               /// Times per node
        ConnectionClosed
    };

    constexpr ServerResponse(const Type type) noexcept : _type(type) {}
    virtual ~ServerResponse() noexcept = default;

    [[nodiscard]] Type type() const noexcept { return _type; }

private:
    const Type _type;
};

template <ServerResponse::Type T> class EmptyResponse final : public ServerResponse
{
public:
    constexpr EmptyResponse() noexcept : ServerResponse(T) {}
    constexpr ~EmptyResponse() override = default;

    static std::string to_string()
    {
        auto response = std::string(sizeof(EmptyResponse), '\0');
        std::ignore = new (response.data()) EmptyResponse();
        return response;
    }
};

template <ServerResponse::Type T> class StringResponse : public ServerResponse
{
public:
    constexpr StringResponse() noexcept : ServerResponse(T) {}
    constexpr ~StringResponse() override = default;

    static std::string to_string(std::string_view message)
    {
        auto response = std::string(sizeof(StringResponse) + message.length(), '\0');
        std::ignore = new (response.data()) StringResponse();
        std::memcpy(response.data() + sizeof(StringResponse), message.data(), message.size());
        return response;
    }

    static std::string to_string(std::string &&message)
    {
        auto view = std::string_view{message};
        return StringResponse::to_string(view);
    }

    [[nodiscard]] virtual std::string_view data() const
    {
        return std::string_view{reinterpret_cast<const char *>(this + 1)};
    }
};

template <ServerResponse::Type T> class ResultStringResponse final : public StringResponse<T>
{
public:
    constexpr explicit ResultStringResponse(const std::chrono::microseconds time) noexcept
        : StringResponse<T>(), _time(time)
    {
    }
    constexpr ResultStringResponse(const std::chrono::microseconds time, const std::uint64_t count_records) noexcept
        : StringResponse<T>(), _time(time), _count_records(count_records)
    {
    }
    constexpr ~ResultStringResponse() override = default;

    static std::string to_string(const std::chrono::microseconds time_in_ms, std::string_view message)
    {
        auto response = std::string(sizeof(ResultStringResponse) + message.length(), '\0');
        std::ignore = new (response.data()) ResultStringResponse(time_in_ms);
        std::memcpy(response.data() + sizeof(ResultStringResponse), message.data(), message.size());
        return response;
    }

    static std::string to_string(const std::chrono::microseconds time_in_ms, const std::uint64_t count_records,
                                 std::string_view message)
    {
        auto response = std::string(sizeof(ResultStringResponse) + message.length(), '\0');
        std::ignore = new (response.data()) ResultStringResponse(time_in_ms, count_records);
        std::memcpy(response.data() + sizeof(ResultStringResponse), message.data(), message.size());
        return response;
    }

    static std::string to_string(const std::chrono::microseconds time_in_ms, std::string &&message)
    {
        auto view = std::string_view{message};
        return ResultStringResponse::to_string(time_in_ms, view);
    }

    static std::string to_string(const std::chrono::microseconds time_in_ms, const std::uint64_t count_records,
                                 std::string &&message)
    {
        auto view = std::string_view{message};
        return ResultStringResponse::to_string(time_in_ms, count_records, view);
    }

    [[nodiscard]] std::string_view data() const override
    {
        return std::string_view{reinterpret_cast<const char *>(this + 1)};
    }

    [[nodiscard]] std::chrono::microseconds time() const noexcept { return _time; }

    [[nodiscard]] std::optional<std::uint64_t> count_records() const noexcept { return _count_records; }

private:
    const std::chrono::microseconds _time;
    const std::optional<std::uint64_t> _count_records{std::nullopt};
};

using SuccessResponse = EmptyResponse<ServerResponse::Success>;
using ConnectionClosedResponse = EmptyResponse<ServerResponse::ConnectionClosed>;
using ErrorResponse = StringResponse<ServerResponse::Error>;
using GetConfigurationResponse = StringResponse<ServerResponse::GetConfiguration>;
using LogicalPlanResponse = ResultStringResponse<ServerResponse::LogicalPlan>;
using TaskGraphResponse = ResultStringResponse<ServerResponse::TaskGraph>;
using DataflowGraphResponse = ResultStringResponse<ServerResponse::DataflowGraph>;
using PerformanceCounterResponse = ResultStringResponse<ServerResponse::PerformanceCounter>;
using FlounderCodeResponse = ResultStringResponse<ServerResponse::FlounderCode>;
using AssemblyCodeResponse = ResultStringResponse<ServerResponse::AssemblyCode>;
using DRAMBandwidthResponse = ResultStringResponse<ServerResponse::DRAMBandwidth>;
using TimesResponse = ResultStringResponse<ServerResponse::Times>;
using SampleMemoryResponse = ResultStringResponse<ServerResponse::SampleMemory>;
using SampleMemoryHistoryResponse = ResultStringResponse<ServerResponse::SampleMemoryHistory>;

class QueryResultResponse final : public ServerResponse
{
public:
    QueryResultResponse(const std::chrono::microseconds time, const std::uint64_t count_rows)
        : ServerResponse(Type::QueryResult), _time(time), _count_rows(count_rows)
    {
    }

    constexpr ~QueryResultResponse() override = default;

    [[nodiscard]] std::uint64_t count_rows() const { return _count_rows; }

    [[nodiscard]] std::chrono::microseconds time() const { return _time; }

    static std::string to_string(const std::chrono::microseconds time, const std::uint64_t count_rows,
                                 io::QueryResult &&result)
    {
        const auto serialized_size = result.serialized_size();
        auto response = std::string(sizeof(QueryResultResponse) + serialized_size, '\0');
        std::ignore = new (response.data()) QueryResultResponse(time, count_rows);

        result.serialize(serialized_size,
                         reinterpret_cast<std::byte *>(std::uintptr_t(response.data() + sizeof(QueryResultResponse))));

        return response;
    }

    [[nodiscard]] const std::byte *data() const { return reinterpret_cast<const std::byte *>(this + 1); }

private:
    const std::chrono::microseconds _time;
    const std::uint64_t _count_rows;
};

class TaskLoadResponse final : public ServerResponse
{
public:
    TaskLoadResponse(const std::chrono::microseconds time, const std::uint64_t count_rows)
        : ServerResponse(Type::TaskLoad), _time(time), _count_rows(count_rows)
    {
    }

    constexpr ~TaskLoadResponse() override = default;

    [[nodiscard]] std::uint64_t count_rows() const { return _count_rows; }

    [[nodiscard]] std::chrono::microseconds time() const { return _time; }

    static std::string to_string(const std::chrono::microseconds time, const std::uint64_t count_rows,
                                 mx::tasking::profiling::WorkerIdleFrames &&idle_frames)
    {
        auto data = idle_frames.to_json();
        auto data_string = data.dump();
        auto response = std::string(sizeof(TaskLoadResponse) + data_string.size(), '\0');
        std::ignore = new (response.data()) TaskLoadResponse(time, count_rows);
        std::memmove(response.data() + sizeof(TaskLoadResponse), data_string.c_str(), data_string.size());
        return response;
    }

    [[nodiscard]] const std::byte *data() const { return reinterpret_cast<const std::byte *>(this + 1); }

private:
    const std::chrono::microseconds _time;
    const std::uint64_t _count_rows;
};

class TaskTraceResponse final : public ServerResponse
{
public:
    TaskTraceResponse(const std::chrono::microseconds time, const std::uint64_t count_rows)
        : ServerResponse(Type::TaskTrace), _time(time), _count_rows(count_rows)
    {
    }

    constexpr ~TaskTraceResponse() override = default;

    [[nodiscard]] std::uint64_t count_rows() const { return _count_rows; }

    [[nodiscard]] std::chrono::microseconds time() const { return _time; }

    static std::string to_string(const std::chrono::microseconds time, const std::uint64_t count_rows,
                                 mx::tasking::profiling::TaskTraces &&task_traces)
    {
        auto data = task_traces.to_json();
        auto data_string = data.dump();
        auto response = std::string(sizeof(TaskTraceResponse) + data_string.size(), '\0');
        std::ignore = new (response.data()) TaskTraceResponse(time, count_rows);
        std::memmove(response.data() + sizeof(TaskTraceResponse), data_string.c_str(), data_string.size());
        return response;
    }

    [[nodiscard]] const std::byte *data() const { return reinterpret_cast<const std::byte *>(this + 1); }

private:
    const std::chrono::microseconds _time;
    const std::uint64_t _count_rows;
};

template <ServerResponse::Type T> class SampleResponse final : public ServerResponse
{
public:
    SampleResponse(const std::chrono::microseconds time, const std::uint64_t count_rows,
                   const std::uint64_t count_samples, const float percentage)
        : ServerResponse(T), _time(time), _count_rows(count_rows), _count_samples(count_samples),
          _percentage(percentage)
    {
    }

    constexpr ~SampleResponse() override = default;

    [[nodiscard]] std::uint64_t count_rows() const { return _count_rows; }
    [[nodiscard]] std::chrono::microseconds time() const { return _time; }
    [[nodiscard]] std::uint64_t count_samples() const { return _count_samples; }
    [[nodiscard]] float percentage() const { return _percentage; }

    static std::string to_string(const std::chrono::microseconds time, const std::uint64_t count_rows,
                                 const std::uint64_t count_samples, const float percentage, std::string &&code)
    {
        auto response = std::string(sizeof(SampleResponse) + code.size(), '\0');
        std::ignore = new (response.data()) SampleResponse(time, count_rows, count_samples, percentage);
        std::memmove(response.data() + sizeof(SampleResponse), code.data(), code.size());
        return response;
    }

    [[nodiscard]] const std::string_view data() const
    {
        return std::string_view{reinterpret_cast<const char *>(this + 1)};
    }

private:
    const std::chrono::microseconds _time;
    const std::uint64_t _count_rows;
    const std::uint64_t _count_samples;
    const float _percentage;
};

using SampleAssemblyResponse = SampleResponse<ServerResponse::Type::SampleAssembly>;
using SampleOperatorsResponse = SampleResponse<ServerResponse::Type::SampleOperators>;

} // namespace db::network