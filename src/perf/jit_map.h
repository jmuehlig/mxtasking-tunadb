#pragma once
#include <cstdint>
#include <elf.h>
#include <string>
#include <tuple>
#include <vector>

namespace perf {
class SymbolEntry
{
public:
    SymbolEntry(const std::size_t offset, const std::size_t size, std::string &&name) noexcept
        : _offset(offset), _size(size), _name(name)
    {
    }

    ~SymbolEntry() noexcept = default;

    [[nodiscard]] std::size_t offset() const noexcept { return _offset; }
    [[nodiscard]] std::size_t size() const noexcept { return _size; }
    [[nodiscard]] const std::string &name() const noexcept { return _name; };

private:
    const std::size_t _offset;
    const std::size_t _size;
    const std::string _name;
};

class JITMap
{
public:
    JITMap() = default;
    ~JITMap() = default;

    void emplace_back(SymbolEntry &&symbol_entry) { _symbols.emplace_back(std::move(symbol_entry)); }
    void write() const;
    [[nodiscard]] bool empty() const noexcept { return _symbols.empty(); }

private:
    std::vector<SymbolEntry> _symbols;
};

class JITDump
{
public:
    JITDump() = default;
    ~JITDump() = default;

    void emplace_back(SymbolEntry &&symbol_entry) { _symbols.emplace_back(std::move(symbol_entry)); }
    void write() const;
    [[nodiscard]] bool empty() const noexcept { return _symbols.empty(); }

private:
    struct Header
    {
        Header(const std::uint32_t total_size_, const std::int32_t pid_, const std::uint64_t timestamp_) noexcept
            : total_size(total_size_), pid(pid_), timestamp(timestamp_)
        {
        }

        ~Header() = default;

        std::uint32_t magic{0x4a695444};
        std::uint32_t version{1U};
        std::uint32_t total_size;
        std::uint32_t elf_mach{EM_X86_64};
        std::uint32_t pad1{0U};
        std::int32_t pid;
        std::uint64_t timestamp;
        std::uint64_t flags{0U};
    };

    struct RecordHeader
    {
        RecordHeader(const std::uint32_t id_, const std::uint32_t total_size_, const std::uint64_t timestamp_) noexcept
            : id(id_), total_size(total_size_), timestamp(timestamp_)
        {
        }

        ~RecordHeader() = default;

        std::uint32_t id;
        std::uint32_t total_size;
        std::uint64_t timestamp;
    };

    struct RecordLoad
    {
        RecordLoad(const std::int32_t pid_, const std::int32_t tid_, const std::uint64_t vma_,
                   const std::uint64_t code_addr_, const std::uint64_t code_size_,
                   const std::uint64_t code_index_) noexcept
            : pid(pid_), tid(tid_), vma(vma_), code_addr(code_addr_), code_size(code_size_), code_index(code_index_)
        {
        }

        ~RecordLoad() = default;

        std::int32_t pid;
        std::int32_t tid;
        std::uint64_t vma;
        std::uint64_t code_addr;
        std::uint64_t code_size;
        std::uint64_t code_index;
    };

    enum RecordType
    {
        JIT_CODE_LOAD = 0,          // describing a jitted function
        JIT_CODE_MOVE = 1,          // already jitted function which is moved
        JIT_CODE_DEBUG_INFO = 2,    // debug info for function
        JIT_CODE_CLOSE = 3,         // end of jit runtime marker (optional)
        JIT_CODE_UNWINDING_INFO = 4 // unwinding info for a function
    };

    std::vector<SymbolEntry> _symbols;
};
} // namespace perf