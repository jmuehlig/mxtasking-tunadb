#include "jit_map.h"
#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <sys/mman.h>
#include <unistd.h>

using namespace perf;

void JITMap::write() const
{
    auto map_stream = std::ofstream{std::string{"/tmp/perf-"} + std::to_string(getpid()) + ".map", std::ios_base::app};
    for (const auto &symbol : this->_symbols)
    {
        map_stream << std::hex << symbol.offset() << " " << std::hex << symbol.size() << " " << symbol.name() << '\n';
    }
    map_stream << std::flush;
}

void JITDump::write() const
{
    const auto process_id = ::getpid();

    auto header =
        Header{sizeof(Header), process_id, std::uint64_t(std::chrono::system_clock::now().time_since_epoch().count())};

    auto file_name = std::string{"jit-"} + std::to_string(header.pid) + ".dump";
    auto file_descriptor = ::open(file_name.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0666);
    if (file_descriptor > -1)
    {
        auto page_size = sysconf(_SC_PAGESIZE);
        if (page_size > -1)
        {
            auto *mmap_marker = ::mmap(nullptr, page_size, PROT_READ | PROT_EXEC, MAP_PRIVATE, file_descriptor, 0);
            if (mmap_marker != MAP_FAILED)
            {
                auto *file = ::fdopen(file_descriptor, "wb");
                if (file != nullptr)
                {
                    ::fwrite(&header, sizeof(Header), 1, file);

                    auto next_id = std::uint32_t{0U};
                    for (const auto &symbol : this->_symbols)
                    {
                        const auto size = std::uint32_t(sizeof(RecordHeader) + sizeof(RecordLoad) +
                                                        symbol.name().size() + 1U + symbol.size());
                        auto record_header =
                            RecordHeader{RecordType::JIT_CODE_LOAD, size,
                                         std::uint64_t(std::chrono::system_clock::now().time_since_epoch().count())};

                        auto record_load = RecordLoad{process_id,      process_id,    symbol.offset(),
                                                      symbol.offset(), symbol.size(), next_id++};

                        ::fwrite(&record_header, sizeof(RecordHeader), 1, file);
                        ::fwrite(&record_load, sizeof(RecordLoad), 1, file);
                        ::fwrite(symbol.name().c_str(), symbol.name().size() + 1U, 1, file);
                        ::fwrite(reinterpret_cast<void *>(symbol.offset()), symbol.size(), 1, file);
                    }

                    ::munmap(mmap_marker, page_size);
                    ::fclose(file);
                }
            }
        }
    }
}