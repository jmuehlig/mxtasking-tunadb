#include "mmio_range.h"
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <unistd.h>

using namespace perf;

MMIORange::MMIORange(const std::uint64_t base_address, const std::uint64_t size) : _size(size)
{
    this->_file_descriptor = ::open("/dev/mem", O_RDONLY);
    if (this->_file_descriptor < 0)
    {
        throw std::invalid_argument{"Can not open '/dev/mem'. Are you root?"};
    }

    this->_mmap_address =
        reinterpret_cast<char *>(::mmap(nullptr, size, PROT_READ, MAP_SHARED, this->_file_descriptor, base_address));
}

MMIORange::~MMIORange()
{
    if (this->_mmap_address != nullptr)
    {
        ::munmap(this->_mmap_address, this->_size);
    }

    if (this->_file_descriptor >= 0)
    {
        ::close(this->_file_descriptor);
    }
}

std::uint32_t MMIORange::read32u(std::uint64_t offset)
{
    return *(reinterpret_cast<std::uint32_t *>(this->_mmap_address + offset));
}

std::uint64_t MMIORange::read64u(std::uint64_t offset)
{
    return *(reinterpret_cast<std::uint64_t *>(this->_mmap_address + offset));
}