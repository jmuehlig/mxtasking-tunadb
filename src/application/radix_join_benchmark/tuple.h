#pragma once
#include <cstdint>

namespace application::rjbenchmark {
struct Tuple
{
    std::int64_t key;
    std::int64_t payload;
};
} // namespace application::rjbenchmark