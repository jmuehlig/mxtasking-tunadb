#pragma once

namespace application::rjbenchmark {
class config
{
public:
    [[nodiscard]] static constexpr auto tuples_per_tile() { return 256U; }

    [[nodiscard]] static constexpr auto prefetch_size() { return 512U; }

    [[nodiscard]] static constexpr auto radix_bits() { return 10U; }
};
} // namespace application::rjbenchmark