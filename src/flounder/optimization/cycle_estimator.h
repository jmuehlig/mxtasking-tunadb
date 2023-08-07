//#pragma once
//#include <cstdint>
//#include <flounder/abi/x86_64.h>
//#include <flounder/instruction_set.h>
//#include <unordered_set>
//
// namespace flounder {
// class CycleEstimator
//{
// public:
//    [[nodiscard]] static std::uint64_t estimate(const InstructionSet &code)
//    {
//        return estimate(code, 0U, code.size() - 1U);
//    }
//
//    [[nodiscard]] static std::uint64_t estimate(const InstructionSet &code, std::uint64_t start_index,
//                                                std::uint64_t end_index)
//    {
//        auto cache = std::unordered_set<std::uint8_t>{ABI::stack_pointer_mreg_id()};
//        return estimate(code, start_index, end_index, cache);
//    }
//
// private:
//    [[nodiscard]] static std::uint64_t estimate(const InstructionSet &code, std::uint64_t start_index,
//                                                std::uint64_t end_index, std::unordered_set<std::uint8_t> &cache);
//    [[nodiscard]] static bool is_uncached_access(MovNode *node, std::unordered_set<std::uint8_t> &cache);
//    static void update_cache(Node *node, std::unordered_set<std::uint8_t> &cache);
//};
//} // namespace flounder