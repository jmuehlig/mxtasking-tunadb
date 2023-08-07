//#include "cycle_estimator.h"
//#include <flounder/annotations.h>
//
// using namespace flounder;
//
// std::uint64_t CycleEstimator::estimate(const InstructionSet &code, std::uint64_t start_index, std::uint64_t
// end_index,
//                                       std::unordered_set<std::uint8_t> &cache)
//{
//    float cycles = .0;
//    for (auto line = start_index; line <= end_index; ++line)
//    {
//        auto *node = code[line];
//        /// Every compare: 4 (branch-miss penalties dominate)
//        if (*node == NodeType::CMP)
//        {
//            cycles += 4;
//        }
//
//        /// Branches may be taken and loops may be executed multitple times.
//        /// Therefore, we have to multiply the cycles of the loop (and branch).
//        else if (*node == NodeType::BRANCH_BEGIN_MARKER)
//        {
//            auto *annotation = reinterpret_cast<BranchBeginMarkerNode *>(node)->annotation();
//            if (annotation != nullptr)
//            {
//                if (annotation->type() == Annotation::Type::Loop)
//                {
//                    auto *loop_annotation = reinterpret_cast<LoopAnnotation *>(annotation);
//                    auto *loop_end_marker = loop_annotation->end_marker();
//                    auto loop_end_iterator =
//                        std::find(code.lines().begin() + line, code.lines().begin() + end_index, loop_end_marker);
//                    if (loop_end_iterator != code.lines().end())
//                    {
//                        const auto loop_end = std::distance(code.lines().begin(), loop_end_iterator);
//                        cycles += (CycleEstimator::estimate(code, line + 1U, loop_end, cache)) *
//                                  loop_annotation->count_iterations().value_or(1U);
//                        line = loop_end + 1U;
//                    }
//                }
//                else if (annotation->type() == Annotation::Type::Branch)
//                {
//                    auto *branch_annotation = reinterpret_cast<BranchAnnotation *>(annotation);
//                    auto *branch_end_marker = branch_annotation->end_marker();
//                    auto branch_end_iterator =
//                        std::find(code.lines().begin() + line, code.lines().begin() + end_index, branch_end_marker);
//                    if (branch_end_iterator != code.lines().end())
//                    {
//                        const auto branch_end = std::distance(code.lines().begin(), branch_end_iterator);
//                        cycles += CycleEstimator::estimate(code, line + 1U, branch_end, cache) *
//                                  branch_annotation->likeliness();
//                        line = branch_end + 1U;
//                    }
//                }
//            }
//        }
//
//        /// Every mov:
//        ///     .25 if access only to constant or mreg
//        ///     .5  if memory is cached
//        ///     8   if memory is not l1 cached
//        else if (*node == NodeType::MOV)
//        {
//            auto *left_child = node->child(0U);
//            auto *right_child = node->child(1U);
//            if (*left_child == NodeType::MEM_AT || *right_child == NodeType::MEM_AT)
//            {
//                const auto is_uncached = CycleEstimator::is_uncached_access(reinterpret_cast<MovNode *>(node), cache);
//                cycles += is_uncached ? 8 : .5;
//            }
//            else
//            {
//                cycles += .25;
//            }
//        }
//
//        /// Every binary instruction:
//        ///     .25 if access to register or constant
//        ///     .5  if access to memory
//        else if (node->size() > 1U)
//        {
//            auto *left_child = node->child(0U);
//            auto *right_child = node->child(0U);
//            cycles += .25 * (1U + static_cast<std::uint8_t>(left_child->type() == NodeType::MEM_AT ||
//                                                            right_child->type() == NodeType::MEM_AT));
//        }
//
//        /// Every unary instruction:
//        ///     .25 if access to register or constant
//        ///     .5  if access to memory
//        else if (node->size() == 1U)
//        {
//            cycles += .25 * (1U + static_cast<std::uint8_t>(node->child(0U)->type() == NodeType::MEM_AT));
//        }
//    }
//
//    return cycles;
//}
//
// bool CycleEstimator::is_uncached_access(MovNode *node, std::unordered_set<std::uint8_t> &cache)
//{
//    if (*node->child(1U) == NodeType::MEM_AT)
//    {
//        auto *mem_at_node = node->child(1U);
//        MachineRegisterIdentifierNode *reg_node = nullptr;
//        if (*mem_at_node->child(0U) == NodeType::MREG)
//        {
//            reg_node = reinterpret_cast<MachineRegisterIdentifierNode *>(mem_at_node->child(0U));
//        }
//        else if (*mem_at_node->child(0U) == NodeType::MEM_ADD || *mem_at_node->child(0U) == NodeType::MEM_SUB)
//        {
//            auto *mem_add_or_sub_node = mem_at_node->child(0U);
//            if (*mem_add_or_sub_node->child(0U) == NodeType::MREG)
//            {
//                reg_node = reinterpret_cast<MachineRegisterIdentifierNode *>(mem_add_or_sub_node->child(0U));
//            }
//        }
//
//        if (reg_node != nullptr)
//        {
//            const auto is_cached = cache.contains(reg_node->id());
//            if (is_cached == false)
//            {
//                cache.insert(reg_node->id());
//                return true;
//            }
//        }
//    }
//
//    return false;
//}