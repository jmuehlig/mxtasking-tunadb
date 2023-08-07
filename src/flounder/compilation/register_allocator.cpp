#include "register_allocator.h"
#include <flounder/exception.h>
#include <fmt/core.h>

using namespace flounder;

std::unordered_map<Register, LiveInterval, RegisterHash> LivenessAnalyzer::analyze(const Program &program)
{
    auto live_ranges = std::unordered_map<Register, LiveInterval, RegisterHash>{};
    live_ranges.reserve(128U);

    auto timepoint = LivenessAnalyzer::analyze(live_ranges, program.arguments(), 0UL);
    timepoint = LivenessAnalyzer::analyze(live_ranges, program.header(), timepoint);
    std::ignore = LivenessAnalyzer::analyze(live_ranges, program.body(), timepoint);

    return live_ranges;
}

std::uint64_t LivenessAnalyzer::analyze(std::unordered_map<Register, LiveInterval, RegisterHash> &active,
                                        const InstructionSet &instructions, std::uint64_t time_point)
{
    for (const auto &instruction : instructions.lines())
    {
        if (std::holds_alternative<VregInstruction>(instruction))
        {
            const auto &vreg_instruction = std::get<VregInstruction>(instruction);
            if (auto iterator = active.find(vreg_instruction.vreg()); iterator == active.end())
            {
                active.insert(std::make_pair(vreg_instruction.vreg(), LiveInterval{time_point, vreg_instruction.width(),
                                                                                   vreg_instruction.sign_type()}));
            }
        }
        else if (std::holds_alternative<ClearInstruction>(instruction))
        {
            const auto &clear_instruction = std::get<ClearInstruction>(instruction);
            if (auto iterator = active.find(clear_instruction.vreg()); iterator != active.end())
            {
                iterator->second.end(time_point);
            }
        }

        ++time_point;
    }

    return time_point;
}

RegisterSchedule LinearScanRegisterAllocator::allocate(const Program &program)
{
    /// Extract variable intervals.
    auto intervals = LivenessAnalyzer::analyze(program);

    /// Sort intervals by start.
    auto live_ranges_sorted_start = std::vector<std::pair<Register, LiveInterval>>{};
    std::transform(intervals.begin(), intervals.end(), std::back_inserter(live_ranges_sorted_start),
                   [](const auto &pair) { return std::make_pair(pair.first, pair.second); });
    std::sort(live_ranges_sorted_start.begin(), live_ranges_sorted_start.end(),
              [](const auto &left, const auto &right) { return left.second.begin() < right.second.begin(); });

    /// Allocation of the registers.
    auto schedule = std::unordered_map<std::string_view, VregAllocation>{};
    schedule.reserve(intervals.size() * 2U);

    /// List of free registers.
    constexpr auto available_mreg_ids = ABI::available_mreg_ids();
    this->_free_machine_register_ids = std::vector<mreg_id_t>{available_mreg_ids.begin(), available_mreg_ids.end()};

    /// Spill stack
    this->_spill_set.clear();

    /// List of active machine register allocations.
    this->_active_registers.clear();

    /// Scan
    for (const auto &[vreg, interval] : live_ranges_sorted_start)
    {
        this->clear_unused_allocations(interval.begin(), schedule);

        if (this->_active_registers.size() == ABI::available_mreg_ids().max_size())
        {
            /// Need to spill: Either this vreg or find a victim.
            const auto &victim = *this->_active_registers.rbegin();
            if (victim.second.end().value() > interval.end().value())
            {
                /// Get  machine register from victim.
                auto &victim_schedule = schedule.at(victim.first.virtual_name().value());
                const auto machine_register_id = victim_schedule.mreg().machine_register_id().value();

                /// Push victom to stack.
                victim_schedule = VregAllocation{this->_spill_set.allocate(victim_schedule.mreg().width().value(),
                                                                           victim_schedule.mreg().sign_type())};

                /// Remove victim from active.
                this->_active_registers.erase(victim);

                /// Schedule current interval.
                schedule.insert(std::make_pair(
                    vreg.virtual_name().value(),
                    VregAllocation{Register{machine_register_id, interval.width(), interval.sign_type()}}));
                this->_active_registers.insert(std::make_pair(vreg, interval));
            }
            else
            {
                schedule.insert(
                    std::make_pair(vreg.virtual_name().value(),
                                   VregAllocation{this->_spill_set.allocate(interval.width(), interval.sign_type())}));
            }
        }
        else
        {
            /// Select free machine register.
            const auto machine_register_id = this->_free_machine_register_ids.back();
            this->_free_machine_register_ids.pop_back();

            /// Schedule current interval.
            schedule.insert(
                std::make_pair(vreg.virtual_name().value(),
                               VregAllocation{Register{machine_register_id, interval.width(), interval.sign_type()}}));
            this->_active_registers.insert(std::make_pair(vreg, interval));
        }
    }

    return RegisterSchedule{this->_spill_set.max_height(), std::move(schedule)};
}

void LinearScanRegisterAllocator::clear_unused_allocations(
    const std::uint64_t current, const std::unordered_map<std::string_view, VregAllocation> &schedule)
{
    for (auto interval = this->_active_registers.begin(); interval != this->_active_registers.end();)
    {
        if (interval->second.end().value() >= current)
        {
            break;
        }

        if (auto iterator = schedule.find(interval->first.virtual_name().value()); iterator != schedule.end())
        {
            if (iterator->second.is_mreg())
            {
                this->_free_machine_register_ids.emplace_back(iterator->second.mreg().machine_register_id().value());
            }
            else
            {
                this->_spill_set.free(iterator->second.spill_slot());
            }
        }

        interval = this->_active_registers.erase(interval);
    }
}
