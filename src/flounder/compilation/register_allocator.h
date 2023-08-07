#pragma once

#include <exception>
#include <flounder/abi/x86_64.h>
#include <flounder/program.h>
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

namespace flounder {

/**
 * Represents a slot in the spill stack for virtual register values.
 */
class SpillSlot
{
public:
    SpillSlot(const std::uint32_t offset, const RegisterWidth width, const std::optional<RegisterSignType> sign_type)
        : _offset(offset), _width(width), _sign_type(sign_type)
    {
    }

    SpillSlot(SpillSlot &&) noexcept = default;
    SpillSlot(const SpillSlot &) noexcept = default;

    ~SpillSlot() = default;

    SpillSlot &operator=(SpillSlot &&) noexcept = default;
    SpillSlot &operator=(const SpillSlot &) noexcept = default;

    [[nodiscard]] std::uint32_t offset() const noexcept { return _offset; }
    [[nodiscard]] RegisterWidth width() const noexcept { return _width; }
    [[nodiscard]] std::optional<RegisterSignType> sign_type() const noexcept { return _sign_type; }

private:
    /// Offset of the spill.
    std::uint32_t _offset;

    /// Width of the register.
    RegisterWidth _width;

    /// Unsigned or Signed value?
    std::optional<RegisterSignType> _sign_type{std::nullopt};
};

/**
 * Represents a living interval of a virtual register.
 */
class LiveInterval
{
public:
    LiveInterval(const std::uint64_t begin, const RegisterWidth width, const RegisterSignType sign_type) noexcept
        : _begin(begin), _width(width), _sign_type(sign_type)
    {
    }

    void end(const std::uint64_t end) noexcept { _end = end; }

    [[nodiscard]] std::uint64_t begin() const noexcept { return _begin; }
    [[nodiscard]] std::optional<std::uint64_t> end() const noexcept { return _end; }

    [[nodiscard]] RegisterWidth width() const noexcept { return _width; }
    [[nodiscard]] RegisterSignType sign_type() const noexcept { return _sign_type; }

private:
    std::uint64_t _begin;
    std::optional<std::uint64_t> _end;

    RegisterWidth _width;
    RegisterSignType _sign_type;
};

class LivenessAnalyzer
{
public:
    [[nodiscard]] static std::unordered_map<Register, LiveInterval, RegisterHash> analyze(const Program &program);

private:
    [[nodiscard]] static std::uint64_t analyze(std::unordered_map<Register, LiveInterval, RegisterHash> &active,
                                               const InstructionSet &instructions, std::uint64_t time_point);
};

/**
 * Represents a machine register or a spill slot a virtual register is allocated to.
 */
class VregAllocation
{
public:
    explicit VregAllocation(Register reg) noexcept : _allocation(reg) {}
    explicit VregAllocation(const SpillSlot spill_offset) noexcept : _allocation(spill_offset) {}
    VregAllocation(VregAllocation &&) noexcept = default;
    VregAllocation(const VregAllocation &) = default;

    VregAllocation &operator=(const VregAllocation &) = default;
    VregAllocation &operator=(VregAllocation &&) noexcept = default;

    [[nodiscard]] bool is_mreg() const noexcept { return std::holds_alternative<Register>(_allocation); }
    [[nodiscard]] bool is_spill() const noexcept { return std::holds_alternative<SpillSlot>(_allocation); }
    [[nodiscard]] Register mreg() const noexcept { return std::get<Register>(_allocation); }
    [[nodiscard]] SpillSlot spill_slot() const noexcept { return std::get<SpillSlot>(_allocation); }

private:
    std::variant<Register, SpillSlot> _allocation;
};

/**
 * Represents a schedule for an entire program that maps from virtual register
 * to machine register or spill slots.
 */
class RegisterSchedule
{
public:
    RegisterSchedule() = default;

    RegisterSchedule(const std::uint32_t max_stack_height,
                     std::unordered_map<std::string_view, VregAllocation> &&schedule) noexcept
        : _max_stack_height(max_stack_height), _schedule(std::move(schedule))
    {
    }

    ~RegisterSchedule() = default;

    RegisterSchedule &operator=(RegisterSchedule &&) noexcept = default;

    [[nodiscard]] std::uint32_t max_stack_height() const noexcept { return _max_stack_height; }
    [[nodiscard]] std::optional<VregAllocation> schedule(const Register &vreg) const noexcept
    {
        if (auto iterator = _schedule.find(vreg.virtual_name().value()); iterator != _schedule.end())
        {
            return iterator->second;
        }

        return std::nullopt;
    }

    [[nodiscard]] std::unordered_set<std::uint8_t> used_machine_register_ids() const noexcept
    {
        auto machine_register_ids = std::unordered_set<std::uint8_t>{};
        machine_register_ids.reserve(_schedule.size());

        for (const auto &[vreg, schedule] : _schedule)
        {
            if (schedule.is_mreg())
            {
                machine_register_ids.insert(schedule.mreg().machine_register_id().value());
            }
        }

        return machine_register_ids;
    }

private:
    std::uint32_t _max_stack_height{0U};
    std::unordered_map<std::string_view, VregAllocation> _schedule;
};

class LinearScanRegisterAllocator
{
public:
    using mreg_id_t = std::uint8_t;

    LinearScanRegisterAllocator() = default;
    ~LinearScanRegisterAllocator() = default;

    /**
     * Performs linear scan register allocation on a given program.
     * Implemented algorithm: https://dl.acm.org/doi/10.1145/330249.330250
     *
     * @param program Program to schedule registers.
     * @return A register schedule that maps each virtual register to a machine register or a spill slot.
     */
    RegisterSchedule allocate(const Program &program);

private:
    class SpillSet
    {
    public:
        SpillSet() { _slots.reserve(1U << 7U); }
        ~SpillSet() = default;

        [[nodiscard]] std::uint32_t max_height() const noexcept { return _max_size * 8U; }
        [[nodiscard]] SpillSlot allocate(const RegisterWidth width, const std::optional<RegisterSignType> sign_type)
        {
            for (auto i = 0U; i < _slots.size(); ++i)
            {
                if (_slots[i] == false)
                {
                    _slots[i] = true;
                    return SpillSlot{std::uint32_t(i * 8U), width, sign_type};
                }
            }

            ++_max_size;
            _slots.emplace_back(true);
            return SpillSlot{std::uint32_t((_slots.size() - 1U) * 8U), width, sign_type};
        }

        void free(const SpillSlot &slot)
        {
            const auto slot_id = std::uint32_t(slot.offset() / 8U);
            _slots[slot_id] = false;
        }

        void clear()
        {
            _slots.clear();
            _max_size = 0U;
        }

    private:
        std::vector<bool> _slots;
        std::uint16_t _max_size{0U};
    };

    class IncreasingEndComparator
    {
    public:
        constexpr bool operator()(const std::pair<Register, LiveInterval> &left,
                                  const std::pair<Register, LiveInterval> &right) const
        {
            return left.second.end().value() < right.second.end().value();
        }
    };

    /// List of free register ids.
    std::vector<mreg_id_t> _free_machine_register_ids;

    /// Set of active registers.
    std::set<std::pair<Register, LiveInterval>, IncreasingEndComparator> _active_registers;

    /// Stack of active spills.
    SpillSet _spill_set;

    /**
     * Removes all unused registers from the active set.
     *
     * @param current
     * @param schedule
     */
    void clear_unused_allocations(std::uint64_t current,
                                  const std::unordered_map<std::string_view, VregAllocation> &schedule);
};

} // namespace flounder