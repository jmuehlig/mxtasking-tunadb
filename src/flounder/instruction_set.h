#pragma once

#include <cstdint>
#include <flounder/ir/instructions.h>
#include <string>
#include <unordered_set>
#include <vector>

namespace flounder {
class InstructionSet
{
public:
    explicit InstructionSet(const std::size_t size) { _lines.reserve(size); }
    InstructionSet() noexcept : InstructionSet(1U << 8U) {}
    explicit InstructionSet(std::string &&name, const std::size_t size) : _name(std::move(name))
    {
        _lines.reserve(size);
    }
    InstructionSet(std::string &&name) : InstructionSet(std::move(name), 1U << 8U) {}
    InstructionSet(InstructionSet &&) = default;
    InstructionSet(const InstructionSet &) = delete;
    ~InstructionSet() noexcept = default;

    InstructionSet &operator=(InstructionSet &&) noexcept = default;
    InstructionSet &operator=(const InstructionSet &) = delete;

    [[nodiscard]] const std::optional<std::string> &name() const noexcept { return _name; }
    [[nodiscard]] const std::vector<Instruction> &lines() const noexcept { return _lines; }
    [[nodiscard]] std::vector<Instruction> &lines() noexcept { return _lines; }

    [[nodiscard]] std::uint64_t size() const noexcept { return _lines.size(); }
    [[nodiscard]] bool empty() const noexcept { return _lines.empty(); }

    /**
     * Inserts the given node at the end of the code.
     *
     * @param node Node to insert.
     * @return The instruction set.
     */
    template <typename T>
    requires is_instruction<T> InstructionSet &operator<<(T &&instruction)
    {
        _lines.emplace_back(std::move(instruction));
        return *this;
    }

    /**
     * Inserts the given node at the end of the code.
     *
     * @param node Node to insert.
     * @return The instruction set.
     */
    template <typename T>
    requires is_instruction<T> InstructionSet &operator<<(T &instruction)
    {
        _lines.emplace_back(instruction);
        return *this;
    }

    /**
     * Inserts the given node at the end of the code.
     *
     * @param node Node to insert.
     * @return The instruction set.
     */
    InstructionSet &operator<<(Instruction &&instruction)
    {
        _lines.emplace_back(std::move(instruction));
        return *this;
    }

    InstructionSet &operator<<(const Instruction &instruction)
    {
        _lines.emplace_back(instruction);
        return *this;
    }

    /**
     * Inserts the given node at the given line.
     *
     * @param node Pair of line and node to insert.
     * @return The instruction set.
     */
    template <typename T>
    requires is_instruction<T> InstructionSet &operator<<(std::pair<std::size_t, T> &&instruction)
    {
        _lines.insert(_lines.begin() + std::get<0>(instruction), std::move(std::get<1>(instruction)));

        return *this;
    }

    /**
     * Inserts the given instructions at the end of the code.
     *
     * @param instructions Instructions to insert.
     * @return The instruction set.
     */
    template <typename T>
    requires is_instruction<T> InstructionSet &operator<<(std::vector<T> &&instructions)
    {
        std::move(instructions.begin(), instructions.end(), std::back_inserter(_lines));
        return *this;
    }

    /**
     * Inserts the given instructions at the end of the code.
     *
     * @param instructions Instructions to insert.
     * @return The instruction set.
     */
    InstructionSet &operator<<(std::vector<Instruction> &&instructions)
    {
        std::move(instructions.begin(), instructions.end(), std::back_inserter(_lines));
        return *this;
    }

    /**
     * Inserts the given code at the end of this code.
     *
     * @param code Code to insert.
     * @return The instruction set.
     */
    InstructionSet &operator<<(InstructionSet &&code) { return *this << std::move(code._lines); }

    /**
     * Inserts the given node at the given line.
     *
     * @param node Pair of line and node to insert.
     * @return The instruction set.
     */
    InstructionSet &operator<<(std::pair<std::size_t, InstructionSet> &&code)
    {
        std::move(std::get<1>(code)._lines.begin(), std::get<1>(code)._lines.end(),
                  std::inserter(_lines, _lines.begin() + std::get<0>(code)));
        return *this;
    }

    [[nodiscard]] const Instruction &operator[](const std::size_t index) const noexcept { return _lines[index]; }
    [[nodiscard]] Instruction &operator[](const std::size_t index) noexcept { return _lines[index]; }

    [[nodiscard]] std::vector<std::string> code() const noexcept;

private:
    std::optional<std::string> _name{std::nullopt};
    std::vector<Instruction> _lines;
};
} // namespace flounder