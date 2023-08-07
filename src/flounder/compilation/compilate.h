#pragma once

#include <cstdint>
#include <iostream>
#include <numeric>
#include <perf/sample.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace flounder {
class Compilate
{
public:
    Compilate()
    {
        _instructions.reserve(1U << 10U);
        _context_addresses.reserve(1U << 4U);
    }

    ~Compilate() = default;

    [[nodiscard]] bool has_code() const noexcept { return _instructions.empty() == false; }
    [[nodiscard]] bool has_contexts() const noexcept { return _context_addresses.empty() == false; }

    [[nodiscard]] const std::vector<std::pair<std::optional<std::uintptr_t>, std::string>> &instructions()
        const noexcept
    {
        return _instructions;
    }

    [[nodiscard]] std::vector<std::pair<std::optional<std::uintptr_t>, std::string>> &instructions() noexcept
    {
        return _instructions;
    }

    void emplace_back(const std::uintptr_t address, std::string &&asm_instruction)
    {
        _instructions.emplace_back(address, std::move(asm_instruction));
    }

    void emplace_back(const std::string &context_name, const std::vector<std::uint32_t> &offsets)
    {
        if (offsets.empty() == false)
        {
            if (auto iterator = _context_addresses.find(context_name); iterator != _context_addresses.end())
            {
                iterator->second.insert(iterator->second.end(), offsets.begin(), offsets.end());
            }
            else
            {
                auto addresses = std::vector<std::uintptr_t>{};
                addresses.reserve(1024U);
                addresses.insert(addresses.end(), offsets.begin(), offsets.end());
                _context_addresses.insert(std::make_pair(context_name, std::move(addresses)));
            }

            if (auto iterator = _context_order.find(context_name); iterator == _context_order.end())
            {
                _context_order.insert(std::make_pair(context_name, ++_next_context_id));
            }
        }
    }

    void remove_last_offset(const std::uint32_t offset) noexcept
    {
        if (_instructions.empty() == false)
        {
            std::get<0>(_instructions.back()) = std::nullopt;
        }

        for (auto &[_, addresses] : _context_addresses)
        {
            if (addresses.empty() == false)
            {
                if (addresses.back() == offset)
                {
                    addresses.erase(addresses.end() - 1U);
                }
            }
        }
    }

    void align_to_base(const std::uintptr_t base_address)
    {
        for (auto &instruction : _instructions)
        {
            if (std::get<0>(instruction).has_value())
            {
                std::get<0>(instruction).value() += base_address;
            }
        }

        for (auto &[_, addresses] : _context_addresses)
        {
            for (auto &address : addresses)
            {
                address += base_address;
            }
        }
    }

    [[nodiscard]] std::vector<std::string> code() const noexcept
    {
        auto lines = std::vector<std::string>{};

        std::transform(_instructions.begin(), _instructions.end(), std::back_inserter(lines),
                       [](const auto &instruction) { return std::get<1>(instruction); });

        return lines;
    }

    [[nodiscard]] std::vector<std::tuple<std::uint64_t, float, std::string>> code(
        const perf::AggregatedSamples &samples) const noexcept
    {
        auto lines = std::vector<std::tuple<std::uint64_t, float, std::string>>{};

        std::transform(
            _instructions.begin(), _instructions.end(), std::back_inserter(lines), [&samples](const auto &instruction) {
                const auto instruction_address = std::get<0>(instruction);
                if (instruction_address.has_value())
                {
                    const auto [count, percentage] = samples.count_and_percentage(instruction_address.value());
                    return std::make_tuple(count, percentage, std::get<1>(instruction));
                }

                return std::make_tuple(std::uint64_t(0U), float(.0), std::get<1>(instruction));
            });

        return lines;
    }

    [[nodiscard]] std::vector<std::tuple<std::uint64_t, float, std::string>> contexts(
        const perf::AggregatedSamples &samples) const noexcept
    {
        auto contexts = std::vector<std::tuple<std::uint64_t, float, std::string>>{};

        for (const auto &[name, addresses] : _context_addresses)
        {
            auto percentage = float(.0);
            auto count = std::uint64_t(0U);
            for (auto address : addresses)
            {
                const auto [count_, percentage_] = samples.count_and_percentage(address);
                count += count_;
                percentage += percentage_;
            }

            contexts.emplace_back(count, percentage, name);
        }

        std::sort(contexts.begin(), contexts.end(),
                  [&keys = this->_context_order](const auto &left, const auto &right) {
                      if (auto left_iterator = keys.find(std::get<2>(left)); left_iterator != keys.end())
                      {
                          if (auto right_iterator = keys.find(std::get<2>(right)); right_iterator != keys.end())
                          {
                              return left_iterator->second < right_iterator->second;
                          }

                          return false;
                      }

                      return true;
                  });

        return contexts;
    }

private:
    std::vector<std::pair<std::optional<std::uintptr_t>, std::string>> _instructions;

    std::unordered_map<std::string, std::vector<std::uintptr_t>> _context_addresses;
    std::unordered_map<std::string, std::uint16_t> _context_order;
    std::uint16_t _next_context_id{0U};
};
} // namespace flounder