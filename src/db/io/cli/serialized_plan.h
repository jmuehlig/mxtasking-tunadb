#pragma once
#include <cstdint>
#include <db/util/text_table.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace db::io {
class SerializedPlan
{
public:
    SerializedPlan(nlohmann::json &&plan) noexcept : _plan(std::move(plan)) {}
    ~SerializedPlan() noexcept = default;

    [[nodiscard]] nlohmann::json &plan() noexcept { return _plan; }
    [[nodiscard]] std::string to_string() const noexcept;
    [[nodiscard]] std::string to_dot() const noexcept;

private:
    nlohmann::json _plan;

    static void add_plan_to_table(util::TextTable &table, const nlohmann::json &layer, std::uint16_t depth) noexcept;
    static std::uint64_t add_plan_to_dot(
        const nlohmann::json &layer, std::uint64_t &current_node_id,
        std::vector<std::tuple<std::uint64_t, std::string, std::optional<std::string>>> &nodes,
        std::vector<std::tuple<std::uint64_t, std::uint64_t, std::uint64_t>> &edges) noexcept;
};
} // namespace db::io