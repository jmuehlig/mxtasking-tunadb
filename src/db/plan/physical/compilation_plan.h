#pragma once

#include <db/execution/compilation/hashtable/abstract_table.h>
#include <db/execution/compilation/hashtable/descriptor.h>
#include <db/execution/compilation/operator/operator_interface.h>
#include <db/plan/logical/plan.h>
#include <db/topology/database.h>
#include <memory>
#include <nlohmann/json.hpp>

namespace db::plan::physical {
class CompilationPlan
{
public:
    [[nodiscard]] static CompilationPlan build(const topology::Database &database, logical::Plan &&logical_plan);

    explicit CompilationPlan(std::unique_ptr<execution::compilation::OperatorInterface> &&root_operator,
                             std::vector<mx::tasking::TaskInterface *> &&preparatory_tasks) noexcept
        : _root_operator(std::move(root_operator)), _preparatory_tasks(std::move(preparatory_tasks))
    {
    }

    ~CompilationPlan() noexcept = default;

    [[nodiscard]] const std::unique_ptr<execution::compilation::OperatorInterface> &root_operator() const noexcept
    {
        return _root_operator;
    }

    [[nodiscard]] std::unique_ptr<execution::compilation::OperatorInterface> &root_operator() noexcept
    {
        return _root_operator;
    }

    [[nodiscard]] std::vector<mx::tasking::TaskInterface *> &preparatory_tasks() noexcept { return _preparatory_tasks; }

    [[nodiscard]] std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>> memory_tags()
        const
    {
        auto tags = std::unordered_map<std::string, std::vector<std::pair<std::uintptr_t, std::uintptr_t>>>{};
        tags.reserve(64U);

        _root_operator->emit_memory_tags(tags);

        return tags;
    }

private:
    std::unique_ptr<execution::compilation::OperatorInterface> _root_operator;
    std::vector<mx::tasking::TaskInterface *> _preparatory_tasks;

    /**
     * Translates the given logical node to a physical operator.
     * Children will be translated recursively.
     *
     * @param database Database.
     * @param logical_node Logical plan node to translate.
     * @return Translated physical operator.
     */
    [[nodiscard]] static std::unique_ptr<execution::compilation::OperatorInterface> build_operator(
        const topology::Database &database, std::unique_ptr<logical::NodeInterface> &&logical_node,
        std::vector<mx::tasking::TaskInterface *> &preparatory_tasks);

    /**
     * Builds a set of hash tables for grouped aggregation.
     * The hash tables will be aligned to a power of two of the expected cardinality.
     *
     * @param count_partitions Number of partitions.
     * @param hash_table_descriptor Descriptor of the hash table.
     * @param preparatory_tasks
     * @return List of hash tables.
     */
    [[nodiscard]] static std::vector<execution::compilation::hashtable::AbstractTable *> build_aggregation_hash_tables(
        std::uint16_t count_partitions, const execution::compilation::hashtable::Descriptor &hash_table_descriptor,
        std::vector<mx::tasking::TaskInterface *> &preparatory_tasks);

    /**
     *
     * @param radix_bits
     * @param pass
     * @param count_worker
     * @return
     */
    [[nodiscard]] static std::vector<mx::resource::ptr> build_radix_partitions(
        const std::vector<std::uint8_t> &radix_bits, std::uint8_t pass, std::uint16_t count_worker);
};
} // namespace db::plan::physical