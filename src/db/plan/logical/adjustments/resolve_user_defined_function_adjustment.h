#pragma once

#include "adjustment_interface.h"
#include <db/expression/operation.h>
#include <memory>

namespace db::plan::logical {
/**
 * This adjustment resolves user defined function names to descriptors
 * and annotates the descriptors to the operations.
 */
class ResolveUserDefinedFunctionAdjustment final : public AdjustmentInterface
{
public:
    explicit constexpr ResolveUserDefinedFunctionAdjustment(const topology::Database &database) noexcept
        : _database(database)
    {
    }
    ~ResolveUserDefinedFunctionAdjustment() noexcept override = default;

    void apply(std::unique_ptr<NodeInterface> &node) override;

private:
    const topology::Database &_database;
};
} // namespace db::plan::logical