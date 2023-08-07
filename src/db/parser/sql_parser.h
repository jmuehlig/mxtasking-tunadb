#pragma once
#include "driver.h"
#include "node.h"
#include <memory>
#include <string>

namespace db::parser {
class SQLParser
{
public:
    SQLParser() noexcept = default;
    ~SQLParser() noexcept = default;

    std::unique_ptr<NodeInterface> parse(std::string &&query);

private:
    Driver _driver;
};
} // namespace db::parser