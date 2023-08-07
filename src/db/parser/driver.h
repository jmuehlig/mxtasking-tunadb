#pragma once

#include "location.hh"
#include "node.h"
#include <iostream>
#include <memory>
#include <vector>

namespace db::parser {

class Parser;

class Driver
{
public:
    Driver() noexcept = default;
    ~Driver() noexcept = default;

    int parse(std::istream &&in);

    [[nodiscard]] const std::unique_ptr<NodeInterface> &ast() const { return _root; }
    [[nodiscard]] std::unique_ptr<NodeInterface> &ast() { return _root; }

    void ast(std::unique_ptr<NodeInterface> &&root) { _root = std::move(root); }
    friend class Parser;
    friend class Scanner;

private:
    std::unique_ptr<NodeInterface> _root;
};
} // namespace db::parser
