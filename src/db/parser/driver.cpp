#include "driver.h"
#include "parser.hpp"
#include "scanner.hpp"

using namespace db::parser;

int Driver::parse(std::istream &&in)
{
    auto scanner = Scanner{in};
    auto parser = db::parser::Parser{*this, scanner};

    return parser.parse();
}
