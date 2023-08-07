#include "sql_parser.h"
#include <sstream>

using namespace db::parser;

std::unique_ptr<NodeInterface> SQLParser::parse(std::string &&query)
{
    /// Let the parser parse the query. The AST will be stored in the driver.
    this->_driver.parse(std::istringstream(std::move(query)));

    return std::move(this->_driver.ast());
}