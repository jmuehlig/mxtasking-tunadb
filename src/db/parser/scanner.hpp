#pragma once

#undef YY_DECL
#define YY_DECL db::parser::symbol_type db::Scanner::lex()

#include <iostream>

#ifndef yyFlexLexerOnce
#include <FlexLexer.h>
#endif

#include "location.hh"
#include "parser.hpp"

namespace db::parser {

// forward declare to avoid an include
class Driver;

class Scanner : public yyFlexLexer
{
public:
    explicit Scanner(std::istream &stream) : yyFlexLexer(stream, std::cout) {}
    ~Scanner() override {}
    Parser::symbol_type lex(Driver &driver);
};
} // namespace db::parser
