#pragma once

#include <db/type/type.h>

namespace application::tunadb {
class UDF
{
public:
    static db::type::underlying<db::type::Id::DECIMAL>::value test(
        db::type::underlying<db::type::Id::DECIMAL>::value o_totalprice,
        db::type::underlying<db::type::Id::DECIMAL>::value l_extendedprice);
};
} // namespace application::tunadb