#include "udf.h"

using namespace application::tunadb;

db::type::underlying<db::type::Id::DECIMAL>::value UDF::test(
    const db::type::underlying<db::type::Id::DECIMAL>::value o_totalprice,
    const db::type::underlying<db::type::Id::DECIMAL>::value l_extendedprice)
{
    auto result = o_totalprice;
    for (auto i = 0U; i < 15; ++i)
    {
        if (i % 5 == 0U || result % 5 == 0U)
        {
            result += l_extendedprice;
        }
        result += l_extendedprice;
    }
    return result;
}