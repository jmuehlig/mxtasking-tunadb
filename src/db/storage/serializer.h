#pragma once

#include <db/topology/database.h>
#include <string>

namespace db::storage {
class Serializer
{
public:
    static void serialize(const topology::Database &database, const std::string &file_name);
    static void deserialize(topology::Database &database, const std::string &file_name);
};
} // namespace db::storage