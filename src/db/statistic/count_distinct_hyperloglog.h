#pragma once

#include <count/hll.h>
#include <db/data/value.h>
#include <memory>
#include <unordered_set>
#include <xxh64.hpp>

namespace db::statistic {
class CountDistinctHyperLogLogBuilder
{
public:
    CountDistinctHyperLogLogBuilder() : _hyperloglog(libcount::HLL::Create(8)) {}

    ~CountDistinctHyperLogLogBuilder() = default;

    void insert(const data::Value &value) { _hyperloglog->Update(CountDistinctHyperLogLogBuilder::hash(value)); }

    [[nodiscard]] std::uint64_t get() const noexcept { return _hyperloglog->Estimate(); }

private:
    std::unique_ptr<libcount::HLL> _hyperloglog;

    [[nodiscard]] static std::uint64_t hash(const data::Value &value) noexcept
    {
        constexpr auto seed = 0xDA05B9E7B4;
        const auto &data = value.value();

        if (std::holds_alternative<type::underlying<type::Id::INT>::value>(data))
        {
            const auto key = std::get<type::underlying<type::Id::INT>::value>(data);
            return xxh64::hash(reinterpret_cast<const char *>(&key), sizeof(key), seed);
        }

        if (std::holds_alternative<type::underlying<type::Id::BIGINT>::value>(data))
        {
            const auto key = std::get<type::underlying<type::Id::BIGINT>::value>(data);
            return xxh64::hash(reinterpret_cast<const char *>(&key), sizeof(key), seed);
        }

        if (std::holds_alternative<type::underlying<type::Id::BOOL>::value>(data))
        {
            const auto key = std::get<type::underlying<type::Id::BOOL>::value>(data);
            return xxh64::hash(reinterpret_cast<const char *>(&key), sizeof(key), seed);
        }

        if (std::holds_alternative<type::underlying<type::Id::DECIMAL>::value>(data))
        {
            const auto key = std::get<type::underlying<type::Id::DECIMAL>::value>(data);
            return xxh64::hash(reinterpret_cast<const char *>(&key), sizeof(key), seed);
        }

        if (std::holds_alternative<type::underlying<type::Id::DATE>::value>(data))
        {
            const auto key = std::get<type::underlying<type::Id::DATE>::value>(data).data();
            return xxh64::hash(reinterpret_cast<const char *>(&key), sizeof(key), seed);
        }

        if (std::holds_alternative<std::string>(data))
        {
            const auto &key = std::get<std::string>(data);
            return xxh64::hash(key.data(), key.size(), seed);
        }

        if (std::holds_alternative<std::string_view>(data))
        {
            const auto key = std::get<std::string_view>(data);
            return xxh64::hash(key.data(), key.size(), seed);
        }

        return 0ULL;
    }
};
} // namespace db::statistic