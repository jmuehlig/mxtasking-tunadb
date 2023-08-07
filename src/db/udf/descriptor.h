#pragma once
#include <db/type/type.h>
#include <string>
#include <utility>
#include <vector>

namespace db::udf {
class Descriptor
{
public:
    Descriptor(std::string &&name, const bool is_compute_bound,
               std::vector<std::pair<std::string, type::Type>> &&input_paramters, const type::Type return_type,
               const std::uintptr_t callable) noexcept
        : _name(std::move(name)), _is_compute_bound(is_compute_bound), _input_parameters(std::move(input_paramters)),
          _return_type(return_type), _callable(callable)
    {
    }

    Descriptor(Descriptor &&) noexcept = default;

    [[nodiscard]] const std::string &name() const noexcept { return _name; }
    [[nodiscard]] bool is_compute_bound() const noexcept { return _is_compute_bound; }
    [[nodiscard]] const std::vector<std::pair<std::string, type::Type>> &input_parameters() const noexcept
    {
        return _input_parameters;
    }
    [[nodiscard]] type::Type return_type() const noexcept { return _return_type; }
    [[nodiscard]] std::uintptr_t callable() const noexcept { return _callable; }

    ~Descriptor() = default;

private:
    std::string _name;
    bool _is_compute_bound;
    std::vector<std::pair<std::string, type::Type>> _input_parameters;
    type::Type _return_type;
    std::uintptr_t _callable;
};
} // namespace db::udf