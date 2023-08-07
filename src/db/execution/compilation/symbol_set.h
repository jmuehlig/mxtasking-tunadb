#pragma once

#include "scan_access_characteristic.h"
#include <db/exception/execution_exception.h>
#include <db/expression/operation.h>
#include <db/expression/term.h>
#include <db/topology/physical_schema.h>
#include <db/util/string.h>
#include <flounder/program.h>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace db::execution::compilation {
/**
 * The symbol set organizes the access to symbols between compilation operators.
 * Each operator can request symbols (=terms). For example, the arithmetic operator
 * will request all physical attributes used for arithmetics (e.g., 1-id where id is
 * an attribute).
 *
 * On the other hand, each operator can set symbols by linking terms to virtual registers.
 * Every time, an operator creates a new (attribute) virtual register, it links the term
 * and the newly virtual register (using "set").
 *
 * After accessing the requested attributes, the operator releases the terms. The last
 * release will end in yielding a "clear" instruction.
 */
class SymbolSet
{
public:
    SymbolSet() = default;
    ~SymbolSet() = default;

    /**
     * Request a given term to be set by other operators.
     *
     * @param term Term to be set.
     */
    void request(const expression::Term &term)
    {
        auto iterator = _requested_symbols.find(term);
        if (iterator == _requested_symbols.end())
        {
            _requested_symbols.insert(std::make_pair(term, 1U));
        }
        else
        {
            iterator->second += 1U;
        }
    }

    /**
     * Request all given terms to be set by other operators.
     *
     * @param terms Terms to be set.
     */
    void request(const std::vector<expression::Term> &terms)
    {
        for (const auto &term : terms)
        {
            request(term);
        }
    }

    /**
     * Request all given terms within the operations
     * to be set by other operators.
     *
     * @param operations Operations holding terms to be set.
     */
    void request(const std::vector<std::unique_ptr<expression::Operation>> &operations)
    {
        for (const auto &operation : operations)
        {
            expression::for_each_term(operation, [&](const expression::Term &term) {
                if (term.is_attribute())
                {
                    this->request(term);
                }
            });
        }
    }

    /**
     * Release the given term. The releasing operator do not need
     * to access the virtual register linked to the term, again.
     * Whenever the last operator releases a term, the virtual register
     * will be clears.
     *
     * @param program Program to possibly emit a clear instruction.
     * @param term Term that is no longer needed.
     */
    void release(flounder::Program &program, const expression::Term &term)
    {
        if (auto iterator = _loaded_symbols.find(term); iterator != _loaded_symbols.end())
        {
            auto &reference_counter = std::get<1>(iterator->second);
            if (--reference_counter == 0U)
            {
                program << program.clear(std::get<0>(iterator->second));
                _loaded_symbols.erase(iterator);
            }
        }
    }

    /**
     * Release the given terms. The releasing operator do not need
     * to access the virtual registers linked to the terms, again.
     * Whenever the last operator releases a term, the virtual register
     * will be clears.
     *
     * @param program Program to possibly emit a clear instruction.
     * @param term Terms that are no longer needed.
     */
    void release(flounder::Program &program, const std::vector<expression::Term> &terms)
    {
        for (const auto &term : terms)
        {
            release(program, term);
        }
    }

    /**
     * Release all terms within the given operations. The releasing operator
     * do not need to access the virtual registers linked to the terms, again.
     * Whenever the last operator releases a term, the virtual register
     * will be clears.
     *
     * @param program Program to possibly emit a clear instruction.
     * @param operations Operations holding terms that are no longer needed.
     */
    void release(flounder::Program &program, const std::vector<std::unique_ptr<expression::Operation>> &operations)
    {
        for (const auto &operation : operations)
        {
            expression::for_each_term(operation, [&](const expression::Term &term) {
                if (term.is_attribute())
                {
                    this->release(program, term);
                }
            });
        }
    }

    /**
     * Access the virtual register behind a requested term.
     *
     * @param term Term that should be read.
     * @return Virtual register holding the term.
     */
    [[nodiscard]] flounder::Register get(const expression::Term &term) const
    {
        if (auto iterator = _loaded_symbols.find(term); iterator != _loaded_symbols.end())
        {
            return std::get<0>(iterator->second);
        }

        throw exception::SymbolNotFoundException{term.to_string()};
    }

    /**
     * Returns true, if the given term is loaded (and linked to a virtual register).
     *
     * @param term Term to verify.
     * @return True, if the given term is loaded.
     */
    [[nodiscard]] bool is_set(const expression::Term &term) const noexcept
    {
        return _loaded_symbols.find(term) != _loaded_symbols.end();
    }

    /**
     * Returns true, if the given term is requested.
     *
     * @param term Term to verify.
     * @return True, if the given term is requested.
     */
    [[nodiscard]] bool is_requested(const expression::Term &term) const noexcept
    {
        return _requested_symbols.find(term) != _requested_symbols.end();
    }

    /**
     * Links the given term to the given virtual register.
     * From now, every time an operator gets the term, the virtual
     * register is returned.
     *
     * @param term Term to set.
     * @param vreg Virtual register to link to the term.
     */
    void set(const expression::Term &term, flounder::Register vreg)
    {
        if (auto iterator = _requested_symbols.find(term); iterator != _requested_symbols.end()) [[likely]]
        {
            _loaded_symbols.insert(std::make_pair(iterator->first, std::make_pair(vreg, iterator->second)));
            _requested_symbols.erase(iterator);
        }
    }

    void touch(const expression::Term &term)
    {
        if (auto iterator = _loaded_symbols.find(term); iterator != _loaded_symbols.end())
        {
            std::get<1>(iterator->second) += 1U;
        }
    }

    /**
     * Calcuates thin min and max requested bytes from a given schema.
     *
     * @param schema
     * @return Pair of min and max requested bytes.
     */
    [[nodiscard]] ScanAccessCharacteristic min_max_requested_bytes(
        const topology::PhysicalSchema &schema) const noexcept
    {
        auto min_byte = std::numeric_limits<std::uint16_t>::max();
        auto max_byte = std::numeric_limits<std::uint16_t>::min();

        for (const auto &[term, _] : _requested_symbols)
        {
            const auto index = schema.index(term);
            if (index.has_value())
            {
                const auto begin = schema.row_offset(index.value());
                const auto end = begin + schema.type(index.value()).size();
                if (begin < min_byte)
                {
                    min_byte = begin;
                }
                if (end > max_byte)
                {
                    max_byte = end;
                }
            }
        }

        return ScanAccessCharacteristic::from_to(min_byte, max_byte);
    }

    [[nodiscard]] std::uint32_t count_requests(const std::unique_ptr<expression::Operation> &operation) const noexcept
    {
        if (auto load_iterator = _loaded_symbols.find(operation->result().value());
            load_iterator != _loaded_symbols.end())
        {
            return load_iterator->second.second;
        }

        if (auto request_iterator = _requested_symbols.find(operation->result().value());
            request_iterator != _requested_symbols.end())
        {
            return request_iterator->second;
        }

        return 0U;
    }

    /**
     * Creates a friendly name of the given term.
     *
     * @param term Term.
     * @return Friendly name of the term.
     */
    [[nodiscard]] static std::string make_vreg_name(const expression::Term &term)
    {
        auto vreg_name =
            util::string::replace(term.to_string(), {{"\\(", "_"}, {"\\)", "_"}, {"\\s", "_"}, {" ", "_"}});
        if (vreg_name.back() == '_')
        {
            vreg_name.pop_back();
        }

        return vreg_name;
    }

    struct TermHash
    {
    public:
        std::size_t operator()(const db::expression::Term &term) const
        {
            if (term.is_attribute())
            {
                return std::hash<db::expression::Attribute>()(term.get<db::expression::Attribute>());
            }

            if (term.is_value())
            {
                return std::hash<db::data::Value>()(term.get<db::data::Value>());
            }

            return 0U;
        }
    };

private:
    /// All requested terms and the number of requests.
    std::unordered_map<expression::Term, std::uint32_t, TermHash> _requested_symbols;

    /// All loaded terms and their virtual registers (and number of requests).
    std::unordered_map<expression::Term, std::pair<flounder::Register, std::uint32_t>, TermHash> _loaded_symbols;
};
} // namespace db::execution::compilation