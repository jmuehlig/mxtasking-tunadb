#pragma once
#include "context.h"
#include <flounder/compilation/compiler.h>
#include <flounder/executable.h>
#include <flounder/program.h>
#include <memory>

namespace db::execution::compilation {
class Program
{
public:
    explicit Program(flounder::Program &&program) : _program(std::move(program)) {}

    Program(flounder::Program &&program, std::unique_ptr<OutputProviderInterface> &&output_provider)
        : _program(std::move(program)), _output_provider(std::move(output_provider))
    {
    }

    virtual ~Program() = default;

    [[nodiscard]] bool compile(flounder::Compiler &compiler)
    {
        const auto is_successful = compiler.compile(_program, _executable);
        if (is_successful) [[likely]]
        {
            _callback = _executable.callback();
        }
        return is_successful;
    }

    [[nodiscard]] const std::unique_ptr<OutputProviderInterface> &output_provider() const noexcept
    {
        return _output_provider;
    }
    [[nodiscard]] const flounder::Program &flounder() const noexcept { return _program; }
    [[nodiscard]] flounder::Program &flounder() noexcept { return _program; }
    [[nodiscard]] const flounder::Executable &executable() const noexcept { return _executable; }

    template <typename R = void, typename... Args> [[nodiscard]] R execute(Args... arguments)
    {
        return reinterpret_cast<R (*)(Args...)>(_callback)(std::forward<Args>(arguments)...);
    }

    [[nodiscard]] std::uintptr_t callback() const noexcept { return std::uintptr_t(_callback); }

protected:
    flounder::Program _program;
    flounder::Executable::callback_t _callback{nullptr};
    flounder::Executable _executable;
    std::unique_ptr<OutputProviderInterface> _output_provider{nullptr};
};

/**
 * The MultiversionProgram can be used to implement adaptive recompilation,
 * holind multiple versions of a generated program.
 * TODO: Use for adaptive recompilation.
 */
class MultiversionProgram final : public Program
{
public:
    MultiversionProgram(flounder::Program &&program, std::unique_ptr<OutputProviderInterface> &&output_provider)
        : Program(std::move(program), std::move(output_provider)), _executables(0U)
    {
    }

    ~MultiversionProgram() override = default;

    /**
     * Translates the program into the given version and updates
     * the callback pointer.
     *
     * @param compiler Compiler to translate from flounder into asm.
     * @return True, if a new version was created.
     */
    [[nodiscard]] bool translate(const std::uint32_t version, flounder::Compiler &compiler)
    {
        auto &new_executable = _executables[version];
        const auto is_successful = compiler.translate(_program, new_executable);
        if (is_successful) [[likely]]
        {
            _callback = new_executable.callback();
        }
        return is_successful;
    }

    void callback(flounder::Executable::callback_t callback) noexcept { _callback = callback; }

    [[nodiscard]] std::uint32_t capacity() const noexcept { return _executables.size(); }
    [[nodiscard]] const flounder::Executable &version(const std::uint32_t index) const noexcept
    {
        return _executables[index];
    }

private:
    std::vector<flounder::Executable> _executables;
};
} // namespace db::execution::compilation