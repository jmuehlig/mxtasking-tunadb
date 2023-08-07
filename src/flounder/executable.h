#pragma once
#include <asmjit/asmjit.h>
#include <cstdint>
#include <flounder/compilation/compilate.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace flounder {
class Executable
{
public:
    using callback_t = void (*)();

    Executable() = default;
    ~Executable()
    {
        if (_callback != nullptr)
        {
            _runtime.release(_callback);
        }
    }

    [[nodiscard]] asmjit::JitRuntime &runtime() noexcept { return _runtime; }

    [[nodiscard]] callback_t callback() const noexcept { return _callback; }
    [[nodiscard]] std::uintptr_t base() const noexcept { return std::uintptr_t(_callback); }
    [[nodiscard]] std::size_t code_size() const noexcept { return _code_size; }
    [[nodiscard]] Compilate &compilate() noexcept { return _compilate; }
    [[nodiscard]] const Compilate &compilate() const noexcept { return _compilate; }

    void code_size(const std::size_t size) noexcept { _code_size = size; }

    [[nodiscard]] asmjit::Error add(asmjit::CodeHolder &code_holder)
    {
        return this->_runtime.add(&_callback, &code_holder);
    }

    template <typename R = void, typename... Args> [[nodiscard]] R execute(Args... arguments)
    {
        return reinterpret_cast<R (*)(Args...)>(_callback)(std::forward<Args>(arguments)...);
    }

private:
    /// Callback that starts execution of the compiled code.
    callback_t _callback{nullptr};

    /// Runtime of asmjit to produce machine code.
    asmjit::JitRuntime _runtime;

    /// ASM code, produced by asmjit, when requested during compilation.
    Compilate _compilate;

    /// Size of the code.
    std::size_t _code_size{0U};
};
} // namespace flounder