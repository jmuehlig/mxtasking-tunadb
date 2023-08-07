#pragma once
#include <cstdint>
#include <string>

namespace db::network {
class Client
{
public:
    Client(std::string &&server_address, std::uint16_t port) noexcept
        : _server_address(std::move(server_address)), _port(port), _socket(-1)
    {
    }

    ~Client() = default;

    bool connect();
    void disconnect() const;
    std::string send(const std::string &message);

    [[nodiscard]] const std::string &server_address() const { return _server_address; }

    [[nodiscard]] std::uint16_t port() const { return _port; }

private:
    const std::string _server_address;
    const std::uint16_t _port;
    std::int32_t _socket;

    std::uint64_t read_into_buffer(std::uint64_t length, void *buffer) const;
};
} // namespace db::network