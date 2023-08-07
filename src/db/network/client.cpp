#include "client.h"
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace db::network;

bool Client::connect()
{
    this->_socket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (this->_socket < 0)
    {
        return false;
    }

    struct addrinfo hints
    {
    };
    struct addrinfo *resource = nullptr;
    hints.ai_flags = AI_CANONNAME;
    hints.ai_family = AF_INET;
    hints.ai_socktype = 0;
    hints.ai_protocol = 0;

    const auto port = std::to_string(static_cast<std::int16_t>(this->_port));
    auto result = ::getaddrinfo(this->_server_address.data(), port.c_str(), &hints, &resource);
    if (result == 0)
    {
        auto *current = resource;
        while (current != nullptr)
        {
            if (::connect(this->_socket, current->ai_addr, current->ai_addrlen) == 0)
            {
                ::freeaddrinfo(resource);
                return true;
            }
            current = current->ai_next;
        }
    }

    return false;
}

void Client::disconnect() const
{
    ::close(this->_socket);
}

std::string Client::send(const std::string &message)
{
    if (::write(this->_socket, message.data(), message.size()) < 0)
    {
        return "Error on sending message.";
    }

    // Read header
    auto header = std::uint64_t(0);
    this->read_into_buffer(sizeof(header), static_cast<void *>(&header));

    // Read data
    auto message_buffer = std::string(header, '\0');
    this->read_into_buffer(header, message_buffer.data());

    return message_buffer;
}

std::uint64_t Client::read_into_buffer(std::uint64_t length, void *buffer) const
{
    auto bytes_read = 0ULL;
    while (bytes_read < length)
    {
        const auto read =
            ::read(this->_socket, reinterpret_cast<void *>(reinterpret_cast<std::uintptr_t>(buffer) + bytes_read),
                   length - bytes_read);
        if (read < 1)
        {
            return bytes_read;
        }

        bytes_read += read;
    }

    return bytes_read;
}