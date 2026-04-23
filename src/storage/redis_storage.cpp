#include "pes/storage/redis_storage.hpp"

#include <spdlog/spdlog.h>

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace storage
{

namespace
{
bool set_socket_blocking(const int socket_fd, const bool blocking)
{
    const int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags < 0)
    {
        return false;
    }

    const int updated_flags = blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK);
    return fcntl(socket_fd, F_SETFL, updated_flags) == 0;
}
}

bool RedisStorage::Reply::IsError() const noexcept
{
    return type == Type::Error;
}

bool RedisStorage::Reply::IsNull() const noexcept
{
    return type == Type::Null;
}

bool RedisStorage::Reply::IsStringLike() const noexcept
{
    return type == Type::SimpleString || type == Type::BulkString || type == Type::Error;
}

std::optional<std::string_view> RedisStorage::Reply::AsString() const noexcept
{
    if (!IsStringLike())
    {
        return std::nullopt;
    }

    return text;
}

std::optional<std::int64_t> RedisStorage::Reply::AsInteger() const noexcept
{
    if (type != Type::Integer)
    {
        return std::nullopt;
    }

    return integer;
}

RedisStorage::RedisStorage(Config config)
    : config_(std::move(config))
{
}

RedisStorage::~RedisStorage()
{
    Shutdown();
}

bool RedisStorage::Initialize()
{
    std::scoped_lock lock(mutex_);

    if (initialized_)
    {
        return socket_fd_ >= 0;
    }

    initialized_ = true;
    if (!ConnectLocked())
    {
        initialized_ = false;
        return false;
    }

    SPDLOG_INFO("RedisStorage connected to {}:{}", config_.host, config_.port);
    return true;
}

void RedisStorage::Shutdown()
{
    std::scoped_lock lock(mutex_);
    initialized_ = false;
    CloseLocked();
}

bool RedisStorage::IsConnected() const
{
    std::scoped_lock lock(mutex_);
    return socket_fd_ >= 0;
}

bool RedisStorage::Ping()
{
    const auto reply = Command("PING");
    const auto text = reply ? reply->AsString() : std::nullopt;
    return text.has_value() && *text == "PONG";
}

std::optional<std::string> RedisStorage::Get(const std::string_view key)
{
    const auto reply = Command("GET", key);
    if (!reply.has_value() || reply->IsError() || reply->IsNull())
    {
        return std::nullopt;
    }

    const auto text = reply->AsString();
    if (!text.has_value())
    {
        return std::nullopt;
    }

    return std::string(*text);
}

bool RedisStorage::Delete(const std::string_view key)
{
    const auto deleted_count = ReplyInteger(Command("DEL", key));
    return deleted_count.has_value() && *deleted_count > 0;
}

bool RedisStorage::Exists(const std::string_view key)
{
    const auto exists = ReplyInteger(Command("EXISTS", key));
    return exists.has_value() && *exists > 0;
}

std::optional<std::unordered_map<std::string, std::string>> RedisStorage::HashGetAll(const std::string_view key)
{
    const auto reply = Command("HGETALL", key);
    if (!reply.has_value() || reply->IsError())
    {
        return std::nullopt;
    }

    if (reply->type != Reply::Type::Array)
    {
        SPDLOG_ERROR("Redis HGETALL reply type mismatch");
        return std::nullopt;
    }

    if ((reply->array.size() % 2U) != 0U)
    {
        SPDLOG_ERROR("Redis HGETALL returned uneven field/value pairs");
        return std::nullopt;
    }

    std::unordered_map<std::string, std::string> result;
    result.reserve(reply->array.size() / 2U);

    for (std::size_t index = 0; index < reply->array.size(); index += 2)
    {
        const auto field = reply->array[index].AsString();
        const auto value = reply->array[index + 1].AsString();

        if (!field.has_value() || !value.has_value())
        {
            SPDLOG_ERROR("Redis HGETALL returned non-string field/value");
            return std::nullopt;
        }

        result.emplace(std::string(*field), std::string(*value));
    }

    return result;
}

std::optional<RedisStorage::Reply> RedisStorage::CommandVector(const std::vector<std::string>& args)
{
    std::scoped_lock lock(mutex_);

    if (!initialized_)
    {
        SPDLOG_ERROR("RedisStorage is not initialized");
        return std::nullopt;
    }

    if (!ConnectLocked())
    {
        return std::nullopt;
    }

    const std::string payload = BuildCommandPayload(args);
    if (!SendAllLocked(payload))
    {
        CloseLocked();
        return std::nullopt;
    }

    auto reply = ReadReplyLocked();
    if (!reply.has_value())
    {
        CloseLocked();
        return std::nullopt;
    }

    if (reply->IsError())
    {
        SPDLOG_ERROR("Redis command '{}' failed: {}", args.empty() ? "<empty>" : args.front(), reply->text);
    }

    return reply;
}

bool RedisStorage::ConnectLocked()
{
    if (socket_fd_ >= 0)
    {
        return true;
    }

    addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* address_info = nullptr;
    const std::string port_text = std::to_string(config_.port);
    const int getaddrinfo_rc = getaddrinfo(config_.host.c_str(), port_text.c_str(), &hints, &address_info);
    if (getaddrinfo_rc != 0)
    {
        SPDLOG_ERROR("Redis getaddrinfo failed: {}", gai_strerror(getaddrinfo_rc));
        return false;
    }

    bool connected = false;
    for (addrinfo* entry = address_info; entry != nullptr; entry = entry->ai_next)
    {
        const int socket_fd = socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
        if (socket_fd < 0)
        {
            continue;
        }

        if (!set_socket_blocking(socket_fd, false))
        {
            close(socket_fd);
            continue;
        }

        const int connect_rc = connect(socket_fd, entry->ai_addr, entry->ai_addrlen);
        if (connect_rc != 0 && errno != EINPROGRESS)
        {
            close(socket_fd);
            continue;
        }

        if (connect_rc != 0)
        {
            pollfd poll_fd {
                .fd = socket_fd,
                .events = POLLOUT,
                .revents = 0,
            };

            const int poll_rc = poll(&poll_fd, 1, static_cast<int>(config_.connect_timeout.count()));
            if (poll_rc <= 0)
            {
                close(socket_fd);
                continue;
            }

            int socket_error = 0;
            socklen_t option_length = sizeof(socket_error);
            if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &socket_error, &option_length) != 0 || socket_error != 0)
            {
                close(socket_fd);
                continue;
            }
        }

        if (!set_socket_blocking(socket_fd, true))
        {
            close(socket_fd);
            continue;
        }

        socket_fd_ = socket_fd;
        receive_buffer_.clear();

        if (!ConfigureSocketLocked())
        {
            CloseLocked();
            continue;
        }

        if (!AuthenticateLocked())
        {
            CloseLocked();
            continue;
        }

        if (!SelectDatabaseLocked())
        {
            CloseLocked();
            continue;
        }

        connected = true;
        break;
    }

    freeaddrinfo(address_info);

    if (!connected)
    {
        SPDLOG_ERROR("Redis connect failed to {}:{}", config_.host, config_.port);
    }

    return connected;
}

bool RedisStorage::ConfigureSocketLocked() const
{
    if (socket_fd_ < 0)
    {
        return false;
    }

    if (config_.tcp_no_delay)
    {
        const int enabled = 1;
        if (setsockopt(socket_fd_, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) != 0)
        {
            SPDLOG_WARN("Redis TCP_NODELAY failed: {}", std::strerror(errno));
        }
    }

    const auto seconds = static_cast<long>(config_.command_timeout.count() / 1000);
    const auto microseconds = static_cast<long>((config_.command_timeout.count() % 1000) * 1000);
    const timeval timeout {
        .tv_sec = seconds,
        .tv_usec = microseconds,
    };

    if (setsockopt(socket_fd_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0)
    {
        SPDLOG_ERROR("Redis SO_SNDTIMEO failed: {}", std::strerror(errno));
        return false;
    }

    if (setsockopt(socket_fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0)
    {
        SPDLOG_ERROR("Redis SO_RCVTIMEO failed: {}", std::strerror(errno));
        return false;
    }

    return true;
}

bool RedisStorage::AuthenticateLocked()
{
    if (config_.password.empty())
    {
        return true;
    }

    const auto reply = ExecuteLocked({"AUTH", config_.password});
    return IsReplyOk(reply);
}

bool RedisStorage::SelectDatabaseLocked()
{
    if (config_.database == 0)
    {
        return true;
    }

    const auto reply = ExecuteLocked({"SELECT", std::to_string(config_.database)});
    return IsReplyOk(reply);
}

void RedisStorage::CloseLocked()
{
    if (socket_fd_ >= 0)
    {
        close(socket_fd_);
        socket_fd_ = -1;
    }

    receive_buffer_.clear();
}

bool RedisStorage::SendAllLocked(const std::string_view payload)
{
    std::size_t bytes_sent = 0;
    while (bytes_sent < payload.size())
    {
        const auto* buffer = payload.data() + bytes_sent;
        const auto bytes_left = payload.size() - bytes_sent;
        const ssize_t send_rc = send(socket_fd_, buffer, bytes_left, MSG_NOSIGNAL);
        if (send_rc < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            SPDLOG_ERROR("Redis send failed: {}", std::strerror(errno));
            return false;
        }

        bytes_sent += static_cast<std::size_t>(send_rc);
    }

    return true;
}

bool RedisStorage::ReadFromSocketLocked()
{
    char buffer[4096] {};
    const ssize_t recv_rc = recv(socket_fd_, buffer, sizeof(buffer), 0);
    if (recv_rc < 0)
    {
        if (errno == EINTR)
        {
            return ReadFromSocketLocked();
        }

        SPDLOG_ERROR("Redis recv failed: {}", std::strerror(errno));
        return false;
    }

    if (recv_rc == 0)
    {
        SPDLOG_WARN("Redis server closed connection");
        return false;
    }

    receive_buffer_.append(buffer, static_cast<std::size_t>(recv_rc));
    return true;
}

bool RedisStorage::EnsureBufferedLocked(const std::size_t bytes)
{
    while (receive_buffer_.size() < bytes)
    {
        if (!ReadFromSocketLocked())
        {
            return false;
        }
    }

    return true;
}

std::optional<char> RedisStorage::ReadPrefixLocked()
{
    if (!EnsureBufferedLocked(1))
    {
        return std::nullopt;
    }

    const char prefix = receive_buffer_.front();
    receive_buffer_.erase(0, 1);
    return prefix;
}

std::optional<std::string> RedisStorage::ReadLineLocked()
{
    while (true)
    {
        const std::size_t line_end = receive_buffer_.find("\r\n");
        if (line_end != std::string::npos)
        {
            std::string line = receive_buffer_.substr(0, line_end);
            receive_buffer_.erase(0, line_end + 2);
            return line;
        }

        if (!ReadFromSocketLocked())
        {
            return std::nullopt;
        }
    }
}

std::optional<std::string> RedisStorage::ReadBulkLocked(const std::size_t bytes)
{
    if (!EnsureBufferedLocked(bytes + 2))
    {
        return std::nullopt;
    }

    std::string bulk = receive_buffer_.substr(0, bytes);
    if (receive_buffer_.compare(bytes, 2, "\r\n") != 0)
    {
        SPDLOG_ERROR("Redis bulk string terminator mismatch");
        return std::nullopt;
    }

    receive_buffer_.erase(0, bytes + 2);
    return bulk;
}

std::optional<RedisStorage::Reply> RedisStorage::ReadReplyLocked()
{
    const auto prefix = ReadPrefixLocked();
    if (!prefix.has_value())
    {
        return std::nullopt;
    }

    switch (*prefix)
    {
    case '+':
    {
        const auto line = ReadLineLocked();
        if (!line.has_value())
        {
            return std::nullopt;
        }
        return Reply {.type = Reply::Type::SimpleString, .text = *line};
    }
    case '-':
    {
        const auto line = ReadLineLocked();
        if (!line.has_value())
        {
            return std::nullopt;
        }
        return Reply {.type = Reply::Type::Error, .text = *line};
    }
    case ':':
    {
        const auto line = ReadLineLocked();
        if (!line.has_value())
        {
            return std::nullopt;
        }

        try
        {
            return Reply {.type = Reply::Type::Integer, .integer = std::stoll(*line)};
        }
        catch (const std::exception&)
        {
            SPDLOG_ERROR("Redis integer reply parse failed");
            return std::nullopt;
        }
    }
    case '$':
    {
        const auto line = ReadLineLocked();
        if (!line.has_value())
        {
            return std::nullopt;
        }

        const long long bulk_length = std::stoll(*line);
        if (bulk_length < 0)
        {
            return Reply {.type = Reply::Type::Null};
        }

        const auto bulk = ReadBulkLocked(static_cast<std::size_t>(bulk_length));
        if (!bulk.has_value())
        {
            return std::nullopt;
        }

        return Reply {.type = Reply::Type::BulkString, .text = *bulk};
    }
    case '*':
    {
        const auto line = ReadLineLocked();
        if (!line.has_value())
        {
            return std::nullopt;
        }

        const long long array_length = std::stoll(*line);
        if (array_length < 0)
        {
            return Reply {.type = Reply::Type::Null};
        }

        Reply reply {.type = Reply::Type::Array};
        reply.array.reserve(static_cast<std::size_t>(array_length));
        for (long long index = 0; index < array_length; ++index)
        {
            auto child = ReadReplyLocked();
            if (!child.has_value())
            {
                return std::nullopt;
            }

            reply.array.push_back(std::move(*child));
        }

        return reply;
    }
    default:
        SPDLOG_ERROR("Redis reply prefix '{}' unsupported", *prefix);
        return std::nullopt;
    }
}

std::string RedisStorage::BuildCommandPayload(const std::vector<std::string>& args)
{
    std::size_t reserve_size = 16;
    for (const auto& arg : args)
    {
        reserve_size += arg.size() + 32;
    }

    std::string payload;
    payload.reserve(reserve_size);
    payload += '*';
    payload += std::to_string(args.size());
    payload += "\r\n";

    for (const auto& arg : args)
    {
        payload += '$';
        payload += std::to_string(arg.size());
        payload += "\r\n";
        payload += arg;
        payload += "\r\n";
    }

    return payload;
}

bool RedisStorage::IsReplyOk(const std::optional<Reply>& reply)
{
    const auto text = reply ? reply->AsString() : std::nullopt;
    return text.has_value() && *text == "OK";
}

std::optional<std::int64_t> RedisStorage::ReplyInteger(const std::optional<Reply>& reply)
{
    return reply ? reply->AsInteger() : std::nullopt;
}

std::optional<RedisStorage::Reply> RedisStorage::ExecuteLocked(const std::vector<std::string>& args)
{
    const std::string payload = BuildCommandPayload(args);
    if (!SendAllLocked(payload))
    {
        return std::nullopt;
    }

    return ReadReplyLocked();
}

} // namespace pes::storage
