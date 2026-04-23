#pragma once

#include <chrono>
#include <concepts>
#include <cstdint>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace storage
{

    template <typename T>
    concept RedisStringViewLike = requires(const std::remove_cvref_t<T> &value) {
        std::string_view{value};
    };

    template <typename T>
    concept RedisArgument =
        RedisStringViewLike<T> || std::integral<std::remove_cvref_t<T>> || std::floating_point<std::remove_cvref_t<T>>;

    template <typename T>
    concept RedisFieldPair = requires(const std::remove_cvref_t<T> &value) {
        value.first;
        value.second;
    } && RedisArgument<decltype(std::declval<const std::remove_cvref_t<T> &>().first)> && RedisArgument<decltype(std::declval<const std::remove_cvref_t<T> &>().second)>;

    template <typename Range>
    concept RedisFieldRange =
        std::ranges::input_range<Range> && RedisFieldPair<std::ranges::range_value_t<Range>>;

    template <typename Range>
    concept RedisArgumentRange =
        std::ranges::input_range<Range> && RedisArgument<std::ranges::range_value_t<Range>>;

    class RedisStorage
    {
    public:
        struct Config
        {
            std::string host{"127.0.0.1"};
            std::uint16_t port{6379};
            std::string password{};
            int database{0};
            std::chrono::milliseconds connect_timeout{1000};
            std::chrono::milliseconds command_timeout{1000};
            bool tcp_no_delay{true};
        };

        struct Reply
        {
            enum class Type
            {
                SimpleString,
                Error,
                Integer,
                BulkString,
                Array,
                Null,
            };

            Type type{Type::Null};
            std::string text{};
            std::int64_t integer{0};
            std::vector<Reply> array{};

            [[nodiscard]] bool IsError() const noexcept;
            [[nodiscard]] bool IsNull() const noexcept;
            [[nodiscard]] bool IsStringLike() const noexcept;
            [[nodiscard]] std::optional<std::string_view> AsString() const noexcept;
            [[nodiscard]] std::optional<std::int64_t> AsInteger() const noexcept;
        };

    public:
        explicit RedisStorage(Config config);
        ~RedisStorage();

        RedisStorage(const RedisStorage &) = delete;
        RedisStorage &operator=(const RedisStorage &) = delete;

        bool Initialize();
        void Shutdown();

        [[nodiscard]] bool IsConnected() const;

        template <RedisArgument... Args>
        std::optional<Reply> Command(const Args &...args)
        {
            return CommandVector({EncodeArgument(args)...});
        }

        bool Ping();

        template <RedisArgument Value>
        bool Set(const std::string_view key, const Value &value)
        {
            return IsReplyOk(Command("SET", key, value));
        }

        std::optional<std::string> Get(std::string_view key);
        bool Delete(std::string_view key);
        bool Exists(std::string_view key);

        template <RedisFieldRange Range>
        std::optional<std::int64_t> HashSet(const std::string_view key, const Range &field_values)
        {
            std::vector<std::string> args;
            args.emplace_back("HSET");
            args.emplace_back(key);

            if constexpr (std::ranges::sized_range<Range>)
            {
                args.reserve(2 + (std::ranges::size(field_values) * 2));
            }

            for (const auto &[field, value] : field_values)
            {
                args.push_back(EncodeArgument(field));
                args.push_back(EncodeArgument(value));
            }

            return ReplyInteger(CommandVector(args));
        }

        std::optional<std::unordered_map<std::string, std::string>> HashGetAll(std::string_view key);

        template <RedisArgumentRange Range>
        std::optional<std::int64_t> PushLeft(const std::string_view key, const Range &values)
        {
            return Push("LPUSH", key, values);
        }

        template <RedisArgumentRange Range>
        std::optional<std::int64_t> PushRight(const std::string_view key, const Range &values)
        {
            return Push("RPUSH", key, values);
        }

    private:
        template <RedisArgument Value>
        static std::string EncodeArgument(const Value &value)
        {
            using CleanValue = std::remove_cvref_t<Value>;

            if constexpr (RedisStringViewLike<CleanValue>)
            {
                return std::string(std::string_view{value});
            }
            else if constexpr (std::same_as<CleanValue, bool>)
            {
                return value ? "1" : "0";
            }
            else
            {
                return std::to_string(value);
            }
        }

        template <RedisArgumentRange Range>
        std::optional<std::int64_t> Push(const std::string_view command, const std::string_view key, const Range &values)
        {
            std::vector<std::string> args;
            args.emplace_back(command);
            args.emplace_back(key);

            if constexpr (std::ranges::sized_range<Range>)
            {
                args.reserve(2 + std::ranges::size(values));
            }

            for (const auto &value : values)
            {
                args.push_back(EncodeArgument(value));
            }

            return ReplyInteger(CommandVector(args));
        }

        std::optional<Reply> CommandVector(const std::vector<std::string> &args);

        bool ConnectLocked();
        bool ConfigureSocketLocked() const;
        bool AuthenticateLocked();
        bool SelectDatabaseLocked();
        void CloseLocked();

        bool SendAllLocked(std::string_view payload);
        bool ReadFromSocketLocked();
        bool EnsureBufferedLocked(std::size_t bytes);
        std::optional<char> ReadPrefixLocked();
        std::optional<std::string> ReadLineLocked();
        std::optional<std::string> ReadBulkLocked(std::size_t bytes);
        std::optional<Reply> ReadReplyLocked();
        std::optional<Reply> ExecuteLocked(const std::vector<std::string> &args);

        static std::string BuildCommandPayload(const std::vector<std::string> &args);
        static bool IsReplyOk(const std::optional<Reply> &reply);
        static std::optional<std::int64_t> ReplyInteger(const std::optional<Reply> &reply);

    private:
        Config config_;
        mutable std::mutex mutex_{};
        int socket_fd_{-1};
        bool initialized_{false};
        std::string receive_buffer_{};
    };

} // namespace pes::storage
