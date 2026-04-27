#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

struct sqlite3;
struct sqlite3_stmt;

namespace storage
{

class SqliteStorage
{
public:
    struct RetentionTarget
    {
        std::string table_name {};
        std::string order_column {"id"};
        std::string processed_column {"processed"};
        std::size_t processed_cleanup_batch {1000};
        std::size_t forced_cleanup_batch {1000};
    };

    struct Config
    {
        std::string db_path {};
        bool create_parent_directories {true};
        std::uint64_t max_database_bytes {0};
        std::chrono::milliseconds busy_timeout {5000};
        int wal_autocheckpoint_pages {1000};
        std::uint64_t journal_size_limit_bytes {64ULL * 1024ULL * 1024ULL};
        std::vector<std::string> schema_statements {};
        std::vector<RetentionTarget> retention_targets {};
    };

    using StatementBinder = std::function<bool(sqlite3_stmt*)>;
    using RowReader = std::function<void(sqlite3_stmt*)>;

public:
    explicit SqliteStorage(Config config);
    ~SqliteStorage();

    SqliteStorage(const SqliteStorage&) = delete;
    SqliteStorage& operator=(const SqliteStorage&) = delete;

    bool Initialize();
    void Shutdown();

    [[nodiscard]] bool IsOpen() const;
    [[nodiscard]] const std::string& DbPath() const noexcept;
    [[nodiscard]] std::uint64_t CurrentDatabaseBytes() const;

    bool Execute(std::string_view sql);
    bool ExecuteBatch(const std::vector<std::string>& statements);
    bool ExecutePrepared(std::string_view sql, StatementBinder binder = {});
    bool Query(std::string_view sql, const RowReader& row_reader) const;
    std::optional<std::int64_t> QueryInt64(std::string_view sql) const;

    bool BeginImmediateTransaction();
    bool CommitTransaction();
    void RollbackTransaction();

    bool RunRetentionPolicy();

private:
    bool Open();
    void Close();
    bool ConfigurePragmas();
    bool CreateSchema();

    bool ExecuteUnlocked(std::string_view sql);
    bool ExecutePreparedUnlocked(std::string_view sql, const StatementBinder& binder);
    std::optional<std::int64_t> QueryInt64Unlocked(std::string_view sql) const;
    std::uint64_t CurrentDatabaseBytesUnlocked() const;

    bool RunRetentionPolicyUnlocked();
    std::size_t DeleteProcessedRowsUnlocked(const RetentionTarget& target);
    std::size_t DeleteOldestRowsUnlocked(const RetentionTarget& target);

    static bool IsSafeIdentifier(std::string_view identifier);

private:
    Config config_;
    sqlite3* db_ {nullptr};
    mutable std::mutex db_mutex_;
};

} // namespace storage
