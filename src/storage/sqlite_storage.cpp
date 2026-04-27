#include "pes/storage/sqlite_storage.hpp"

#include <spdlog/spdlog.h>
#include <sqlite3.h>

#include <cctype>
#include <filesystem>

namespace storage
{

namespace
{
std::uint64_t file_size_or_zero(const std::filesystem::path& path)
{
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    return error ? 0U : static_cast<std::uint64_t>(size);
}
}

SqliteStorage::SqliteStorage(Config config)
    : config_(std::move(config))
{
}

SqliteStorage::~SqliteStorage()
{
    Shutdown();
}

bool SqliteStorage::Initialize()
{
    std::lock_guard<std::mutex> lock(db_mutex_);

    if (config_.db_path.empty())
    {
        SPDLOG_ERROR("SqliteStorage db_path is empty");
        return false;
    }

    if (!Open())
    {
        return false;
    }

    if (!ConfigurePragmas())
    {
        return false;
    }

    if (!CreateSchema())
    {
        return false;
    }

    if (!RunRetentionPolicyUnlocked())
    {
        return false;
    }

    SPDLOG_INFO("SqliteStorage initialized at {}", config_.db_path);
    return true;
}

void SqliteStorage::Shutdown()
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    Close();
}

bool SqliteStorage::IsOpen() const
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    return db_ != nullptr;
}

const std::string& SqliteStorage::DbPath() const noexcept
{
    return config_.db_path;
}

std::uint64_t SqliteStorage::CurrentDatabaseBytes() const
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    return CurrentDatabaseBytesUnlocked();
}

bool SqliteStorage::Execute(const std::string_view sql)
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    return ExecuteUnlocked(sql);
}

bool SqliteStorage::ExecuteBatch(const std::vector<std::string>& statements)
{
    std::lock_guard<std::mutex> lock(db_mutex_);

    for (const auto& statement : statements)
    {
        if (!ExecuteUnlocked(statement))
        {
            return false;
        }
    }

    return true;
}

bool SqliteStorage::ExecutePrepared(const std::string_view sql, StatementBinder binder)
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    return ExecutePreparedUnlocked(sql, binder);
}

bool SqliteStorage::Query(const std::string_view sql, const RowReader& row_reader) const
{
    std::lock_guard<std::mutex> lock(db_mutex_);

    if (db_ == nullptr)
    {
        SPDLOG_ERROR("SqliteStorage is not initialized");
        return false;
    }

    sqlite3_stmt* stmt = nullptr;
    const std::string sql_string(sql);
    const int prepare_rc = sqlite3_prepare_v2(db_, sql_string.c_str(), -1, &stmt, nullptr);
    if (prepare_rc != SQLITE_OK)
    {
        SPDLOG_ERROR("SQLite query prepare failed: {}", sqlite3_errmsg(db_));
        return false;
    }

    while (true)
    {
        const int step_rc = sqlite3_step(stmt);
        if (step_rc == SQLITE_ROW)
        {
            if (row_reader)
            {
                row_reader(stmt);
            }
            continue;
        }

        sqlite3_finalize(stmt);
        if (step_rc == SQLITE_DONE)
        {
            return true;
        }

        SPDLOG_ERROR("SQLite query step failed: {}", sqlite3_errmsg(db_));
        return false;
    }
}

std::optional<std::int64_t> SqliteStorage::QueryInt64(const std::string_view sql) const
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    return QueryInt64Unlocked(sql);
}

bool SqliteStorage::BeginImmediateTransaction()
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    return ExecuteUnlocked("BEGIN IMMEDIATE TRANSACTION;");
}

bool SqliteStorage::CommitTransaction()
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    return ExecuteUnlocked("COMMIT;");
}

void SqliteStorage::RollbackTransaction()
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    static_cast<void>(ExecuteUnlocked("ROLLBACK;"));
}

bool SqliteStorage::RunRetentionPolicy()
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    return RunRetentionPolicyUnlocked();
}

bool SqliteStorage::Open()
{
    if (db_ != nullptr)
    {
        return true;
    }

    if (config_.create_parent_directories)
    {
        const std::filesystem::path db_path(config_.db_path);
        const auto parent = db_path.parent_path();
        if (!parent.empty())
        {
            std::error_code error;
            std::filesystem::create_directories(parent, error);
            if (error)
            {
                SPDLOG_ERROR("SQLite directory creation failed path={} error={}", parent.string(), error.message());
                return false;
            }
        }
    }

    const int open_rc = sqlite3_open(config_.db_path.c_str(), &db_);
    if (open_rc != SQLITE_OK)
    {
        SPDLOG_ERROR("SQLite open failed path={} error={}", config_.db_path, db_ ? sqlite3_errmsg(db_) : "unknown");
        Close();
        return false;
    }

    return true;
}

void SqliteStorage::Close()
{
    if (db_ != nullptr)
    {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

bool SqliteStorage::ConfigurePragmas()
{
    if (!ExecuteUnlocked("PRAGMA auto_vacuum=INCREMENTAL;"))
    {
        return false;
    }

    if (!ExecuteUnlocked("PRAGMA journal_mode=WAL;"))
    {
        return false;
    }

    if (!ExecuteUnlocked("PRAGMA synchronous=NORMAL;"))
    {
        return false;
    }

    if (!ExecuteUnlocked("PRAGMA busy_timeout=" + std::to_string(config_.busy_timeout.count()) + ";"))
    {
        return false;
    }

    if (!ExecuteUnlocked("PRAGMA wal_autocheckpoint=" + std::to_string(config_.wal_autocheckpoint_pages) + ";"))
    {
        return false;
    }

    if (!ExecuteUnlocked("PRAGMA journal_size_limit=" + std::to_string(config_.journal_size_limit_bytes) + ";"))
    {
        return false;
    }

    if (config_.max_database_bytes > 0)
    {
        constexpr std::uint64_t sqlite_page_size = 4096U;
        const std::uint64_t max_pages = config_.max_database_bytes / sqlite_page_size;
        if (max_pages > 0 && !ExecuteUnlocked("PRAGMA max_page_count=" + std::to_string(max_pages) + ";"))
        {
            return false;
        }
    }

    return true;
}

bool SqliteStorage::CreateSchema()
{
    for (const auto& statement : config_.schema_statements)
    {
        if (!ExecuteUnlocked(statement))
        {
            return false;
        }
    }

    return true;
}

bool SqliteStorage::ExecuteUnlocked(const std::string_view sql)
{
    if (db_ == nullptr)
    {
        SPDLOG_ERROR("SqliteStorage is not initialized");
        return false;
    }

    char* error_message = nullptr;
    const std::string sql_string(sql);
    const int exec_rc = sqlite3_exec(db_, sql_string.c_str(), nullptr, nullptr, &error_message);
    if (exec_rc != SQLITE_OK)
    {
        SPDLOG_ERROR("SQLite exec failed: {}", error_message ? error_message : "unknown");
        sqlite3_free(error_message);
        return false;
    }

    return true;
}

bool SqliteStorage::ExecutePreparedUnlocked(const std::string_view sql, const StatementBinder& binder)
{
    if (db_ == nullptr)
    {
        SPDLOG_ERROR("SqliteStorage is not initialized");
        return false;
    }

    sqlite3_stmt* stmt = nullptr;
    const std::string sql_string(sql);
    const int prepare_rc = sqlite3_prepare_v2(db_, sql_string.c_str(), -1, &stmt, nullptr);
    if (prepare_rc != SQLITE_OK)
    {
        SPDLOG_ERROR("SQLite prepare failed: {}", sqlite3_errmsg(db_));
        return false;
    }

    if (binder && !binder(stmt))
    {
        sqlite3_finalize(stmt);
        SPDLOG_ERROR("SQLite statement binding failed");
        return false;
    }

    const int step_rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (step_rc != SQLITE_DONE)
    {
        SPDLOG_ERROR("SQLite statement step failed: {}", sqlite3_errmsg(db_));
        return false;
    }

    return true;
}

std::optional<std::int64_t> SqliteStorage::QueryInt64Unlocked(const std::string_view sql) const
{
    if (db_ == nullptr)
    {
        SPDLOG_ERROR("SqliteStorage is not initialized");
        return std::nullopt;
    }

    sqlite3_stmt* stmt = nullptr;
    const std::string sql_string(sql);
    const int prepare_rc = sqlite3_prepare_v2(db_, sql_string.c_str(), -1, &stmt, nullptr);
    if (prepare_rc != SQLITE_OK)
    {
        SPDLOG_ERROR("SQLite int64 query prepare failed: {}", sqlite3_errmsg(db_));
        return std::nullopt;
    }

    const int step_rc = sqlite3_step(stmt);
    if (step_rc != SQLITE_ROW)
    {
        sqlite3_finalize(stmt);
        return std::nullopt;
    }

    const auto value = static_cast<std::int64_t>(sqlite3_column_int64(stmt, 0));
    sqlite3_finalize(stmt);
    return value;
}

std::uint64_t SqliteStorage::CurrentDatabaseBytesUnlocked() const
{
    const std::filesystem::path db_path(config_.db_path);
    return file_size_or_zero(db_path)
        + file_size_or_zero(std::filesystem::path(config_.db_path + "-wal"))
        + file_size_or_zero(std::filesystem::path(config_.db_path + "-shm"));
}

bool SqliteStorage::RunRetentionPolicyUnlocked()
{
    if (config_.max_database_bytes == 0 || config_.retention_targets.empty())
    {
        return true;
    }

    static_cast<void>(ExecuteUnlocked("PRAGMA wal_checkpoint(TRUNCATE);"));

    if (CurrentDatabaseBytesUnlocked() <= config_.max_database_bytes)
    {
        return true;
    }

    for (const auto& target : config_.retention_targets)
    {
        while (CurrentDatabaseBytesUnlocked() > config_.max_database_bytes)
        {
            const std::size_t removed = DeleteProcessedRowsUnlocked(target);
            if (removed == 0)
            {
                break;
            }

            static_cast<void>(ExecuteUnlocked("PRAGMA incremental_vacuum;"));
            static_cast<void>(ExecuteUnlocked("PRAGMA wal_checkpoint(TRUNCATE);"));
            SPDLOG_INFO("SQLite retention removed {} processed rows from {}", removed, target.table_name);
        }
    }

    for (const auto& target : config_.retention_targets)
    {
        while (CurrentDatabaseBytesUnlocked() > config_.max_database_bytes)
        {
            const std::size_t removed = DeleteOldestRowsUnlocked(target);
            if (removed == 0)
            {
                break;
            }

            static_cast<void>(ExecuteUnlocked("PRAGMA incremental_vacuum;"));
            static_cast<void>(ExecuteUnlocked("PRAGMA wal_checkpoint(TRUNCATE);"));
            SPDLOG_WARN("SQLite retention removed {} oldest rows from {}", removed, target.table_name);
        }
    }

    if (CurrentDatabaseBytesUnlocked() > config_.max_database_bytes)
    {
        SPDLOG_ERROR(
            "SQLite retention could not reduce database below limit path={} current_bytes={} max_bytes={}",
            config_.db_path,
            CurrentDatabaseBytesUnlocked(),
            config_.max_database_bytes);
        return false;
    }

    return true;
}

std::size_t SqliteStorage::DeleteProcessedRowsUnlocked(const RetentionTarget& target)
{
    if (target.processed_column.empty() || target.processed_cleanup_batch == 0)
    {
        return 0;
    }

    if (!IsSafeIdentifier(target.table_name) || !IsSafeIdentifier(target.order_column) || !IsSafeIdentifier(target.processed_column))
    {
        SPDLOG_ERROR("SQLite retention target contains unsafe identifier");
        return 0;
    }

    const std::string sql =
        "DELETE FROM " + target.table_name +
        " WHERE " + target.order_column + " IN ("
        "SELECT " + target.order_column +
        " FROM " + target.table_name +
        " WHERE " + target.processed_column + " = 1"
        " ORDER BY " + target.order_column + " ASC"
        " LIMIT " + std::to_string(target.processed_cleanup_batch) +
        ");";

    if (!ExecuteUnlocked(sql))
    {
        return 0;
    }

    return static_cast<std::size_t>(sqlite3_changes(db_));
}

std::size_t SqliteStorage::DeleteOldestRowsUnlocked(const RetentionTarget& target)
{
    if (target.forced_cleanup_batch == 0)
    {
        return 0;
    }

    if (!IsSafeIdentifier(target.table_name) || !IsSafeIdentifier(target.order_column))
    {
        SPDLOG_ERROR("SQLite retention target contains unsafe identifier");
        return 0;
    }

    const std::string sql =
        "DELETE FROM " + target.table_name +
        " WHERE " + target.order_column + " IN ("
        "SELECT " + target.order_column +
        " FROM " + target.table_name +
        " ORDER BY " + target.order_column + " ASC"
        " LIMIT " + std::to_string(target.forced_cleanup_batch) +
        ");";

    if (!ExecuteUnlocked(sql))
    {
        return 0;
    }

    return static_cast<std::size_t>(sqlite3_changes(db_));
}

bool SqliteStorage::IsSafeIdentifier(const std::string_view identifier)
{
    if (identifier.empty())
    {
        return false;
    }

    const auto first = static_cast<unsigned char>(identifier.front());
    if (!std::isalpha(first) && identifier.front() != '_')
    {
        return false;
    }

    for (const char character : identifier)
    {
        const auto value = static_cast<unsigned char>(character);
        if (!std::isalnum(value) && character != '_')
        {
            return false;
        }
    }

    return true;
}

} // namespace storage
