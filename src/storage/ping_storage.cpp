#include "pes/storage/ping_storage.hpp"

#include <spdlog/spdlog.h>
#include <sqlite3.h>

namespace pes::storage
{

namespace
{
constexpr const char* kCreateTableSql =
    "CREATE TABLE IF NOT EXISTS ping_results ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "timestamp_ms INTEGER NOT NULL,"
    "target TEXT NOT NULL,"
    "success INTEGER NOT NULL,"
    "latency_ms REAL,"
    "timeout INTEGER NOT NULL,"
    "error_message TEXT,"
    "sequence_number INTEGER NOT NULL,"
    "processed INTEGER NOT NULL DEFAULT 0"
    ");";

constexpr const char* kCreateTimestampIndexSql =
    "CREATE INDEX IF NOT EXISTS idx_ping_results_timestamp "
    "ON ping_results(timestamp_ms);";

constexpr const char* kCreateProcessedIndexSql =
    "CREATE INDEX IF NOT EXISTS idx_ping_results_processed "
    "ON ping_results(processed);";

constexpr const char* kInsertSql =
    "INSERT INTO ping_results "
    "(timestamp_ms, target, success, latency_ms, timeout, error_message, sequence_number, processed) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, 0);";

constexpr const char* kCountSql =
    "SELECT COUNT(*) FROM ping_results;";
}

PingStorage::PingStorage(Config config)
    : config_(std::move(config))
{
}

PingStorage::~PingStorage()
{
    Shutdown();
}

bool PingStorage::Initialize()
{
    std::lock_guard<std::mutex> lock(db_mutex_);

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

    SPDLOG_INFO("PingStorage initialized at {}", config_.db_path);
    return true;
}

void PingStorage::Shutdown()
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    Close();
}

bool PingStorage::StoreRecord(const module::ping::PingRecord& record)
{
    std::lock_guard<std::mutex> lock(db_mutex_);

    if (db_ == nullptr)
    {
        SPDLOG_ERROR("PingStorage is not initialized");
        return false;
    }

    if (!BeginTransaction())
    {
        return false;
    }

    if (!InsertRecordInternal(record))
    {
        RollbackTransaction();
        return false;
    }

    if (!RunRetentionPolicy())
    {
        RollbackTransaction();
        return false;
    }

    if (!CommitTransaction())
    {
        RollbackTransaction();
        return false;
    }

    return true;
}

bool PingStorage::StoreRecords(const std::vector<module::ping::PingRecord>& records)
{
    if (records.empty())
    {
        return true;
    }

    std::lock_guard<std::mutex> lock(db_mutex_);

    if (db_ == nullptr)
    {
        SPDLOG_ERROR("PingStorage is not initialized");
        return false;
    }

    if (!BeginTransaction())
    {
        return false;
    }

    for (const auto& record : records)
    {
        if (!InsertRecordInternal(record))
        {
            RollbackTransaction();
            return false;
        }
    }

    if (!RunRetentionPolicy())
    {
        RollbackTransaction();
        return false;
    }

    if (!CommitTransaction())
    {
        RollbackTransaction();
        return false;
    }

    return true;
}

std::int64_t PingStorage::GetRowCount() const
{
    if (db_ == nullptr)
    {
        return 0;
    }

    sqlite3_stmt* stmt = nullptr;
    const int rc_prepare = sqlite3_prepare_v2(db_, kCountSql, -1, &stmt, nullptr);
    if (rc_prepare != SQLITE_OK)
    {
        SPDLOG_ERROR("Count prepare failed: {}", sqlite3_errmsg(db_));
        return 0;
    }

    std::int64_t count = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW)
    {
        count = sqlite3_column_int64(stmt, 0);
    }

    sqlite3_finalize(stmt);
    return count;
}

bool PingStorage::Open()
{
    if (db_ != nullptr)
    {
        return true;
    }

    const int rc = sqlite3_open(config_.db_path.c_str(), &db_);
    if (rc != SQLITE_OK)
    {
        SPDLOG_ERROR("sqlite open failed: {}", sqlite3_errmsg(db_));
        Close();
        return false;
    }

    return true;
}

void PingStorage::Close()
{
    if (db_ != nullptr)
    {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

bool PingStorage::ConfigurePragmas()
{
    char* err_msg = nullptr;

    if (sqlite3_exec(db_, "PRAGMA journal_mode=WAL;", nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("PRAGMA journal_mode=WAL failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return false;
    }

    if (sqlite3_exec(db_, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("PRAGMA synchronous=NORMAL failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return false;
    }

    if (sqlite3_exec(db_, "PRAGMA busy_timeout=5000;", nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("PRAGMA busy_timeout failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return false;
    }

    return true;
}

bool PingStorage::CreateSchema()
{
    char* err_msg = nullptr;

    if (sqlite3_exec(db_, kCreateTableSql, nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("Create table failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return false;
    }

    if (sqlite3_exec(db_, kCreateTimestampIndexSql, nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("Create timestamp index failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return false;
    }

    if (sqlite3_exec(db_, kCreateProcessedIndexSql, nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("Create processed index failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return false;
    }

    return true;
}

bool PingStorage::InsertRecordInternal(const module::ping::PingRecord& record)
{
    sqlite3_stmt* stmt = nullptr;
    const int rc_prepare = sqlite3_prepare_v2(db_, kInsertSql, -1, &stmt, nullptr);
    if (rc_prepare != SQLITE_OK)
    {
        SPDLOG_ERROR("Insert prepare failed: {}", sqlite3_errmsg(db_));
        return false;
    }

    sqlite3_bind_int64(stmt, 1, record.timestamp_ms);
    sqlite3_bind_text(stmt, 2, record.target.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, record.success ? 1 : 0);

    if (record.latency_ms.has_value())
    {
        sqlite3_bind_double(stmt, 4, *record.latency_ms);
    }
    else
    {
        sqlite3_bind_null(stmt, 4);
    }

    sqlite3_bind_int(stmt, 5, record.timeout ? 1 : 0);
    sqlite3_bind_text(stmt, 6, record.error_message.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 7, record.sequence_number);

    const int rc_step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc_step != SQLITE_DONE)
    {
        SPDLOG_ERROR("Insert step failed: {}", sqlite3_errmsg(db_));
        return false;
    }

    return true;
}

bool PingStorage::BeginTransaction()
{
    char* err_msg = nullptr;
    if (sqlite3_exec(db_, "BEGIN IMMEDIATE TRANSACTION;", nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("Begin transaction failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return false;
    }

    return true;
}

bool PingStorage::CommitTransaction()
{
    char* err_msg = nullptr;
    if (sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("Commit transaction failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return false;
    }

    return true;
}

void PingStorage::RollbackTransaction()
{
    sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
}

std::size_t PingStorage::DeleteProcessedRowsInternal(std::size_t limit)
{
    if (limit == 0)
    {
        return 0;
    }

    const std::string sql =
        "DELETE FROM ping_results "
        "WHERE id IN ("
        "  SELECT id FROM ping_results "
        "  WHERE processed = 1 "
        "  ORDER BY id ASC "
        "  LIMIT " + std::to_string(limit) +
        ");";

    char* err_msg = nullptr;
    if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("Delete processed rows failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return 0;
    }

    return static_cast<std::size_t>(sqlite3_changes(db_));
}

std::size_t PingStorage::DeleteOldestRowsInternal(std::size_t limit)
{
    if (limit == 0)
    {
        return 0;
    }

    const std::string sql =
        "DELETE FROM ping_results "
        "WHERE id IN ("
        "  SELECT id FROM ping_results "
        "  ORDER BY id ASC "
        "  LIMIT " + std::to_string(limit) +
        ");";

    char* err_msg = nullptr;
    if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        SPDLOG_ERROR("Delete oldest rows failed: {}", err_msg ? err_msg : "unknown");
        sqlite3_free(err_msg);
        return 0;
    }

    return static_cast<std::size_t>(sqlite3_changes(db_));
}

bool PingStorage::RunRetentionPolicy()
{
    const auto total_rows_initial = GetRowCount();
    if (total_rows_initial <= 0)
    {
        return true;
    }

    if (static_cast<std::size_t>(total_rows_initial) <= config_.max_rows)
    {
        return true;
    }

    std::size_t removed_processed = 0;
    while (static_cast<std::size_t>(GetRowCount()) > config_.max_rows)
    {
        const auto removed = DeleteProcessedRowsInternal(config_.processed_cleanup_batch);
        if (removed == 0)
        {
            break;
        }

        removed_processed += removed;
    }

    if (removed_processed > 0)
    {
        SPDLOG_INFO("PingStorage retention removed {} processed rows", removed_processed);
    }

    if (static_cast<std::size_t>(GetRowCount()) <= config_.max_rows)
    {
        return true;
    }

    std::size_t removed_forced = 0;
    while (static_cast<std::size_t>(GetRowCount()) > config_.max_rows)
    {
        const auto removed = DeleteOldestRowsInternal(config_.forced_purge_batch);
        if (removed == 0)
        {
            SPDLOG_ERROR("PingStorage forced purge could not remove rows");
            return false;
        }

        removed_forced += removed;
    }

    if (removed_forced > 0)
    {
        SPDLOG_WARN("PingStorage forced purge removed {} oldest rows", removed_forced);
    }

    return true;
}

} // namespace pes::storage