#include "pes/storage/sensor_storage.hpp"

#include <spdlog/spdlog.h>
#include <sqlite3.h>

#include <utility>

namespace storage
{

namespace
{
constexpr const char* kInsertSampleSql =
    "INSERT INTO sensor_samples "
    "(timestamp_ms, source, device_id, device_name, device_type, metric, value, unit, quality, details_json, processed) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0);";

constexpr const char* kInsertEventSql =
    "INSERT INTO sensor_events "
    "(timestamp_ms, source, device_id, device_name, device_type, severity, event_type, message, details_json, processed) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0);";

void bind_text(sqlite3_stmt* stmt, const int index, const std::string& value)
{
    sqlite3_bind_text(stmt, index, value.c_str(), -1, SQLITE_TRANSIENT);
}
}

SensorStorage::SensorStorage(Config config)
    : config_(std::move(config))
    , db_(MakeSqliteConfig(config_))
{
}

bool SensorStorage::Initialize()
{
    return db_.Initialize();
}

void SensorStorage::Shutdown()
{
    db_.Shutdown();
}

bool SensorStorage::StoreSample(const SensorSampleRecord& record)
{
    return StoreSamples({record});
}

bool SensorStorage::StoreSamples(const std::vector<SensorSampleRecord>& records)
{
    if (records.empty())
    {
        return true;
    }

    if (db_.EnsureReady() && StoreSamplesOnce(records))
    {
        return true;
    }

    SPDLOG_WARN("Sensor sample storage failed; attempting SQLite recovery path={}", config_.db_path);
    if (!db_.Recover())
    {
        return false;
    }

    return StoreSamplesOnce(records);
}

bool SensorStorage::StoreEvent(const SensorEventRecord& record)
{
    return StoreEvents({record});
}

bool SensorStorage::StoreEvents(const std::vector<SensorEventRecord>& records)
{
    if (records.empty())
    {
        return true;
    }

    if (db_.EnsureReady() && StoreEventsOnce(records))
    {
        return true;
    }

    SPDLOG_WARN("Sensor event storage failed; attempting SQLite recovery path={}", config_.db_path);
    if (!db_.Recover())
    {
        return false;
    }

    return StoreEventsOnce(records);
}

bool SensorStorage::StoreSamplesOnce(const std::vector<SensorSampleRecord>& records)
{
    if (!db_.BeginImmediateTransaction())
    {
        return false;
    }

    for (const auto& record : records)
    {
        if (!InsertSample(record))
        {
            db_.RollbackTransaction();
            return false;
        }
    }

    if (!db_.CommitTransaction())
    {
        db_.RollbackTransaction();
        return false;
    }

    if (!db_.RunRetentionPolicy())
    {
        SPDLOG_WARN("SQLite retention failed after storing samples; attempting recovery path={}", config_.db_path);
        static_cast<void>(db_.Recover());
    }

    return true;
}

bool SensorStorage::StoreEventsOnce(const std::vector<SensorEventRecord>& records)
{
    if (!db_.BeginImmediateTransaction())
    {
        return false;
    }

    for (const auto& record : records)
    {
        if (!InsertEvent(record))
        {
            db_.RollbackTransaction();
            return false;
        }
    }

    if (!db_.CommitTransaction())
    {
        db_.RollbackTransaction();
        return false;
    }

    if (!db_.RunRetentionPolicy())
    {
        SPDLOG_WARN("SQLite retention failed after storing events; attempting recovery path={}", config_.db_path);
        static_cast<void>(db_.Recover());
    }

    return true;
}

SqliteStorage::Config SensorStorage::MakeSqliteConfig(const Config& config)
{
    return SqliteStorage::Config{
        .db_path = config.db_path,
        .create_parent_directories = true,
        .max_database_bytes = config.max_database_bytes,
        .schema_statements = {
            "CREATE TABLE IF NOT EXISTS sensor_samples ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "timestamp_ms INTEGER NOT NULL,"
            "source TEXT NOT NULL,"
            "device_id TEXT NOT NULL,"
            "device_name TEXT,"
            "device_type TEXT,"
            "metric TEXT NOT NULL,"
            "value REAL,"
            "unit TEXT,"
            "quality TEXT NOT NULL,"
            "details_json TEXT,"
            "processed INTEGER NOT NULL DEFAULT 0"
            ");",
            "CREATE INDEX IF NOT EXISTS idx_sensor_samples_time ON sensor_samples(timestamp_ms);",
            "CREATE INDEX IF NOT EXISTS idx_sensor_samples_device_time ON sensor_samples(device_id, timestamp_ms);",
            "CREATE INDEX IF NOT EXISTS idx_sensor_samples_processed ON sensor_samples(processed);",

            "CREATE TABLE IF NOT EXISTS sensor_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "timestamp_ms INTEGER NOT NULL,"
            "source TEXT NOT NULL,"
            "device_id TEXT NOT NULL,"
            "device_name TEXT,"
            "device_type TEXT,"
            "severity TEXT NOT NULL,"
            "event_type TEXT NOT NULL,"
            "message TEXT NOT NULL,"
            "details_json TEXT,"
            "processed INTEGER NOT NULL DEFAULT 0"
            ");",
            "CREATE INDEX IF NOT EXISTS idx_sensor_events_time ON sensor_events(timestamp_ms);",
            "CREATE INDEX IF NOT EXISTS idx_sensor_events_device_time ON sensor_events(device_id, timestamp_ms);",
            "CREATE INDEX IF NOT EXISTS idx_sensor_events_processed ON sensor_events(processed);",
        },
        .retention_targets = {
            {.table_name = "sensor_samples"},
            {.table_name = "sensor_events"},
        },
    };
}

bool SensorStorage::InsertSample(const SensorSampleRecord& record)
{
    return db_.ExecutePrepared(kInsertSampleSql, [&record](sqlite3_stmt* stmt) {
        sqlite3_bind_int64(stmt, 1, record.timestamp_ms);
        bind_text(stmt, 2, record.source);
        bind_text(stmt, 3, record.device_id);
        bind_text(stmt, 4, record.device_name);
        bind_text(stmt, 5, record.device_type);
        bind_text(stmt, 6, record.metric);
        sqlite3_bind_double(stmt, 7, record.value);
        bind_text(stmt, 8, record.unit);
        bind_text(stmt, 9, record.quality);
        bind_text(stmt, 10, record.details_json);
        return true;
    });
}

bool SensorStorage::InsertEvent(const SensorEventRecord& record)
{
    return db_.ExecutePrepared(kInsertEventSql, [&record](sqlite3_stmt* stmt) {
        sqlite3_bind_int64(stmt, 1, record.timestamp_ms);
        bind_text(stmt, 2, record.source);
        bind_text(stmt, 3, record.device_id);
        bind_text(stmt, 4, record.device_name);
        bind_text(stmt, 5, record.device_type);
        bind_text(stmt, 6, record.severity);
        bind_text(stmt, 7, record.event_type);
        bind_text(stmt, 8, record.message);
        bind_text(stmt, 9, record.details_json);
        return true;
    });
}

} // namespace storage
