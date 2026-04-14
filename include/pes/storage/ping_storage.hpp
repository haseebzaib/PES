#pragma once

#include "pes/modules/ping/ping.hpp"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

struct sqlite3;

namespace pes::storage
{

class PingStorage
{
public:
    struct Config
    {
        std::string db_path {};
        std::size_t max_rows {100000};
        std::size_t processed_cleanup_batch {1000};
        std::size_t forced_purge_batch {1000};
    };

public:
    explicit PingStorage(Config config);
    ~PingStorage();

    PingStorage(const PingStorage&) = delete;
    PingStorage& operator=(const PingStorage&) = delete;

    bool Initialize();
    void Shutdown();

    bool StoreRecord(const module::ping::PingRecord& record);
    bool StoreRecords(const std::vector<module::ping::PingRecord>& records);

    std::int64_t GetRowCount() const;

private:
    bool Open();
    void Close();
    bool ConfigurePragmas();
    bool CreateSchema();

    bool InsertRecordInternal(const module::ping::PingRecord& record);
    bool BeginTransaction();
    bool CommitTransaction();
    void RollbackTransaction();

    std::size_t DeleteProcessedRowsInternal(std::size_t limit);
    std::size_t DeleteOldestRowsInternal(std::size_t limit);
    bool RunRetentionPolicy();

private:
    Config config_;
    sqlite3* db_ {nullptr};
    mutable std::mutex db_mutex_;
};

} // namespace pes::storage