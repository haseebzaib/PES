#pragma once

#include "pes/storage/sqlite_storage.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace storage
{

struct SensorMetricValue
{
    std::string name {};
    double value {0.0};
    std::string unit {};
    std::string quality {"good"};
    std::int64_t timestamp_ms {0};
};

struct SensorEventInfo
{
    std::int64_t timestamp_ms {0};
    std::string severity {"warning"};
    std::string event_type {};
    std::string message {};
    std::string details_json {};
};

struct SensorSampleRecord
{
    std::int64_t timestamp_ms {0};
    std::string source {};
    std::string device_id {};
    std::string device_name {};
    std::string device_type {};
    std::string metric {};
    double value {0.0};
    std::string unit {};
    std::string quality {"good"};
    std::string details_json {};
};

struct SensorEventRecord
{
    std::int64_t timestamp_ms {0};
    std::string source {};
    std::string device_id {};
    std::string device_name {};
    std::string device_type {};
    std::string severity {"warning"};
    std::string event_type {};
    std::string message {};
    std::string details_json {};
};

struct SensorDeviceState
{
    std::int64_t timestamp_ms {0};
    std::string source {};
    std::string device_id {};
    std::string device_name {};
    std::string device_type {};
    std::string status {"ok"};

    std::string transport_type {};
    std::string endpoint {};
    std::string network_interface {};
    std::optional<int> port {};
    std::optional<int> slave_address {};
    std::optional<int> unit_id {};

    std::vector<SensorMetricValue> metrics {};
    std::optional<SensorEventInfo> error {};
    std::string raw_json {};
};

class SensorStorage
{
public:
    struct Config
    {
        std::string db_path {"/opt/gateway/software_storage/PES/pes.db"};
        std::uint64_t max_database_bytes {5ULL * 1024ULL * 1024ULL * 1024ULL};
    };

public:
    explicit SensorStorage(Config config);

    bool Initialize();
    void Shutdown();

    bool StoreSample(const SensorSampleRecord& record);
    bool StoreSamples(const std::vector<SensorSampleRecord>& records);
    bool StoreEvent(const SensorEventRecord& record);
    bool StoreEvents(const std::vector<SensorEventRecord>& records);

private:
    static SqliteStorage::Config MakeSqliteConfig(const Config& config);

    bool InsertSample(const SensorSampleRecord& record);
    bool InsertEvent(const SensorEventRecord& record);

private:
    Config config_;
    SqliteStorage db_;
};

} // namespace storage
