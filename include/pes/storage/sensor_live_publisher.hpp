#pragma once

#include "pes/storage/redis_storage.hpp"
#include "pes/storage/sensor_storage.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace storage
{

class SensorLivePublisher
{
public:
    struct Config
    {
        std::string device_index_key {"pes:devices:index"};
        std::size_t sample_buffer_size {100};
    };

    struct DeviceIndexEntry
    {
        std::string source {};
        std::string device_id {};
        std::string device_name {};
        std::string state_key {};
    };

public:
    SensorLivePublisher(RedisStorage& redis, Config config);

    bool PublishDeviceState(const SensorDeviceState& state);
    bool PublishDeviceIndex(std::int64_t timestamp_ms, const std::vector<DeviceIndexEntry>& devices);

    [[nodiscard]] static std::string StateKey(std::string_view source, std::string_view device_id);
    [[nodiscard]] static std::string SamplesKey(std::string_view source, std::string_view device_id);

private:
    bool PushSamplePacket(const std::string& key, const std::string& payload);

    static std::string KeyComponent(std::string_view value);
    static std::string SerializeDeviceState(const SensorDeviceState& state);
    static std::string SerializeDeviceIndex(std::int64_t timestamp_ms, const std::vector<DeviceIndexEntry>& devices);

private:
    RedisStorage& redis_;
    Config config_;
};

} // namespace storage
