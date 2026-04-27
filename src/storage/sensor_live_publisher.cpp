#include "pes/storage/sensor_live_publisher.hpp"

#include "pes/utils/file/thirdparty/json.hpp"

#include <cctype>
#include <spdlog/spdlog.h>
#include <utility>

namespace storage
{

namespace
{
using json = nlohmann::json;

bool command_ok(const std::optional<RedisStorage::Reply>& reply)
{
    return reply.has_value() && !reply->IsError();
}

json parse_raw_json(const std::string& payload)
{
    if (payload.empty())
    {
        return nullptr;
    }

    json parsed = json::parse(payload, nullptr, false);
    if (parsed.is_discarded())
    {
        return payload;
    }

    return parsed;
}
}

SensorLivePublisher::SensorLivePublisher(RedisStorage& redis, Config config)
    : redis_(redis)
    , config_(std::move(config))
{
}

bool SensorLivePublisher::PublishDeviceState(const SensorDeviceState& state)
{
    const std::string state_key = StateKey(state.source, state.device_id);
    const std::string samples_key = SamplesKey(state.source, state.device_id);
    const std::string payload = SerializeDeviceState(state);

    bool ok = true;
    if (!redis_.Set(state_key, payload))
    {
        SPDLOG_WARN("Failed to publish Redis sensor state key={}", state_key);
        ok = false;
    }

    if (!state.metrics.empty() && !PushSamplePacket(samples_key, payload))
    {
        ok = false;
    }

    return ok;
}

bool SensorLivePublisher::PublishDeviceIndex(
    const std::int64_t timestamp_ms,
    const std::vector<DeviceIndexEntry>& devices)
{
    const std::string payload = SerializeDeviceIndex(timestamp_ms, devices);
    if (!redis_.Set(config_.device_index_key, payload))
    {
        SPDLOG_WARN("Failed to publish Redis sensor device index key={}", config_.device_index_key);
        return false;
    }

    return true;
}

std::string SensorLivePublisher::StateKey(const std::string_view source, const std::string_view device_id)
{
    return "pes:device:" + KeyComponent(source) + ":" + KeyComponent(device_id) + ":state";
}

std::string SensorLivePublisher::SamplesKey(const std::string_view source, const std::string_view device_id)
{
    return "pes:device:" + KeyComponent(source) + ":" + KeyComponent(device_id) + ":samples";
}

bool SensorLivePublisher::PushSamplePacket(const std::string& key, const std::string& payload)
{
    const auto push_reply = redis_.PushLeft(key, std::vector<std::string>{payload});
    bool ok = push_reply.has_value();
    if (!ok)
    {
        SPDLOG_WARN("Failed to push Redis sensor sample key={}", key);
    }

    const std::size_t end_index = config_.sample_buffer_size == 0 ? 0 : config_.sample_buffer_size - 1;
    const auto trim_reply = redis_.Command("LTRIM", key, "0", std::to_string(end_index));
    if (!command_ok(trim_reply))
    {
        SPDLOG_WARN("Failed to trim Redis sensor sample key={}", key);
        ok = false;
    }

    return ok;
}

std::string SensorLivePublisher::KeyComponent(const std::string_view value)
{
    std::string key;
    key.reserve(value.size());

    for (const char character : value)
    {
        const auto byte = static_cast<unsigned char>(character);
        if (std::isalnum(byte) || character == '_' || character == '-' || character == '.')
        {
            key.push_back(character);
        }
        else
        {
            key.push_back('_');
        }
    }

    return key.empty() ? "unknown" : key;
}

std::string SensorLivePublisher::SerializeDeviceState(const SensorDeviceState& state)
{
    json payload;
    payload["timestamp_ms"] = state.timestamp_ms;
    payload["source"] = state.source;
    payload["device_id"] = state.device_id;
    payload["name"] = state.device_name;
    payload["device_type"] = state.device_type;
    payload["status"] = state.status;

    json transport;
    transport["type"] = state.transport_type;
    transport["endpoint"] = state.endpoint;
    if (!state.network_interface.empty())
    {
        transport["interface"] = state.network_interface;
    }
    if (state.port.has_value())
    {
        transport["port"] = *state.port;
    }
    if (state.slave_address.has_value())
    {
        transport["slave_address"] = *state.slave_address;
    }
    if (state.unit_id.has_value())
    {
        transport["unit_id"] = *state.unit_id;
    }
    payload["transport"] = transport;

    json metrics = json::object();
    for (const auto& metric : state.metrics)
    {
        metrics[metric.name] = {
            {"value", metric.value},
            {"unit", metric.unit},
            {"quality", metric.quality},
            {"timestamp_ms", metric.timestamp_ms},
        };
    }
    payload["metrics"] = metrics;

    if (state.error.has_value())
    {
        payload["error"] = {
            {"timestamp_ms", state.error->timestamp_ms},
            {"severity", state.error->severity},
            {"type", state.error->event_type},
            {"message", state.error->message},
            {"details", parse_raw_json(state.error->details_json)},
        };
    }
    else
    {
        payload["error"] = nullptr;
    }

    if (!state.raw_json.empty())
    {
        payload["raw"] = parse_raw_json(state.raw_json);
    }

    return payload.dump();
}

std::string SensorLivePublisher::SerializeDeviceIndex(
    const std::int64_t timestamp_ms,
    const std::vector<DeviceIndexEntry>& devices)
{
    json payload;
    payload["timestamp_ms"] = timestamp_ms;
    payload["devices"] = json::array();

    for (const auto& device : devices)
    {
        payload["devices"].push_back({
            {"key", device.state_key.empty() ? StateKey(device.source, device.device_id) : device.state_key},
            {"source", device.source},
            {"device_id", device.device_id},
            {"name", device.device_name},
            {"device_type", device.device_type},
        });
    }

    return payload.dump();
}

} // namespace storage
