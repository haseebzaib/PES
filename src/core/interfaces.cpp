#include "pes/core/application.hpp"
#include "pes/core/sensor_thread.hpp"
#include "pes/core/interfaces.hpp"
#include "pes/modules/protocols/serial/serial.hpp"
#include "pes/modules/protocols/modbus/modbus_client.hpp"
#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "pes/utils/file/file_reader.hpp"
#include "pes/utils/file/json_file.hpp"
#include "spdlog/spdlog.h"
#include "vector"
#include <algorithm>
#include <cmath>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <utility>
#include "cstdlib"

namespace core
{

    std::array<sensorRuntime, 4> sensors;
    std::array<modbusRtuRuntime, 2> modbusRtuRuntimes;
    std::array<modbusTcpRuntime, modbusTcpMaxConnections> modbusTcpRuntimes;
    utils::file::jsonFile::json root_packet;
    constexpr std::int64_t dustrakMeasurementStaleAfterMs = 5000;
    constexpr std::int64_t minimumMetricStaleAfterMs = 5000;
    constexpr int runtimeErrorThreshold = 3;

    struct rs232PortMap
    {
        std::string_view key;
        std::string_view devicePath;
        std::size_t sensorIndex;
    };

    constexpr std::array<rs232PortMap, 2> rs232Ports{{
        {"port_0", rs232Ch0Path, 0},
        {"port_1", rs232Ch1Path, 1},
    }};

    struct rs485PortMap
    {
        std::string_view key;
        std::string_view devicePath;
        std::size_t runtimeIndex;
    };

    constexpr std::array<rs485PortMap, 2> rs485Ports{{
        {"port_2", rs485Ch2Path, 0},
        {"port_3", rs485Ch3Path, 1},
    }};

    static std::int64_t unix_timestamp_ms()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    static std::string json_dump(const utils::file::jsonFile::json &value)
    {
        return value.dump();
    }

    static std::string modbus_tcp_device_id(const modbusTcpRuntime &runtime, const std::size_t slot)
    {
        return runtime.config.id.empty() ? "slot_" + std::to_string(slot) : runtime.config.id;
    }

    static std::string sensor_kind_text(const sensorKind kind)
    {
        switch (kind)
        {
        case sensorKind::dustrak:
            return "dustrak";
        case sensorKind::None:
        case sensorKind::TOTAL:
        default:
            return "unknown";
        }
    }

    static std::string default_sensor_device_name(const sensorKind kind, const std::string_view deviceId)
    {
        switch (kind)
        {
        case sensorKind::dustrak:
            return "DustTrak " + std::string(deviceId);
        case sensorKind::None:
        case sensorKind::TOTAL:
        default:
            return std::string(deviceId);
        }
    }

    static void store_sensor_samples(const std::vector<storage::SensorSampleRecord> &samples)
    {
        if (!samples.empty() && !sensor_storage.StoreSamples(samples))
        {
            SPDLOG_WARN("Failed to store {} sensor sample records", samples.size());
        }
    }

    static void store_sensor_event(const storage::SensorEventRecord &event)
    {
        if (!sensor_storage.StoreEvent(event))
        {
            SPDLOG_WARN("Failed to store sensor event source={} device={} type={}", event.source, event.device_id, event.event_type);
        }
    }

    static void publish_sensor_state(const storage::SensorDeviceState &state)
    {
        static_cast<void>(sensor_live_publisher.PublishDeviceState(state));
    }

    static std::string modbus_register_details_json(const module::protocols::modbus::registerConfig &config)
    {
        utils::file::jsonFile::json details;
        details["address"] = config.address;
        details["scale"] = config.scale;
        details["unit"] = config.unit;
        details["register_type"] = config.registerType_ == module::protocols::modbus::registerType::Input
                                       ? "input_register"
                                       : "holding_register";
        return json_dump(details);
    }

    static storage::SensorEventRecord make_sensor_event(
        const std::int64_t timestampMs,
        std::string source,
        std::string deviceId,
        std::string deviceName,
        std::string deviceType,
        std::string severity,
        std::string eventType,
        std::string message,
        std::string detailsJson = {})
    {
        return {
            .timestamp_ms = timestampMs,
            .source = std::move(source),
            .device_id = std::move(deviceId),
            .device_name = std::move(deviceName),
            .device_type = std::move(deviceType),
            .severity = std::move(severity),
            .event_type = std::move(eventType),
            .message = std::move(message),
            .details_json = std::move(detailsJson),
        };
    }

    static std::unordered_map<std::string, std::string> activeSensorEventSignatures;

    static std::string sensor_event_key(
        const std::string_view source,
        const std::string_view deviceId,
        const std::string_view eventType)
    {
        return std::string(source) + ":" + std::string(deviceId) + ":" + std::string(eventType);
    }

    static void clear_active_sensor_events(
        const std::string_view source,
        const std::string_view deviceId)
    {
        const std::string prefix = std::string(source) + ":" + std::string(deviceId) + ":";
        for (auto it = activeSensorEventSignatures.begin(); it != activeSensorEventSignatures.end();)
        {
            if (it->first.rfind(prefix, 0) == 0)
            {
                it = activeSensorEventSignatures.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

    static void store_sensor_event_once(
        const std::int64_t timestampMs,
        const std::string &source,
        const std::string &deviceId,
        const std::string &deviceName,
        const std::string &deviceType,
        const storage::SensorEventInfo &event)
    {
        const std::string key = sensor_event_key(source, deviceId, event.event_type);
        const std::string signature = event.severity + "|" + event.message;
        auto active = activeSensorEventSignatures.find(key);
        if (active != activeSensorEventSignatures.end() && active->second == signature)
        {
            return;
        }

        activeSensorEventSignatures[key] = signature;
        store_sensor_event(make_sensor_event(
            timestampMs,
            source,
            deviceId,
            deviceName,
            deviceType,
            event.severity,
            event.event_type,
            event.message,
            event.details_json));
    }

    static void publish_device_index()
    {
        std::vector<storage::SensorLivePublisher::DeviceIndexEntry> devices;

        for (const sensorRuntime &runtime : sensors)
        {
            if (!runtime.config.enabled)
            {
                continue;
            }

            if (runtime.config.transportKind_ == transportKind::RS232)
            {
                devices.push_back({
                    .source = "rs232",
                    .device_id = runtime.config.name,
                    .device_name = runtime.config.deviceName,
                    .device_type = runtime.config.deviceType,
                    .state_key = storage::SensorLivePublisher::StateKey("rs232", runtime.config.name),
                });
            }
        }

        for (const modbusRtuRuntime &runtime : modbusRtuRuntimes)
        {
            if (!runtime.enabled)
            {
                continue;
            }

            devices.push_back({
                .source = "rs485",
                .device_id = runtime.config.name,
                .device_name = runtime.config.name,
                .device_type = "modbus_rtu",
                .state_key = storage::SensorLivePublisher::StateKey("rs485", runtime.config.name),
            });
        }

        for (std::size_t index = 0; index < modbusTcpRuntimes.size(); ++index)
        {
            const modbusTcpRuntime &runtime = modbusTcpRuntimes[index];
            if (!runtime.enabled)
            {
                continue;
            }

            const std::string deviceId = modbus_tcp_device_id(runtime, index);
            devices.push_back({
                .source = "modbus_tcp",
                .device_id = deviceId,
                .device_name = runtime.config.name,
                .device_type = "modbus_tcp",
                .state_key = storage::SensorLivePublisher::StateKey("modbus_tcp", deviceId),
            });
        }

        static_cast<void>(sensor_live_publisher.PublishDeviceIndex(unix_timestamp_ms(), devices));
    }

    static std::vector<storage::SensorSampleRecord> build_modbus_sample_records(
        const std::int64_t timestampMs,
        const std::string &source,
        const std::string &deviceId,
        const std::string &deviceName,
        const std::string &deviceType,
        const std::vector<module::protocols::modbus::sample> &samples)
    {
        std::vector<storage::SensorSampleRecord> records;
        records.reserve(samples.size());

        for (const auto &sample : samples)
        {
            records.push_back({
                .timestamp_ms = timestampMs,
                .source = source,
                .device_id = deviceId,
                .device_name = deviceName,
                .device_type = deviceType,
                .metric = sample.name,
                .value = sample.value,
                .unit = sample.unit,
                .quality = "good",
                .details_json = modbus_register_details_json(sample.source),
            });
        }

        return records;
    }

    static void ensure_modbus_metric_cache(
        std::vector<runtimeMetricCache> &cache,
        const std::vector<module::protocols::modbus::registerConfig> &registers)
    {
        if (cache.size() != registers.size())
        {
            cache.clear();
            cache.reserve(registers.size());
            for (const auto &config : registers)
            {
                cache.push_back({
                    .name = config.name,
                    .unit = config.unit,
                });
            }
            return;
        }

        for (std::size_t index = 0; index < registers.size(); ++index)
        {
            cache[index].name = registers[index].name;
            cache[index].unit = registers[index].unit;
        }
    }

    static std::int64_t modbus_stale_after_ms(const std::chrono::milliseconds pollInterval)
    {
        return std::max<std::int64_t>(minimumMetricStaleAfterMs, pollInterval.count() * 3);
    }

    static const module::protocols::modbus::readError *find_modbus_error(
        const std::vector<module::protocols::modbus::readError> &errors,
        const std::string &metric)
    {
        const auto match = std::ranges::find_if(errors, [&metric](const auto &error) {
            return error.name == metric;
        });
        return match == errors.end() ? nullptr : &(*match);
    }

    static void update_modbus_metric_cache(
        std::vector<runtimeMetricCache> &cache,
        const std::vector<module::protocols::modbus::registerConfig> &registers,
        const std::vector<module::protocols::modbus::sample> &samples,
        const std::int64_t timestampMs)
    {
        ensure_modbus_metric_cache(cache, registers);

        for (const auto &sample : samples)
        {
            auto match = std::ranges::find_if(cache, [&sample](const runtimeMetricCache &entry) {
                return entry.name == sample.name;
            });

            if (match == cache.end())
            {
                continue;
            }

            match->value = sample.value;
            match->unit = sample.unit;
            match->timestamp_ms = timestampMs;
            match->has_value = true;
        }
    }

    static void reset_runtime_health(runtimeDeviceHealth &health)
    {
        health.consecutiveFailures = 0;
        health.lastFailedMetric.clear();
        health.lastError.clear();
        health.lastEventType.clear();
    }

    static void record_runtime_success(runtimeDeviceHealth &health, const std::int64_t timestampMs)
    {
        reset_runtime_health(health);
        health.lastSuccessTimestampMs = timestampMs;
    }

    static void record_runtime_failure(
        runtimeDeviceHealth &health,
        const std::int64_t timestampMs,
        std::string eventType,
        std::string error,
        std::string failedMetric = {})
    {
        ++health.consecutiveFailures;
        health.lastFailureTimestampMs = timestampMs;
        health.lastEventType = std::move(eventType);
        health.lastError = std::move(error);
        health.lastFailedMetric = std::move(failedMetric);
    }

    static bool modbus_cache_has_stale_metrics(
        const std::vector<runtimeMetricCache> &cache,
        const std::int64_t timestampMs,
        const std::int64_t staleAfterMs)
    {
        return std::ranges::any_of(cache, [timestampMs, staleAfterMs](const runtimeMetricCache &entry) {
            return !entry.has_value || timestampMs - entry.timestamp_ms > staleAfterMs;
        });
    }

    static std::vector<storage::SensorMetricValue> build_modbus_metrics(
        const std::int64_t timestampMs,
        const std::vector<runtimeMetricCache> &cache,
        const std::vector<module::protocols::modbus::readError> &errors,
        const std::int64_t staleAfterMs)
    {
        std::vector<storage::SensorMetricValue> metrics;
        metrics.reserve(cache.size());

        for (const auto &entry : cache)
        {
            const bool failedThisPoll = find_modbus_error(errors, entry.name) != nullptr;
            const bool stale = !entry.has_value || timestampMs - entry.timestamp_ms > staleAfterMs;
            const std::string quality = failedThisPoll ? "error" : stale ? "stale" : "good";

            metrics.push_back({
                .name = entry.name,
                .value = entry.value,
                .unit = entry.unit,
                .quality = quality,
                .timestamp_ms = entry.has_value ? entry.timestamp_ms : timestampMs,
            });
        }

        return metrics;
    }

    static storage::SensorEventInfo make_runtime_error_info(
        const std::int64_t timestampMs,
        const runtimeDeviceHealth &health,
        const std::string &message,
        const std::string &severity)
    {
        utils::file::jsonFile::json details;
        details["consecutive_failures"] = health.consecutiveFailures;
        details["last_success_timestamp_ms"] = health.lastSuccessTimestampMs;
        details["last_failure_timestamp_ms"] = health.lastFailureTimestampMs;
        details["last_failed_metric"] = health.lastFailedMetric;
        details["last_error"] = health.lastError;
        details["last_event_type"] = health.lastEventType;

        return {
            .timestamp_ms = timestampMs,
            .severity = severity,
            .event_type = health.lastEventType.empty() ? "poll_failed" : health.lastEventType,
            .message = message,
            .details_json = json_dump(details),
        };
    }

    static std::vector<storage::SensorSampleRecord> build_dustrak_sample_records(
        const std::int64_t timestampMs,
        const sensorRuntime &runtime)
    {
        constexpr std::array<std::string_view, 5> channels{{
            "pm1",
            "pm25",
            "pm4",
            "pm10",
            "total",
        }};

        const auto &state = runtime.dustTrak.state();
        std::vector<storage::SensorSampleRecord> records;
        const bool useMeasurementStats = state.latestMeasurement.channelValuesMgPerM3.empty()
                                         && state.latestMeasurementStats.has_value();
        const std::size_t valueCount = useMeasurementStats
                                           ? state.latestMeasurementStats->channels.size()
                                           : state.latestMeasurement.channelValuesMgPerM3.size();
        records.reserve(valueCount);

        for (std::size_t index = 0; index < valueCount; ++index)
        {
            const std::string metric = index < channels.size()
                                           ? std::string(channels[index])
                                           : "channel_" + std::to_string(index);
            const double value = useMeasurementStats
                                     ? state.latestMeasurementStats->channels[index].currentMgPerM3
                                     : state.latestMeasurement.channelValuesMgPerM3[index];
            utils::file::jsonFile::json details;
            details["elapsed_seconds"] = useMeasurementStats
                                             ? state.latestMeasurementStats->elapsedSeconds
                                             : state.latestMeasurement.elapsedSeconds;
            details["channel_index"] = index;
            details["model"] = state.identity.modelText;
            details["serial_number"] = state.identity.serialNumber;
            details["source_command"] = useMeasurementStats ? "RMMEASSTATS" : "RMMEAS";

            records.push_back({
                .timestamp_ms = timestampMs,
                .source = "rs232",
                .device_id = runtime.config.name,
                .device_name = runtime.config.deviceName,
                .device_type = runtime.config.deviceType,
                .metric = metric,
                .value = value,
                .unit = "mg/m3",
                .quality = "good",
                .details_json = json_dump(details),
            });
        }

        return records;
    }

    static std::vector<storage::SensorMetricValue> build_dustrak_metrics(
        const std::int64_t timestampMs,
        const sensorRuntime &runtime,
        const std::string &quality)
    {
        constexpr std::array<std::string_view, 5> channels{{
            "pm1",
            "pm25",
            "pm4",
            "pm10",
            "total",
        }};

        const auto &state = runtime.dustTrak.state();
        const std::int64_t metricTimestampMs = runtime.dustTrak.last_measurement_timestamp_ms() > 0
                                                   ? runtime.dustTrak.last_measurement_timestamp_ms()
                                                   : timestampMs;
        const bool useMeasurementStats = state.latestMeasurement.channelValuesMgPerM3.empty()
                                         && state.latestMeasurementStats.has_value();
        const std::size_t valueCount = useMeasurementStats
                                           ? state.latestMeasurementStats->channels.size()
                                           : state.latestMeasurement.channelValuesMgPerM3.size();
        std::vector<storage::SensorMetricValue> metrics;
        metrics.reserve(valueCount);

        for (std::size_t index = 0; index < valueCount; ++index)
        {
            const double value = useMeasurementStats
                                     ? state.latestMeasurementStats->channels[index].currentMgPerM3
                                     : state.latestMeasurement.channelValuesMgPerM3[index];
            metrics.push_back({
                .name = index < channels.size() ? std::string(channels[index]) : "channel_" + std::to_string(index),
                .value = value,
                .unit = "mg/m3",
                .quality = quality,
                .timestamp_ms = metricTimestampMs,
            });
        }

        return metrics;
    }

    static bool dustrak_measurement_stale(const std::int64_t timestampMs, const sensorRuntime &runtime)
    {
        const std::int64_t measurementTimestampMs = runtime.dustTrak.last_measurement_timestamp_ms();
        return measurementTimestampMs == 0 || timestampMs - measurementTimestampMs > dustrakMeasurementStaleAfterMs;
    }

    static std::optional<storage::SensorEventInfo> dustrak_communication_event(
        const std::int64_t timestampMs,
        const sensorRuntime &runtime,
        const bool measurementStale)
    {
        const auto &health = runtime.dustTrak.health();
        if (health.consecutiveFailures >= 3)
        {
            utils::file::jsonFile::json details;
            details["last_failed_command"] = health.lastFailedCommand;
            details["last_error"] = health.lastError;
            details["consecutive_failures"] = health.consecutiveFailures;
            details["last_failure_timestamp_ms"] = health.lastFailureTimestampMs;
            details["last_measurement_timestamp_ms"] = runtime.dustTrak.last_measurement_timestamp_ms();

            return storage::SensorEventInfo{
                .timestamp_ms = timestampMs,
                .severity = "error",
                .event_type = "poll_failed",
                .message = "DustTrak communication failed repeatedly",
                .details_json = json_dump(details),
            };
        }

        if (measurementStale)
        {
            utils::file::jsonFile::json details;
            details["last_measurement_timestamp_ms"] = runtime.dustTrak.last_measurement_timestamp_ms();
            details["stale_after_ms"] = dustrakMeasurementStaleAfterMs;
            details["consecutive_failures"] = health.consecutiveFailures;
            details["last_failed_command"] = health.lastFailedCommand;
            details["last_error"] = health.lastError;

            return storage::SensorEventInfo{
                .timestamp_ms = timestampMs,
                .severity = "warning",
                .event_type = "stale_data",
                .message = "DustTrak measurement data is stale",
                .details_json = json_dump(details),
            };
        }

        return std::nullopt;
    }

    static std::optional<storage::SensorEventInfo> dustrak_fault_event(
        const std::int64_t timestampMs,
        const sensorRuntime &runtime)
    {
        const auto &faultFlags = runtime.dustTrak.state().faultMessageFlags;
        if (std::ranges::none_of(faultFlags, [](const int flag) { return flag != 0; }))
        {
            return std::nullopt;
        }

        utils::file::jsonFile::json details;
        details["fault_flags"] = faultFlags;

        return storage::SensorEventInfo{
            .timestamp_ms = timestampMs,
            .severity = "warning",
            .event_type = "fault_flags",
            .message = "DustTrak reported fault flags",
            .details_json = json_dump(details),
        };
    }

    static void publish_error_state(
        const std::int64_t timestampMs,
        const std::string &source,
        const std::string &deviceId,
        const std::string &deviceName,
        const std::string &transportType,
        const std::string &endpoint,
        const storage::SensorEventInfo &event)
    {
        storage::SensorDeviceState state;
        state.timestamp_ms = timestampMs;
        state.source = source;
        state.device_id = deviceId;
        state.device_name = deviceName;
        state.device_type = transportType;
        state.status = "error";
        state.transport_type = transportType;
        state.endpoint = endpoint;
        state.error = event;
        publish_sensor_state(state);
    }

    static module::protocols::serial::parity parse_parity(const std::string &value)
    {
        if (value == "even")
            return module::protocols::serial::parity::Even;
        if (value == "odd")
            return module::protocols::serial::parity::Odd;
        return module::protocols::serial::parity::None;
    }

    static module::protocols::serial::stopBits parse_stop_bits(int value)
    {
        return value == 2
                   ? module::protocols::serial::stopBits::Two
                   : module::protocols::serial::stopBits::One;
    }

    static char parse_modbus_parity(const std::string &value)
    {
        if (value == "even")
            return 'E';
        if (value == "odd")
            return 'O';
        return 'N';
    }

    static module::protocols::modbus::registerType parse_modbus_register_type(const std::string &value)
    {
        if (value == "input_register")
            return module::protocols::modbus::registerType::Input;
        return module::protocols::modbus::registerType::Holding;
    }

    static module::protocols::modbus::dataType parse_modbus_data_type(const std::string &value)
    {
        if (value == "uint16")
            return module::protocols::modbus::dataType::UInt16;
        if (value == "int32")
            return module::protocols::modbus::dataType::Int32;
        if (value == "uint32")
            return module::protocols::modbus::dataType::UInt32;
        if (value == "float32")
            return module::protocols::modbus::dataType::Float32;
        return module::protocols::modbus::dataType::Int16;
    }

    static module::protocols::modbus::wordOrder parse_modbus_word_order(const std::string &value)
    {
        if (value == "little")
            return module::protocols::modbus::wordOrder::Little;
        return module::protocols::modbus::wordOrder::Big;
    }

    static std::chrono::milliseconds parse_milliseconds(
        const utils::file::jsonFile::json &json,
        const char *key,
        int defaultValue)
    {
        return std::chrono::milliseconds(std::max(json.value(key, defaultValue), 1));
    }

    static std::vector<module::protocols::modbus::registerConfig> parse_modbus_registers(
        const utils::file::jsonFile::json &registersJson)
    {
        std::vector<module::protocols::modbus::registerConfig> registers;
        if (!registersJson.is_array())
        {
            return registers;
        }

        registers.reserve(registersJson.size());
        for (const auto &registerJson : registersJson)
        {
            if (!registerJson.is_object())
            {
                continue;
            }

            module::protocols::modbus::registerConfig config;
            config.name = registerJson.value("name", "");
            config.registerType_ = parse_modbus_register_type(registerJson.value("register_type", "holding_register"));
            config.address = registerJson.value("address", 0);
            config.dataType_ = parse_modbus_data_type(registerJson.value("data_type", "int16"));
            config.wordOrder_ = parse_modbus_word_order(registerJson.value("word_order", "big"));
            config.scale = registerJson.value("scale", 1.0);
            config.unit = registerJson.value("unit", "");

            if (config.name.empty())
            {
                config.name = "register_" + std::to_string(config.address);
            }

            registers.push_back(config);
        }

        return registers;
    }

    static sensorKind parse_sensor_kind(const std::string &value)
    {
        if (value == "dustrak")
            return sensorKind::dustrak;
        return sensorKind::None;
    }

    static module::drivers::dustrak::drx85xx::alarmState parse_alarm_state(const std::string &value)
    {
        using alarmState = module::drivers::dustrak::drx85xx::alarmState;

        if (value == "audible")
            return alarmState::Audible;
        if (value == "visible")
            return alarmState::Visible;
        if (value == "audible_visible")
            return alarmState::AudibleVisible;
        if (value == "relay")
            return alarmState::Relay;
        if (value == "audible_relay")
            return alarmState::AudibleRelay;
        if (value == "visible_relay")
            return alarmState::VisibleRelay;
        if (value == "audible_visible_relay")
            return alarmState::AudibleVisibleRelay;

        return alarmState::Off;
    }

    static module::drivers::dustrak::drx85xx::analogOutputState parse_analog_output_state(const std::string &value)
    {
        using analogOutputState = module::drivers::dustrak::drx85xx::analogOutputState;

        if (value == "voltage")
            return analogOutputState::Voltage;
        if (value == "current")
            return analogOutputState::Current;

        return analogOutputState::Off;
    }

    static std::optional<module::drivers::dustrak::drx85xx::channel> parse_dustrak_channel(const std::string &value)
    {
        using channel = module::drivers::dustrak::drx85xx::channel;

        if (value == "pm1")
            return channel::PM1;
        if (value == "pm25")
            return channel::PM25;
        if (value == "pm4")
            return channel::RespirablePM4;
        if (value == "pm10")
            return channel::PM10;
        if (value == "total")
            return channel::Total;

        return std::nullopt;
    }

    static constexpr std::size_t runtime_config_index(sensorDustrakRuntimeConfigs::runTimeConfig config)
    {
        return static_cast<std::size_t>(config);
    }

    static bool alarm_config_active(const module::drivers::dustrak::drx85xx::alarmConfig &config)
    {
        using alarmState = module::drivers::dustrak::drx85xx::alarmState;

        return config.alarm1State != alarmState::Off || config.alarm2State != alarmState::Off || config.stelAlarm1Enabled;
    }

    static bool nearly_equal(double left, double right)
    {
        return std::fabs(left - right) <= 0.000001;
    }

    static bool alarm_configs_equal(
        const module::drivers::dustrak::drx85xx::alarmConfig &expected,
        const module::drivers::dustrak::drx85xx::alarmConfig &actual)
    {
        return expected.alarm1State == actual.alarm1State && nearly_equal(expected.alarm1ValueMgPerM3, actual.alarm1ValueMgPerM3) && expected.stelAlarm1Enabled == actual.stelAlarm1Enabled && expected.alarm2State == actual.alarm2State && nearly_equal(expected.alarm2ValueMgPerM3, actual.alarm2ValueMgPerM3);
    }

    static bool analog_output_configs_equal(
        const module::drivers::dustrak::drx85xx::analogOutputConfig &expected,
        const module::drivers::dustrak::drx85xx::analogOutputConfig &actual)
    {
        return expected.outputState == actual.outputState && expected.outputChannel == actual.outputChannel && nearly_equal(expected.minimumRangeMgPerM3, actual.minimumRangeMgPerM3) && nearly_equal(expected.maximumRangeMgPerM3, actual.maximumRangeMgPerM3);
    }

    static bool logging_mode_configs_equal(
        const module::drivers::dustrak::drx85xx::loggingModeConfig &expected,
        const module::drivers::dustrak::drx85xx::loggingModeConfig &actual)
    {
        return expected.startTime == actual.startTime && expected.startDate == actual.startDate && expected.interval == actual.interval && expected.testLength == actual.testLength && expected.numberOfTests == actual.numberOfTests && expected.timeBetweenTests == actual.timeBetweenTests && expected.timeConstant == actual.timeConstant && expected.useStartTime == actual.useStartTime && expected.useStartDate == actual.useStartDate && expected.autoZeroInterval == actual.autoZeroInterval && expected.autoZeroEnabled == actual.autoZeroEnabled && expected.programName == actual.programName;
    }

    static bool user_calibration_configs_equal(
        const module::drivers::dustrak::drx85xx::userCalibrationConfig &expected,
        const module::drivers::dustrak::drx85xx::userCalibrationConfig &actual)
    {
        return expected.index == actual.index && nearly_equal(expected.sizeCorrectionFactor, actual.sizeCorrectionFactor) && nearly_equal(expected.photometricCalibrationFactor, actual.photometricCalibrationFactor) && expected.name == actual.name;
    }

    static void dustrakConfig(const utils::file::jsonFile::json &portJson, sensorDustrakConfigs &config, std::string_view portName)
    {
        using runTimeConfig = sensorDustrakRuntimeConfigs::runTimeConfig;

        const utils::file::jsonFile::json dustrakJson = portJson.value("dustrak", utils::file::jsonFile::json::object());
        const utils::file::jsonFile::json pollingJson = dustrakJson.value("polling", utils::file::jsonFile::json::object());
        const utils::file::jsonFile::json driverJson = dustrakJson.value("driver", utils::file::jsonFile::json::object());

        auto &driverConfig = config.driverConfig;
        driverConfig.polling.readIdentityOnInit = pollingJson.value("read_identity_on_init", true);
        driverConfig.polling.pollStatus = pollingJson.value("poll_status", true);
        driverConfig.polling.autoStartMeasurement = pollingJson.value("auto_start_measurement", false);
        driverConfig.polling.pollMeasurements = pollingJson.value("poll_measurements", true);
        driverConfig.polling.pollMeasurementStats = pollingJson.value("poll_measurement_stats", false);
        driverConfig.polling.pollFaultMessages = pollingJson.value("poll_fault_messages", false);
        driverConfig.polling.pollAlarmMessages = pollingJson.value("poll_alarm_messages", false);
        driverConfig.polling.pollLogInfo = pollingJson.value("poll_log_info", false);
        driverConfig.updateRamAfterWrite = driverJson.value("update_ram_after_write", true);

        config.runTimeConfigs.enabled.fill(false);

        constexpr std::array<std::string_view, 5> alarmKeys{{
            "pm1",
            "pm25",
            "pm4",
            "pm10",
            "total",
        }};

        const utils::file::jsonFile::json alarmsJson = dustrakJson.value("alarms", utils::file::jsonFile::json::object());
        for (std::size_t index = 0; index < alarmKeys.size(); ++index)
        {
            const utils::file::jsonFile::json alarmJson = alarmsJson.value(alarmKeys[index], utils::file::jsonFile::json::object());
            auto &alarmConfig = config.runTimeConfigs.alarmConfigs[index];

            alarmConfig.alarm1State = parse_alarm_state(alarmJson.value("alarm1_state", "off"));
            alarmConfig.alarm1ValueMgPerM3 = alarmJson.value("alarm1_mg_per_m3", 0.0);
            alarmConfig.stelAlarm1Enabled = alarmJson.value("stel_alarm1_enabled", false);
            alarmConfig.alarm2State = parse_alarm_state(alarmJson.value("alarm2_state", "off"));
            alarmConfig.alarm2ValueMgPerM3 = alarmJson.value("alarm2_mg_per_m3", 0.0);

            if (alarm_config_active(alarmConfig))
            {
                config.runTimeConfigs.enabled[runtime_config_index(runTimeConfig::alarmconfig)] = true;
            }
        }

        const utils::file::jsonFile::json analogJson = dustrakJson.value("analog_output", utils::file::jsonFile::json::object());
        auto &analogOutputConfig = config.runTimeConfigs.analogOutputConfig;

        analogOutputConfig.outputState = parse_analog_output_state(analogJson.value("state", "off"));
        analogOutputConfig.outputChannel = std::nullopt;
        if (analogJson.contains("channel") && analogJson["channel"].is_string())
        {
            analogOutputConfig.outputChannel = parse_dustrak_channel(analogJson["channel"].get<std::string>());
        }
        analogOutputConfig.minimumRangeMgPerM3 = analogJson.value("min_mg_per_m3", 0.0);
        analogOutputConfig.maximumRangeMgPerM3 = analogJson.value("max_mg_per_m3", 1.0);
        config.runTimeConfigs.enabled[runtime_config_index(runTimeConfig::analogoutputconfig)] =
            analogOutputConfig.outputState != module::drivers::dustrak::drx85xx::analogOutputState::Off;

        SPDLOG_INFO(
            "Loaded DustTrak config port={} read_identity={} poll_status={} auto_start={} poll_measurements={} poll_stats={} poll_faults={} poll_alarms={} poll_log={} update_ram={} pending_alarm={} pending_analog={} pending_logging={} pending_user_cal={} analog_min={} analog_max={}",
            portName,
            driverConfig.polling.readIdentityOnInit,
            driverConfig.polling.pollStatus,
            driverConfig.polling.autoStartMeasurement,
            driverConfig.polling.pollMeasurements,
            driverConfig.polling.pollMeasurementStats,
            driverConfig.polling.pollFaultMessages,
            driverConfig.polling.pollAlarmMessages,
            driverConfig.polling.pollLogInfo,
            driverConfig.updateRamAfterWrite,
            config.runTimeConfigs.enabled[runtime_config_index(runTimeConfig::alarmconfig)],
            config.runTimeConfigs.enabled[runtime_config_index(runTimeConfig::analogoutputconfig)],
            config.runTimeConfigs.enabled[runtime_config_index(runTimeConfig::loggingmodeconfig)],
            config.runTimeConfigs.enabled[runtime_config_index(runTimeConfig::usercalibrationconfig)],
            analogOutputConfig.minimumRangeMgPerM3,
            analogOutputConfig.maximumRangeMgPerM3);
    }

    static bool read_rs232_config(bool startup)
    {
        if (startup != true)
        {
            // const bool enabled = redis_storage.Get(rs232ConfigRedisKey).value_or("0") == "1";
            std::optional<std::string> value = redis_storage.Get(rs232ConfigRedisKey);
            if (!value || *value != "1")
            {
                return false;
            }

            if (!redis_storage.Set(rs232ConfigRedisKey, std::uint8_t{0}))
            {
                SPDLOG_ERROR("Failed to reset Redis key '{}'", rs232ConfigRedisKey);
                return false;
            }
        }

        std::optional<utils::file::jsonFile::json> json_file = utils::file::jsonFile::load(rs232ConfigFile);

        if (!json_file.has_value())
        {
            SPDLOG_WARN("File does not exists or empty {}", rs232ConfigFile);
            return false;
        }

        root_packet = *json_file;

        if (!root_packet.contains("rs232") || !root_packet["rs232"].is_object())
        {

            SPDLOG_WARN("Missing rs232 config object");
            return false;
        }

        utils::file::jsonFile::json rs232_packet = root_packet["rs232"];

        for (const rs232PortMap &portMap : rs232Ports)
        {

            sensorRuntime &runTime = sensors[portMap.sensorIndex];
            sensorConfig &config = runTime.config;
            clear_active_sensor_events("rs232", portMap.key);

            if (!rs232_packet.contains(portMap.key) || !rs232_packet[portMap.key].is_object())
            {
                config.enabled = false;
                continue;
            }
            utils::file::jsonFile::json portJson = rs232_packet[portMap.key];

            config.enabled = portJson.value("enabled", false);
            config.name = std::string(portMap.key);
            config.transportKind_ = transportKind::RS232;

            utils::file::jsonFile::json serialJson = portJson.value("serial", utils::file::jsonFile::json::object());

            config.serial.devicePath_ = std::string(portMap.devicePath);
            config.serial.baudRate_ = serialJson.value("baud_rate", 9600U);
            config.serial.charSize_ = serialJson.value("data_bits", 8U);
            config.serial.parity_ = parse_parity(serialJson.value("parity", "none"));
            config.serial.stopBit_ = parse_stop_bits(serialJson.value("stop_bits", 1));

            config.sensorKind_ = parse_sensor_kind(portJson.value("sensor", "none"));
            const module::protocols::serial::serialConfig defaultSerialConfig;
            config.serial.startupSettleDelayMs_ = serialJson.value(
                "startup_settle_delay_ms",
                defaultSerialConfig.startupSettleDelayMs_);
            config.serial.txPostWriteDelayMs_ = serialJson.value(
                "tx_post_write_delay_ms",
                defaultSerialConfig.txPostWriteDelayMs_);
            config.serial.rxTimeoutMs_ = serialJson.value(
                "rx_timeout_ms",
                config.sensorKind_ == sensorKind::dustrak ? 1000U : 300U);
            config.deviceType = sensor_kind_text(config.sensorKind_);
            config.deviceName = portJson.value(
                "name",
                default_sensor_device_name(config.sensorKind_, config.name));

            switch (config.sensorKind_)
            {
            case sensorKind::dustrak:
            {

                dustrakConfig(portJson, config.sensorDustrakConfig, config.name);

                break;
            }

            case sensorKind::None:
            default:
            {
                SPDLOG_WARN("No sensor kind selected");
                break;
            }
            }

            SPDLOG_INFO(
                "Loaded RS232 config port={} enabled={} path={} sensor={} baud={} data_bits={} stop_bits={} rx_timeout_ms={} tx_post_write_delay_ms={} startup_settle_delay_ms={}",
                portMap.key,
                config.enabled,
                config.serial.devicePath_,
                portJson.value("sensor", "none"),
                config.serial.baudRate_,
                config.serial.charSize_,
                serialJson.value("stop_bits", 1),
                config.serial.rxTimeoutMs_,
                config.serial.txPostWriteDelayMs_,
                config.serial.startupSettleDelayMs_);
        }

        return true;
    }

    static bool read_rs485_config(bool startup)
    {
        if (startup != true)
        {
            std::optional<std::string> value = redis_storage.Get(rs485ConfigRedisKey);
            if (!value || *value != "1")
            {
                return false;
            }

            if (!redis_storage.Set(rs485ConfigRedisKey, std::uint8_t{0}))
            {
                SPDLOG_ERROR("Failed to reset Redis key '{}'", rs485ConfigRedisKey);
                return false;
            }
        }

        std::optional<utils::file::jsonFile::json> json_file = utils::file::jsonFile::load(rs485ConfigFile);
        if (!json_file.has_value())
        {
            SPDLOG_WARN("File does not exists or empty {}", rs485ConfigFile);
            return false;
        }

        const utils::file::jsonFile::json &packet = *json_file;
        if (!packet.contains("rs485") || !packet["rs485"].is_object())
        {
            SPDLOG_WARN("Missing rs485 config object");
            return false;
        }

        const utils::file::jsonFile::json rs485_packet = packet["rs485"];
        for (const rs485PortMap &portMap : rs485Ports)
        {
            modbusRtuRuntime &runtime = modbusRtuRuntimes[portMap.runtimeIndex];
            clear_active_sensor_events("rs485", portMap.key);

            if (!rs485_packet.contains(portMap.key) || !rs485_packet[portMap.key].is_object())
            {
                runtime.enabled = false;
                continue;
            }

            const utils::file::jsonFile::json portJson = rs485_packet[portMap.key];
            const utils::file::jsonFile::json serialJson = portJson.value("serial", utils::file::jsonFile::json::object());
            const utils::file::jsonFile::json modbusJson = portJson.value("modbus_rtu", utils::file::jsonFile::json::object());

            runtime.enabled = portJson.value("enabled", false);
            runtime.initialized = false;
            runtime.metricCache.clear();
            runtime.health = {};

            auto &config = runtime.config;
            config.name = std::string(portMap.key);
            config.devicePath = std::string(portMap.devicePath);
            config.baudRate = serialJson.value("baud_rate", 9600);
            config.dataBits = serialJson.value("data_bits", 8);
            config.parity = parse_modbus_parity(serialJson.value("parity", "none"));
            config.stopBits = serialJson.value("stop_bits", 1);
            config.slaveAddress = modbusJson.value("slave_address", 1);
            config.pollInterval = parse_milliseconds(modbusJson, "poll_interval_ms", 1000);
            config.responseTimeout = parse_milliseconds(modbusJson, "response_timeout_ms", 1000);
            config.registers = parse_modbus_registers(modbusJson.value("registers", utils::file::jsonFile::json::array()));

            SPDLOG_INFO(
                "Loaded RS485 Modbus RTU config port={} enabled={} path={} slave={} baud={} data_bits={} stop_bits={} registers={}",
                portMap.key,
                runtime.enabled,
                config.devicePath,
                config.slaveAddress,
                config.baudRate,
                config.dataBits,
                config.stopBits,
                config.registers.size());
        }

        return true;
    }

    static bool read_modbus_tcp_config(bool startup)
    {
        if (startup != true)
        {
            std::optional<std::string> value = redis_storage.Get(modbusTcpConfigRedisKey);
            if (!value || *value != "1")
            {
                return false;
            }

            if (!redis_storage.Set(modbusTcpConfigRedisKey, std::uint8_t{0}))
            {
                SPDLOG_ERROR("Failed to reset Redis key '{}'", modbusTcpConfigRedisKey);
                return false;
            }
        }

        std::optional<utils::file::jsonFile::json> json_file = utils::file::jsonFile::load(modbusTcpConfigFile);
        if (!json_file.has_value())
        {
            SPDLOG_WARN("File does not exists or empty {}", modbusTcpConfigFile);
            return false;
        }

        const utils::file::jsonFile::json &packet = *json_file;
        if (!packet.contains("connections") || !packet["connections"].is_array())
        {
            SPDLOG_WARN("Missing Modbus TCP connections array");
            return false;
        }

        for (std::size_t index = 0; index < modbusTcpRuntimes.size(); ++index)
        {
            modbusTcpRuntime &runtime = modbusTcpRuntimes[index];
            clear_active_sensor_events("modbus_tcp", modbus_tcp_device_id(runtime, index));
            runtime.enabled = false;
            runtime.initialized = false;
        }

        const int configuredMaxConnections = packet.value("max_connections", static_cast<int>(modbusTcpConnectionsPerInterface));
        const std::size_t perInterfaceLimit = static_cast<std::size_t>(
            std::clamp(configuredMaxConnections, 1, static_cast<int>(modbusTcpConnectionsPerInterface)));

        std::unordered_map<std::string, std::size_t> enabledByInterface;
        std::size_t slot = 0;

        for (const auto &connectionJson : packet["connections"])
        {
            if (!connectionJson.is_object())
            {
                continue;
            }

            if (slot >= modbusTcpRuntimes.size())
            {
                SPDLOG_WARN("Ignoring Modbus TCP connection beyond runtime capacity {}", modbusTcpRuntimes.size());
                break;
            }

            const bool enabled = connectionJson.value("enabled", false);
            const std::string interface = connectionJson.value("interface", "");
            if (enabled && enabledByInterface[interface] >= perInterfaceLimit)
            {
                SPDLOG_WARN(
                    "Ignoring enabled Modbus TCP connection id={} interface={} because interface limit {} is reached",
                    connectionJson.value("id", ""),
                    interface,
                    perInterfaceLimit);
                continue;
            }

            modbusTcpRuntime &runtime = modbusTcpRuntimes[slot++];
            runtime.enabled = enabled;
            runtime.initialized = false;
            runtime.metricCache.clear();
            runtime.health = {};

            auto &config = runtime.config;
            config.id = connectionJson.value("id", "");
            config.name = connectionJson.value("name", config.id);
            config.interface = interface;
            config.ip = connectionJson.value("ip", "");
            config.port = connectionJson.value("port", 502);
            config.unitId = connectionJson.value("unit_id", 1);
            config.pollInterval = parse_milliseconds(connectionJson, "poll_interval_ms", 1000);
            config.responseTimeout = parse_milliseconds(connectionJson, "response_timeout_ms", 1000);
            config.registers = parse_modbus_registers(connectionJson.value("registers", utils::file::jsonFile::json::array()));

            if (runtime.enabled)
            {
                ++enabledByInterface[config.interface];
            }

            SPDLOG_INFO(
                "Loaded Modbus TCP config slot={} id={} enabled={} interface={} endpoint={}:{} unit={} registers={}",
                slot - 1,
                config.id,
                runtime.enabled,
                config.interface,
                config.ip,
                config.port,
                config.unitId,
                config.registers.size());
        }

        return true;
    }

    static void rs232_init()
    {
        for (sensorRuntime &runtime : sensors)
        {
            sensorConfig &config = runtime.config;

            if (!config.enabled)
            {
                runtime.serialPort_.close();
                SPDLOG_INFO("Skipping disabled sensor slot {}", config.name);
                continue;
            }

            bool transportReady = false;
            switch (config.transportKind_)
            {
            case transportKind::RS232:
            {
                transportReady = runtime.serialPort_.open(config.serial);
                if (!transportReady)
                {
                    SPDLOG_ERROR("Failed to initialize RS232 transport slot={} path={}", config.name, config.serial.devicePath_);
                    const std::int64_t timestampMs = unix_timestamp_ms();
                    storage::SensorEventInfo event{
                        .timestamp_ms = timestampMs,
                        .severity = "error",
                        .event_type = "serial_open_failed",
                        .message = "Failed to initialize RS232 transport",
                        .details_json = "{}",
                    };
                    store_sensor_event_once(
                        timestampMs,
                        "rs232",
                        config.name,
                        config.deviceName,
                        config.deviceType,
                        event);
                    publish_error_state(timestampMs, "rs232", config.name, config.deviceName, "serial", config.serial.devicePath_, event);
                }
                break;
            }

            case transportKind::None:
            {
                SPDLOG_WARN("No transport selected for sensor slot {}", config.name);
                break;
            }

            case transportKind::RS485:
            case transportKind::MOD_TCP:
            case transportKind::TOTAL:
            default:
            {
                SPDLOG_WARN("Unsupported transport selected for sensor slot {}", config.name);
                break;
            }
            }

            if (!transportReady)
            {
                continue;
            }

            switch (config.sensorKind_)
            {
            case sensorKind::dustrak:
            {
                runtime.dustTrak.init(runtime.serialPort_);
                runtime.dustTrak.configure(config.sensorDustrakConfig.driverConfig);
                SPDLOG_INFO("Initialized DustTrak sensor slot={} path={}", config.name, config.serial.devicePath_);
                break;
            }

            case sensorKind::None:
            {
                SPDLOG_WARN("No sensor selected for enabled slot {}", config.name);
                break;
            }

            case sensorKind::TOTAL:
            default:
            {
                SPDLOG_WARN("Unsupported sensor selected for slot {}", config.name);
                break;
            }
            }
        }
    }

    static void modbus_rtu_init()
    {
        const auto now = std::chrono::steady_clock::now();

        for (modbusRtuRuntime &runtime : modbusRtuRuntimes)
        {
            runtime.client_.disconnect();
            runtime.samples.clear();
            runtime.initialized = false;

            if (!runtime.enabled)
            {
                SPDLOG_INFO("Skipping disabled Modbus RTU slot {}", runtime.config.name);
                continue;
            }

            runtime.initialized = runtime.client_.connect_rtu(runtime.config);
            runtime.lastPoll = now - runtime.config.pollInterval;

            if (!runtime.initialized)
            {
                SPDLOG_ERROR("Failed to initialize Modbus RTU slot={} path={}", runtime.config.name, runtime.config.devicePath);
                const std::int64_t timestampMs = unix_timestamp_ms();
                ensure_modbus_metric_cache(runtime.metricCache, runtime.config.registers);
                record_runtime_failure(runtime.health, timestampMs, "connect_failed", "Failed to initialize Modbus RTU connection");
                storage::SensorEventInfo event = make_runtime_error_info(
                    timestampMs,
                    runtime.health,
                    "Failed to initialize Modbus RTU connection",
                    "error");
                store_sensor_event_once(
                    timestampMs,
                    "rs485",
                    runtime.config.name,
                    runtime.config.name,
                    "modbus_rtu",
                    event);

                storage::SensorDeviceState state;
                state.timestamp_ms = timestampMs;
                state.source = "rs485";
                state.device_id = runtime.config.name;
                state.device_name = runtime.config.name;
                state.device_type = "modbus_rtu";
                state.status = "error";
                state.transport_type = "modbus_rtu";
                state.endpoint = runtime.config.devicePath;
                state.slave_address = runtime.config.slaveAddress;
                state.metrics = build_modbus_metrics(timestampMs, runtime.metricCache, {}, modbus_stale_after_ms(runtime.config.pollInterval));
                state.error = event;
                publish_sensor_state(state);
            }
        }
    }

    static void modbus_tcp_init()
    {
        const auto now = std::chrono::steady_clock::now();

        for (std::size_t index = 0; index < modbusTcpRuntimes.size(); ++index)
        {
            modbusTcpRuntime &runtime = modbusTcpRuntimes[index];
            runtime.client_.disconnect();
            runtime.samples.clear();
            runtime.initialized = false;

            if (!runtime.enabled)
            {
                continue;
            }

            runtime.initialized = runtime.client_.connect_tcp(runtime.config);
            runtime.lastPoll = now - runtime.config.pollInterval;

            if (!runtime.initialized)
            {
                SPDLOG_ERROR(
                    "Failed to initialize Modbus TCP slot={} endpoint={}:{}",
                    runtime.config.id,
                    runtime.config.ip,
                    runtime.config.port);
                const std::int64_t timestampMs = unix_timestamp_ms();
                const std::string deviceId = modbus_tcp_device_id(runtime, index);
                const std::string endpoint = runtime.config.ip + ":" + std::to_string(runtime.config.port);
                ensure_modbus_metric_cache(runtime.metricCache, runtime.config.registers);
                record_runtime_failure(runtime.health, timestampMs, "connect_failed", "Failed to initialize Modbus TCP connection");
                storage::SensorEventInfo event = make_runtime_error_info(
                    timestampMs,
                    runtime.health,
                    "Failed to initialize Modbus TCP connection",
                    "error");
                store_sensor_event_once(
                    timestampMs,
                    "modbus_tcp",
                    deviceId,
                    runtime.config.name,
                    "modbus_tcp",
                    event);

                storage::SensorDeviceState state;
                state.timestamp_ms = timestampMs;
                state.source = "modbus_tcp";
                state.device_id = deviceId;
                state.device_name = runtime.config.name;
                state.device_type = "modbus_tcp";
                state.status = "error";
                state.transport_type = "modbus_tcp";
                state.endpoint = endpoint;
                state.network_interface = runtime.config.interface;
                state.port = runtime.config.port;
                state.unit_id = runtime.config.unitId;
                state.metrics = build_modbus_metrics(timestampMs, runtime.metricCache, {}, modbus_stale_after_ms(runtime.config.pollInterval));
                state.error = event;
                publish_sensor_state(state);
            }
        }
    }

    static bool send_dustrak_alarm_configs(sensorRuntime &runtime)
    {
        constexpr std::array<module::drivers::dustrak::drx85xx::channel, 5> alarmChannels{{
            module::drivers::dustrak::drx85xx::channel::PM1,
            module::drivers::dustrak::drx85xx::channel::PM25,
            module::drivers::dustrak::drx85xx::channel::RespirablePM4,
            module::drivers::dustrak::drx85xx::channel::PM10,
            module::drivers::dustrak::drx85xx::channel::Total,
        }};

        for (std::size_t index = 0; index < alarmChannels.size(); ++index)
        {
            if (!alarm_config_active(runtime.config.sensorDustrakConfig.runTimeConfigs.alarmConfigs[index]))
            {
                continue;
            }

            if (!runtime.dustTrak.write_alarm_settings(
                    alarmChannels[index],
                    runtime.config.sensorDustrakConfig.runTimeConfigs.alarmConfigs[index]))
            {
                SPDLOG_WARN("DustTrak alarm runtime config send failed slot={} channel_index={}", runtime.config.name, index);
                return false;
            }

            module::drivers::dustrak::drx85xx::alarmConfig readBackConfig;
            if (!runtime.dustTrak.read_alarm_settings(alarmChannels[index], readBackConfig))
            {
                SPDLOG_WARN("DustTrak alarm runtime config read-back failed slot={} channel_index={}", runtime.config.name, index);
                return false;
            }

            if (!alarm_configs_equal(runtime.config.sensorDustrakConfig.runTimeConfigs.alarmConfigs[index], readBackConfig))
            {
                SPDLOG_WARN("DustTrak alarm runtime config verification mismatch slot={} channel_index={}", runtime.config.name, index);
                return false;
            }
        }

        return true;
    }

    static void handle_dustrak_runtime_config(sensorRuntime &runtime)
    {
        using runTimeConfig = sensorDustrakRuntimeConfigs::runTimeConfig;

        auto &runTimeConfigs = runtime.config.sensorDustrakConfig.runTimeConfigs;

        if (runTimeConfigs.enabled[runtime_config_index(runTimeConfig::alarmconfig)])
        {
            if (send_dustrak_alarm_configs(runtime))
            {
                runTimeConfigs.enabled[runtime_config_index(runTimeConfig::alarmconfig)] = false;
                SPDLOG_INFO("DustTrak alarm runtime config applied and verified slot={}", runtime.config.name);
            }
        }

        if (runTimeConfigs.enabled[runtime_config_index(runTimeConfig::analogoutputconfig)])
        {
            if (runtime.dustTrak.write_analog_output_settings(runTimeConfigs.analogOutputConfig))
            {
                module::drivers::dustrak::drx85xx::analogOutputConfig readBackConfig;
                if (runtime.dustTrak.read_analog_output_settings(readBackConfig) && analog_output_configs_equal(runTimeConfigs.analogOutputConfig, readBackConfig))
                {
                    runTimeConfigs.enabled[runtime_config_index(runTimeConfig::analogoutputconfig)] = false;
                    SPDLOG_INFO("DustTrak analog output runtime config applied and verified slot={}", runtime.config.name);
                }
                else
                {
                    SPDLOG_WARN("DustTrak analog output runtime config verification failed slot={}", runtime.config.name);
                }
            }
            else
            {
                SPDLOG_WARN("DustTrak analog output runtime config send failed slot={}", runtime.config.name);
            }
        }

        if (runTimeConfigs.enabled[runtime_config_index(runTimeConfig::loggingmodeconfig)])
        {
            constexpr auto loggingProgram = module::drivers::dustrak::drx85xx::loggingProgram::Program1;

            if (runtime.dustTrak.write_logging_mode(
                    loggingProgram,
                    runTimeConfigs.loggingModeConfig))
            {
                module::drivers::dustrak::drx85xx::loggingModeConfig readBackConfig;
                if (runtime.dustTrak.read_logging_mode(loggingProgram, readBackConfig) && logging_mode_configs_equal(runTimeConfigs.loggingModeConfig, readBackConfig))
                {
                    runTimeConfigs.enabled[runtime_config_index(runTimeConfig::loggingmodeconfig)] = false;
                    SPDLOG_INFO("DustTrak logging mode runtime config applied and verified slot={}", runtime.config.name);
                }
                else
                {
                    SPDLOG_WARN("DustTrak logging mode runtime config verification failed slot={}", runtime.config.name);
                }
            }
            else
            {
                SPDLOG_WARN("DustTrak logging mode runtime config send failed slot={}", runtime.config.name);
            }
        }

        if (runTimeConfigs.enabled[runtime_config_index(runTimeConfig::usercalibrationconfig)])
        {
            if (runtime.dustTrak.write_user_calibration(
                    runTimeConfigs.userCalibrationConfig.index,
                    runTimeConfigs.userCalibrationConfig))
            {
                module::drivers::dustrak::drx85xx::userCalibrationConfig readBackConfig;
                if (runtime.dustTrak.read_user_calibration(runTimeConfigs.userCalibrationConfig.index, readBackConfig) && user_calibration_configs_equal(runTimeConfigs.userCalibrationConfig, readBackConfig))
                {
                    runTimeConfigs.enabled[runtime_config_index(runTimeConfig::usercalibrationconfig)] = false;
                    SPDLOG_INFO("DustTrak user calibration runtime config applied and verified slot={}", runtime.config.name);
                }
                else
                {
                    SPDLOG_WARN("DustTrak user calibration runtime config verification failed slot={}", runtime.config.name);
                }
            }
            else
            {
                SPDLOG_WARN("DustTrak user calibration runtime config send failed slot={}", runtime.config.name);
            }
        }
    }

    static void run_sensor_loop()
    {
        std::string sensorPacket;

        for (sensorRuntime &runtime : sensors)
        {
            if (!runtime.config.enabled)
            {
                continue;
            }

            switch (runtime.config.sensorKind_)
            {
            case sensorKind::dustrak:
            {
                runtime.dustTrak.loop(sensorPacket);
                const std::int64_t timestampMs = unix_timestamp_ms();
                const bool measurementStale = dustrak_measurement_stale(timestampMs, runtime);
                const std::string metricQuality = measurementStale ? "stale" : "good";
                if (!measurementStale)
                {
                    const auto sampleRecords = build_dustrak_sample_records(timestampMs, runtime);
                    store_sensor_samples(sampleRecords);
                }

                auto faultEvent = dustrak_fault_event(timestampMs, runtime);
                auto communicationEvent = dustrak_communication_event(timestampMs, runtime, measurementStale);

                storage::SensorDeviceState state;
                state.timestamp_ms = timestampMs;
                state.source = "rs232";
                state.device_id = runtime.config.name;
                state.device_name = runtime.config.deviceName;
                state.device_type = runtime.config.deviceType;
                if (communicationEvent.has_value())
                {
                    state.status = communicationEvent->severity == "error" ? "error" : "warning";
                }
                else
                {
                    state.status = faultEvent.has_value() ? "warning" : "ok";
                }
                state.transport_type = "serial";
                state.endpoint = runtime.config.serial.devicePath_;
                state.metrics = build_dustrak_metrics(timestampMs, runtime, metricQuality);
                state.error = communicationEvent.has_value() ? communicationEvent : faultEvent;
                state.raw_json = sensorPacket;
                if (state.error.has_value())
                {
                    store_sensor_event_once(
                        timestampMs,
                        state.source,
                        state.device_id,
                        state.device_name,
                        state.device_type,
                        *state.error);
                }
                else
                {
                    clear_active_sensor_events(state.source, state.device_id);
                }
                publish_sensor_state(state);
                handle_dustrak_runtime_config(runtime);
                break;
            }

            case sensorKind::None:
            {
                break;
            }

            case sensorKind::TOTAL:
            default:
            {
                SPDLOG_WARN("Unsupported sensor loop slot={}", runtime.config.name);
                break;
            }
            }
        }
    }

    static bool poll_due(
        const std::chrono::steady_clock::time_point lastPoll,
        const std::chrono::milliseconds pollInterval,
        const std::chrono::steady_clock::time_point now)
    {
        return lastPoll.time_since_epoch().count() == 0 || now - lastPoll >= pollInterval;
    }

    static void run_modbus_rtu_loop(const std::chrono::steady_clock::time_point now)
    {
        for (modbusRtuRuntime &runtime : modbusRtuRuntimes)
        {
            if (!runtime.enabled || !poll_due(runtime.lastPoll, runtime.config.pollInterval, now))
            {
                continue;
            }

            runtime.lastPoll = now;

            if (!runtime.initialized)
            {
                runtime.initialized = runtime.client_.connect_rtu(runtime.config);
                if (!runtime.initialized)
                {
                    const std::int64_t timestampMs = unix_timestamp_ms();
                    ensure_modbus_metric_cache(runtime.metricCache, runtime.config.registers);
                    record_runtime_failure(runtime.health, timestampMs, "connect_failed", "Failed to reconnect Modbus RTU connection");
                    storage::SensorEventInfo event = make_runtime_error_info(
                        timestampMs,
                        runtime.health,
                        "Failed to reconnect Modbus RTU connection",
                        "error");
                    store_sensor_event_once(
                        timestampMs,
                        "rs485",
                        runtime.config.name,
                        runtime.config.name,
                        "modbus_rtu",
                        event);

                    storage::SensorDeviceState state;
                    state.timestamp_ms = timestampMs;
                    state.source = "rs485";
                    state.device_id = runtime.config.name;
                    state.device_name = runtime.config.name;
                    state.device_type = "modbus_rtu";
                    state.status = "error";
                    state.transport_type = "modbus_rtu";
                    state.endpoint = runtime.config.devicePath;
                    state.slave_address = runtime.config.slaveAddress;
                    state.metrics = build_modbus_metrics(timestampMs, runtime.metricCache, {}, modbus_stale_after_ms(runtime.config.pollInterval));
                    state.error = event;
                    publish_sensor_state(state);
                }
                continue;
            }

            const std::int64_t timestampMs = unix_timestamp_ms();
            const bool pollOk = runtime.client_.poll(runtime.samples);
            const bool connectionLost = runtime.client_.connection_lost();
            const auto &errors = runtime.client_.last_errors();
            update_modbus_metric_cache(runtime.metricCache, runtime.config.registers, runtime.samples, timestampMs);
            if (pollOk)
            {
                record_runtime_success(runtime.health, timestampMs);
            }
            else
            {
                const std::string eventType = connectionLost ? "connect_lost" : errors.empty() ? "poll_failed" : "register_read_failed";
                const std::string errorMessage = errors.empty() ? "Modbus RTU poll had read failures" : errors.front().message;
                const std::string failedMetric = errors.empty() ? std::string{} : errors.front().name;
                record_runtime_failure(runtime.health, timestampMs, eventType, errorMessage, failedMetric);
                if (!runtime.samples.empty())
                {
                    runtime.health.lastSuccessTimestampMs = timestampMs;
                }
            }
            if (!pollOk)
            {
                SPDLOG_WARN("Modbus RTU poll had read failures slot={}", runtime.config.name);
            }

            for (const auto &sample : runtime.samples)
            {
                SPDLOG_INFO("Modbus RTU sample slot={} name={} value={} unit={}", runtime.config.name, sample.name, sample.value, sample.unit);
            }

            store_sensor_samples(build_modbus_sample_records(timestampMs, "rs485", runtime.config.name, runtime.config.name, "modbus_rtu", runtime.samples));

            storage::SensorDeviceState state;
            state.timestamp_ms = timestampMs;
            state.source = "rs485";
            state.device_id = runtime.config.name;
            state.device_name = runtime.config.name;
            state.device_type = "modbus_rtu";
            state.status = pollOk ? "ok" : "error";
            state.transport_type = "modbus_rtu";
            state.endpoint = runtime.config.devicePath;
            state.slave_address = runtime.config.slaveAddress;
            const std::int64_t staleAfterMs = modbus_stale_after_ms(runtime.config.pollInterval);
            const bool hasStaleMetrics = modbus_cache_has_stale_metrics(runtime.metricCache, timestampMs, staleAfterMs);
            state.metrics = build_modbus_metrics(timestampMs, runtime.metricCache, errors, staleAfterMs);
            if (!pollOk)
            {
                const std::string severity = runtime.health.consecutiveFailures >= runtimeErrorThreshold || connectionLost ? "error" : "warning";
                state.status = severity == "error" ? "error" : "warning";
                state.error = make_runtime_error_info(
                    timestampMs,
                    runtime.health,
                    "Modbus RTU poll had read failures",
                    severity);
            }
            else if (hasStaleMetrics)
            {
                state.status = "warning";
                runtime.health.lastEventType = "stale_data";
                state.error = make_runtime_error_info(
                    timestampMs,
                    runtime.health,
                    "Modbus RTU metric data is stale",
                    "warning");
            }
            if (state.error.has_value())
            {
                store_sensor_event_once(
                    timestampMs,
                    state.source,
                    state.device_id,
                    state.device_name,
                    state.device_type,
                    *state.error);
            }
            else
            {
                clear_active_sensor_events(state.source, state.device_id);
            }
            publish_sensor_state(state);

            if (connectionLost)
            {
                SPDLOG_WARN("Modbus RTU connection lost slot={} path={}; reconnecting on next poll", runtime.config.name, runtime.config.devicePath);
                runtime.client_.disconnect();
                runtime.initialized = false;
            }
        }
    }

    static void run_modbus_tcp_loop(const std::chrono::steady_clock::time_point now)
    {
        for (std::size_t index = 0; index < modbusTcpRuntimes.size(); ++index)
        {
            modbusTcpRuntime &runtime = modbusTcpRuntimes[index];
            if (!runtime.enabled || !poll_due(runtime.lastPoll, runtime.config.pollInterval, now))
            {
                continue;
            }

            runtime.lastPoll = now;

            if (!runtime.initialized)
            {
                runtime.initialized = runtime.client_.connect_tcp(runtime.config);
                if (!runtime.initialized)
                {
                    const std::int64_t timestampMs = unix_timestamp_ms();
                    const std::string deviceId = modbus_tcp_device_id(runtime, index);
                    const std::string endpoint = runtime.config.ip + ":" + std::to_string(runtime.config.port);
                    ensure_modbus_metric_cache(runtime.metricCache, runtime.config.registers);
                    record_runtime_failure(runtime.health, timestampMs, "connect_failed", "Failed to reconnect Modbus TCP connection");
                    storage::SensorEventInfo event = make_runtime_error_info(
                        timestampMs,
                        runtime.health,
                        "Failed to reconnect Modbus TCP connection",
                        "error");
                    store_sensor_event_once(
                        timestampMs,
                        "modbus_tcp",
                        deviceId,
                        runtime.config.name,
                        "modbus_tcp",
                        event);

                    storage::SensorDeviceState state;
                    state.timestamp_ms = timestampMs;
                    state.source = "modbus_tcp";
                    state.device_id = deviceId;
                    state.device_name = runtime.config.name;
                    state.device_type = "modbus_tcp";
                    state.status = "error";
                    state.transport_type = "modbus_tcp";
                    state.endpoint = endpoint;
                    state.network_interface = runtime.config.interface;
                    state.port = runtime.config.port;
                    state.unit_id = runtime.config.unitId;
                    state.metrics = build_modbus_metrics(timestampMs, runtime.metricCache, {}, modbus_stale_after_ms(runtime.config.pollInterval));
                    state.error = event;
                    publish_sensor_state(state);
                }
                continue;
            }

            const std::int64_t timestampMs = unix_timestamp_ms();
            const std::string deviceId = modbus_tcp_device_id(runtime, index);
            const bool pollOk = runtime.client_.poll(runtime.samples);
            const bool connectionLost = runtime.client_.connection_lost();
            const auto &errors = runtime.client_.last_errors();
            update_modbus_metric_cache(runtime.metricCache, runtime.config.registers, runtime.samples, timestampMs);
            if (pollOk)
            {
                record_runtime_success(runtime.health, timestampMs);
            }
            else
            {
                const std::string eventType = connectionLost ? "connect_lost" : errors.empty() ? "poll_failed" : "register_read_failed";
                const std::string errorMessage = errors.empty() ? "Modbus TCP poll had read failures" : errors.front().message;
                const std::string failedMetric = errors.empty() ? std::string{} : errors.front().name;
                record_runtime_failure(runtime.health, timestampMs, eventType, errorMessage, failedMetric);
                if (!runtime.samples.empty())
                {
                    runtime.health.lastSuccessTimestampMs = timestampMs;
                }
            }
            if (!pollOk)
            {
                SPDLOG_WARN("Modbus TCP poll had read failures slot={} endpoint={}:{}", runtime.config.id, runtime.config.ip, runtime.config.port);
            }

            for (const auto &sample : runtime.samples)
            {
                SPDLOG_INFO("Modbus TCP sample slot={} name={} value={} unit={}", runtime.config.id, sample.name, sample.value, sample.unit);
            }

            store_sensor_samples(build_modbus_sample_records(timestampMs, "modbus_tcp", deviceId, runtime.config.name, "modbus_tcp", runtime.samples));

            storage::SensorDeviceState state;
            state.timestamp_ms = timestampMs;
            state.source = "modbus_tcp";
            state.device_id = deviceId;
            state.device_name = runtime.config.name;
            state.device_type = "modbus_tcp";
            state.status = pollOk ? "ok" : "error";
            state.transport_type = "modbus_tcp";
            state.endpoint = runtime.config.ip;
            state.network_interface = runtime.config.interface;
            state.port = runtime.config.port;
            state.unit_id = runtime.config.unitId;
            const std::int64_t staleAfterMs = modbus_stale_after_ms(runtime.config.pollInterval);
            const bool hasStaleMetrics = modbus_cache_has_stale_metrics(runtime.metricCache, timestampMs, staleAfterMs);
            state.metrics = build_modbus_metrics(timestampMs, runtime.metricCache, errors, staleAfterMs);
            if (!pollOk)
            {
                const std::string severity = runtime.health.consecutiveFailures >= runtimeErrorThreshold || connectionLost ? "error" : "warning";
                state.status = severity == "error" ? "error" : "warning";
                state.error = make_runtime_error_info(
                    timestampMs,
                    runtime.health,
                    "Modbus TCP poll had read failures",
                    severity);
            }
            else if (hasStaleMetrics)
            {
                state.status = "warning";
                runtime.health.lastEventType = "stale_data";
                state.error = make_runtime_error_info(
                    timestampMs,
                    runtime.health,
                    "Modbus TCP metric data is stale",
                    "warning");
            }
            if (state.error.has_value())
            {
                store_sensor_event_once(
                    timestampMs,
                    state.source,
                    state.device_id,
                    state.device_name,
                    state.device_type,
                    *state.error);
            }
            else
            {
                clear_active_sensor_events(state.source, state.device_id);
            }
            publish_sensor_state(state);

            if (connectionLost)
            {
                SPDLOG_WARN(
                    "Modbus TCP connection lost slot={} endpoint={}:{}; reconnecting on next poll",
                    runtime.config.id,
                    runtime.config.ip,
                    runtime.config.port);
                runtime.client_.disconnect();
                runtime.initialized = false;
            }
        }
    }

    static void run_modbus_loop()
    {
        const auto now = std::chrono::steady_clock::now();
        run_modbus_rtu_loop(now);
        run_modbus_tcp_loop(now);
    }

    void interfaces_startup()
    {
        if (read_rs232_config(true))
        {
            rs232_init();
        }

        if (read_rs485_config(true))
        {
            modbus_rtu_init();
        }

        if (read_modbus_tcp_config(true))
        {
            modbus_tcp_init();
        }

        publish_device_index();
    }

    void interfaces_loop()
    {
        if (read_rs232_config(false))
        {
            rs232_init();
        }

        if (read_rs485_config(false))
        {
            modbus_rtu_init();
        }

        if (read_modbus_tcp_config(false))
        {
            modbus_tcp_init();
        }

        run_sensor_loop();
        run_modbus_loop();
        publish_device_index();
    }

}
