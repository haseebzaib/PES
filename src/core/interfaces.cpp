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

    static std::vector<storage::SensorMetricValue> build_modbus_metrics(
        const std::int64_t timestampMs,
        const std::vector<module::protocols::modbus::sample> &samples)
    {
        std::vector<storage::SensorMetricValue> metrics;
        metrics.reserve(samples.size());

        for (const auto &sample : samples)
        {
            metrics.push_back({
                .name = sample.name,
                .value = sample.value,
                .unit = sample.unit,
                .quality = "good",
                .timestamp_ms = timestampMs,
            });
        }

        return metrics;
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
        records.reserve(state.latestMeasurement.channelValuesMgPerM3.size());

        for (std::size_t index = 0; index < state.latestMeasurement.channelValuesMgPerM3.size(); ++index)
        {
            const std::string metric = index < channels.size()
                                           ? std::string(channels[index])
                                           : "channel_" + std::to_string(index);
            utils::file::jsonFile::json details;
            details["elapsed_seconds"] = state.latestMeasurement.elapsedSeconds;
            details["channel_index"] = index;
            details["model"] = state.identity.modelText;
            details["serial_number"] = state.identity.serialNumber;

            records.push_back({
                .timestamp_ms = timestampMs,
                .source = "rs232",
                .device_id = runtime.config.name,
                .device_name = runtime.config.deviceName,
                .device_type = runtime.config.deviceType,
                .metric = metric,
                .value = state.latestMeasurement.channelValuesMgPerM3[index],
                .unit = "mg/m3",
                .quality = "good",
                .details_json = json_dump(details),
            });
        }

        return records;
    }

    static std::vector<storage::SensorMetricValue> build_dustrak_metrics(
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
        std::vector<storage::SensorMetricValue> metrics;
        metrics.reserve(state.latestMeasurement.channelValuesMgPerM3.size());

        for (std::size_t index = 0; index < state.latestMeasurement.channelValuesMgPerM3.size(); ++index)
        {
            metrics.push_back({
                .name = index < channels.size() ? std::string(channels[index]) : "channel_" + std::to_string(index),
                .value = state.latestMeasurement.channelValuesMgPerM3[index],
                .unit = "mg/m3",
                .quality = "good",
                .timestamp_ms = timestampMs,
            });
        }

        return metrics;
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
                "Loaded RS232 config port={} enabled={} path={} sensor={} baud={} data_bits={} stop_bits={}",
                portMap.key,
                config.enabled,
                config.serial.devicePath_,
                portJson.value("sensor", "none"),
                config.serial.baudRate_,
                config.serial.charSize_,
                serialJson.value("stop_bits", 1));
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

        for (modbusTcpRuntime &runtime : modbusTcpRuntimes)
        {
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
                    };
                    store_sensor_event(make_sensor_event(
                        timestampMs,
                        "rs232",
                        config.name,
                        config.deviceName,
                        config.deviceType,
                        event.severity,
                        event.event_type,
                        event.message,
                        "{}"));
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
                storage::SensorEventInfo event{
                    .timestamp_ms = timestampMs,
                    .severity = "error",
                    .event_type = "connect_failed",
                    .message = "Failed to initialize Modbus RTU connection",
                };
                store_sensor_event(make_sensor_event(
                    timestampMs,
                    "rs485",
                    runtime.config.name,
                    runtime.config.name,
                    "modbus_rtu",
                    event.severity,
                    event.event_type,
                    event.message,
                    "{}"));
                publish_error_state(timestampMs, "rs485", runtime.config.name, runtime.config.name, "modbus_rtu", runtime.config.devicePath, event);
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
                storage::SensorEventInfo event{
                    .timestamp_ms = timestampMs,
                    .severity = "error",
                    .event_type = "connect_failed",
                    .message = "Failed to initialize Modbus TCP connection",
                };
                store_sensor_event(make_sensor_event(
                    timestampMs,
                    "modbus_tcp",
                    deviceId,
                    runtime.config.name,
                    "modbus_tcp",
                    event.severity,
                    event.event_type,
                    event.message,
                    "{}"));
                publish_error_state(timestampMs, "modbus_tcp", deviceId, runtime.config.name, "modbus_tcp", endpoint, event);
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
                const auto sampleRecords = build_dustrak_sample_records(timestampMs, runtime);
                store_sensor_samples(sampleRecords);

                auto faultEvent = dustrak_fault_event(timestampMs, runtime);
                if (faultEvent.has_value())
                {
                    store_sensor_event(make_sensor_event(
                        timestampMs,
                        "rs232",
                        runtime.config.name,
                        runtime.config.deviceName,
                        runtime.config.deviceType,
                        faultEvent->severity,
                        faultEvent->event_type,
                        faultEvent->message,
                        faultEvent->details_json));
                }

                storage::SensorDeviceState state;
                state.timestamp_ms = timestampMs;
                state.source = "rs232";
                state.device_id = runtime.config.name;
                state.device_name = runtime.config.deviceName;
                state.device_type = runtime.config.deviceType;
                state.status = faultEvent.has_value() ? "warning" : "ok";
                state.transport_type = "serial";
                state.endpoint = runtime.config.serial.devicePath_;
                state.metrics = build_dustrak_metrics(timestampMs, runtime);
                state.error = faultEvent;
                state.raw_json = sensorPacket;
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
                    storage::SensorEventInfo event{
                        .timestamp_ms = timestampMs,
                        .severity = "error",
                        .event_type = "connect_failed",
                        .message = "Failed to reconnect Modbus RTU connection",
                    };
                    store_sensor_event(make_sensor_event(
                        timestampMs,
                        "rs485",
                        runtime.config.name,
                        runtime.config.name,
                        "modbus_rtu",
                        event.severity,
                        event.event_type,
                        event.message,
                        "{}"));
                    publish_error_state(timestampMs, "rs485", runtime.config.name, runtime.config.name, "modbus_rtu", runtime.config.devicePath, event);
                }
                continue;
            }

            const std::int64_t timestampMs = unix_timestamp_ms();
            const bool pollOk = runtime.client_.poll(runtime.samples);
            if (!pollOk)
            {
                SPDLOG_WARN("Modbus RTU poll had read failures slot={}", runtime.config.name);
                const auto &errors = runtime.client_.last_errors();
                if (errors.empty())
                {
                    storage::SensorEventInfo event{
                        .timestamp_ms = timestampMs,
                        .severity = "warning",
                        .event_type = "poll_failed",
                        .message = "Modbus RTU poll had read failures",
                    };
                    store_sensor_event(make_sensor_event(timestampMs, "rs485", runtime.config.name, runtime.config.name, "modbus_rtu", event.severity, event.event_type, event.message, "{}"));
                }
                else
                {
                    for (const auto &error : errors)
                    {
                        store_sensor_event(make_sensor_event(
                            timestampMs,
                            "rs485",
                            runtime.config.name,
                            runtime.config.name,
                            "modbus_rtu",
                            "warning",
                            "register_read_failed",
                            error.message,
                            modbus_register_details_json(error.source)));
                    }
                }
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
            state.metrics = build_modbus_metrics(timestampMs, runtime.samples);
            if (!pollOk)
            {
                state.error = storage::SensorEventInfo{
                    .timestamp_ms = timestampMs,
                    .severity = "warning",
                    .event_type = "poll_failed",
                    .message = "Modbus RTU poll had read failures",
                    .details_json = "{}",
                };
            }
            publish_sensor_state(state);
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
                    storage::SensorEventInfo event{
                        .timestamp_ms = timestampMs,
                        .severity = "error",
                        .event_type = "connect_failed",
                        .message = "Failed to reconnect Modbus TCP connection",
                    };
                    store_sensor_event(make_sensor_event(timestampMs, "modbus_tcp", deviceId, runtime.config.name, "modbus_tcp", event.severity, event.event_type, event.message, "{}"));
                    publish_error_state(timestampMs, "modbus_tcp", deviceId, runtime.config.name, "modbus_tcp", endpoint, event);
                }
                continue;
            }

            const std::int64_t timestampMs = unix_timestamp_ms();
            const std::string deviceId = modbus_tcp_device_id(runtime, index);
            const bool pollOk = runtime.client_.poll(runtime.samples);
            if (!pollOk)
            {
                SPDLOG_WARN("Modbus TCP poll had read failures slot={} endpoint={}:{}", runtime.config.id, runtime.config.ip, runtime.config.port);
                const auto &errors = runtime.client_.last_errors();
                if (errors.empty())
                {
                    storage::SensorEventInfo event{
                        .timestamp_ms = timestampMs,
                        .severity = "warning",
                        .event_type = "poll_failed",
                        .message = "Modbus TCP poll had read failures",
                    };
                    store_sensor_event(make_sensor_event(timestampMs, "modbus_tcp", deviceId, runtime.config.name, "modbus_tcp", event.severity, event.event_type, event.message, "{}"));
                }
                else
                {
                    for (const auto &error : errors)
                    {
                        store_sensor_event(make_sensor_event(
                            timestampMs,
                            "modbus_tcp",
                            deviceId,
                            runtime.config.name,
                            "modbus_tcp",
                            "warning",
                            "register_read_failed",
                            error.message,
                            modbus_register_details_json(error.source)));
                    }
                }
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
            state.metrics = build_modbus_metrics(timestampMs, runtime.samples);
            if (!pollOk)
            {
                state.error = storage::SensorEventInfo{
                    .timestamp_ms = timestampMs,
                    .severity = "warning",
                    .event_type = "poll_failed",
                    .message = "Modbus TCP poll had read failures",
                    .details_json = "{}",
                };
            }
            publish_sensor_state(state);
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
