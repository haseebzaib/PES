#include "pes/core/application.hpp"
#include "pes/core/sensor_thread.hpp"
#include "pes/core/interfaces.hpp"
#include "pes/modules/protocols/serial/serial.hpp"
#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "pes/utils/file/file_reader.hpp"
#include "pes/utils/file/json_file.hpp"
#include "spdlog/spdlog.h"
#include "vector"
#include <cmath>
#include <thread>
#include "cstdlib"

namespace core
{

    std::array<sensorRuntime, 4> sensors;
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

    void interfaces_startup()
    {
        if (read_rs232_config(true))
        {
            rs232_init();
        }
    }

    void interfaces_loop()
    {
        if (read_rs232_config(false))
        {
            rs232_init();
        }

        run_sensor_loop();
    }

}
