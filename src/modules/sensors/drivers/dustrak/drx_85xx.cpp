#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cctype>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace module::drivers::dustrak
{

    namespace
    {
        constexpr auto init_retry_interval = std::chrono::seconds(2);
        constexpr auto start_retry_interval = std::chrono::seconds(5);
        constexpr auto status_poll_interval = std::chrono::seconds(5);
        constexpr auto measurement_poll_interval = std::chrono::seconds(1);
        constexpr auto optional_poll_interval = std::chrono::seconds(5);
        constexpr auto timeout_cooldown = std::chrono::seconds(2);

        std::int64_t unix_timestamp_ms()
        {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                .count();
        }

        std::string trim(std::string value)
        {
            auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };

            value.erase(value.begin(), std::find_if_not(value.begin(), value.end(), is_space));
            value.erase(std::find_if_not(value.rbegin(), value.rend(), is_space).base(), value.end());
            return value;
        }

        std::vector<std::string> split_csv(const std::string& input)
        {
            std::vector<std::string> tokens;
            std::stringstream stream(input);
            std::string token;

            while (std::getline(stream, token, ','))
            {
                const std::string trimmed = trim(token);
                if (!trimmed.empty())
                {
                    tokens.push_back(trimmed);
                }
            }

            return tokens;
        }

        int parse_int(const std::string& value)
        {
            return std::stoi(trim(value));
        }

        double parse_double(const std::string& value)
        {
            return std::stod(trim(value));
        }

        drx85xx::modelKind parse_model_kind(const std::string& model)
        {
            if (model == "8530") return drx85xx::modelKind::DustTrakII_8530;
            if (model == "8532") return drx85xx::modelKind::DustTrakII_8532;
            if (model == "8533") return drx85xx::modelKind::DustTrakDRX_8533;
            if (model == "8534") return drx85xx::modelKind::DustTrakDRX_8534;
            return drx85xx::modelKind::Unknown;
        }

        std::size_t expected_measurement_field_count(const drx85xx::modelKind model)
        {
            switch (model)
            {
            case drx85xx::modelKind::DustTrakII_8530:
            case drx85xx::modelKind::DustTrakII_8532:
                return 2;
            case drx85xx::modelKind::DustTrakDRX_8533:
            case drx85xx::modelKind::DustTrakDRX_8534:
                return 6;
            case drx85xx::modelKind::Unknown:
                return 0;
            }

            return 0;
        }

        std::size_t expected_measurement_stats_field_count(const drx85xx::modelKind model)
        {
            switch (model)
            {
            case drx85xx::modelKind::DustTrakII_8530:
            case drx85xx::modelKind::DustTrakII_8532:
                return 6;
            case drx85xx::modelKind::DustTrakDRX_8533:
            case drx85xx::modelKind::DustTrakDRX_8534:
                return 26;
            case drx85xx::modelKind::Unknown:
                return 0;
            }

            return 0;
        }

        std::size_t expected_fault_flag_count(const drx85xx::modelKind model)
        {
            switch (model)
            {
            case drx85xx::modelKind::DustTrakII_8530:
                return 13;
            case drx85xx::modelKind::DustTrakII_8532:
                return 12;
            case drx85xx::modelKind::DustTrakDRX_8533:
                return 17;
            case drx85xx::modelKind::DustTrakDRX_8534:
                return 16;
            case drx85xx::modelKind::Unknown:
                return 0;
            }

            return 0;
        }

        std::size_t expected_alarm_flag_count(const drx85xx::modelKind model)
        {
            switch (model)
            {
            case drx85xx::modelKind::DustTrakII_8530:
            case drx85xx::modelKind::DustTrakII_8532:
                return 2;
            case drx85xx::modelKind::DustTrakDRX_8533:
            case drx85xx::modelKind::DustTrakDRX_8534:
                return 10;
            case drx85xx::modelKind::Unknown:
                return 0;
            }

            return 0;
        }

        std::size_t expected_analog_field_count(const drx85xx::modelKind model)
        {
            switch (model)
            {
            case drx85xx::modelKind::DustTrakII_8530:
            case drx85xx::modelKind::DustTrakII_8532:
                return 3;
            case drx85xx::modelKind::DustTrakDRX_8533:
            case drx85xx::modelKind::DustTrakDRX_8534:
                return 4;
            case drx85xx::modelKind::Unknown:
                return 0;
            }

            return 0;
        }

        bool is_digits_only(const std::string& value)
        {
            return !value.empty() && std::all_of(value.begin(), value.end(), [](unsigned char ch) {
                return std::isdigit(ch) != 0;
            });
        }

        bool looks_like_version(const std::string& value)
        {
            if (value.empty() || value.find(',') != std::string::npos)
            {
                return false;
            }

            bool hasDigit = false;
            for (const unsigned char ch : value)
            {
                if (std::isdigit(ch) != 0)
                {
                    hasDigit = true;
                    continue;
                }

                if (ch == '.' || ch == '-' || ch == '_' || std::isalpha(ch) != 0)
                {
                    continue;
                }

                return false;
            }

            return hasDigit;
        }

        bool looks_like_date(const std::string& value)
        {
            if (value.size() != 10 || value[2] != '/' || value[5] != '/')
            {
                return false;
            }

            return is_digits_only(value.substr(0, 2))
                && is_digits_only(value.substr(3, 2))
                && is_digits_only(value.substr(6, 4));
        }

        bool looks_like_time(const std::string& value)
        {
            if (value.size() != 8 || value[2] != ':' || value[5] != ':')
            {
                return false;
            }

            return is_digits_only(value.substr(0, 2))
                && is_digits_only(value.substr(3, 2))
                && is_digits_only(value.substr(6, 2));
        }

        bool looks_like_datetime(const std::string& value)
        {
            if (value.size() != 19 || value[10] != ',')
            {
                return false;
            }

            return looks_like_date(value.substr(0, 10))
                && looks_like_time(value.substr(11, 8));
        }

        bool parseable_int(const std::string& value)
        {
            try
            {
                static_cast<void>(parse_int(value));
                return true;
            }
            catch (const std::exception&)
            {
                return false;
            }
        }

        bool parseable_double(const std::string& value)
        {
            try
            {
                static_cast<void>(parse_double(value));
                return true;
            }
            catch (const std::exception&)
            {
                return false;
            }
        }

        bool all_parseable_ints(const std::vector<std::string>& values)
        {
            return std::all_of(values.begin(), values.end(), [](const std::string& value) {
                return parseable_int(value);
            });
        }

        std::string to_string(drx85xx::loggingProgram program)
        {
            return std::to_string(static_cast<int>(program));
        }

        std::string to_string(drx85xx::channel channel)
        {
            return std::to_string(static_cast<int>(channel));
        }

        std::string to_string(drx85xx::alarmState state)
        {
            return std::to_string(static_cast<int>(state));
        }

        std::string to_string(drx85xx::analogOutputState state)
        {
            return std::to_string(static_cast<int>(state));
        }

        std::string escape_json(const std::string& input)
        {
            std::string escaped;
            escaped.reserve(input.size());

            for (const char ch : input)
            {
                switch (ch)
                {
                case '\\':
                    escaped += "\\\\";
                    break;
                case '"':
                    escaped += "\\\"";
                    break;
                case '\b':
                    escaped += "\\b";
                    break;
                case '\f':
                    escaped += "\\f";
                    break;
                case '\n':
                    escaped += "\\n";
                    break;
                case '\r':
                    escaped += "\\r";
                    break;
                case '\t':
                    escaped += "\\t";
                    break;
                default:
                    escaped += ch;
                    break;
                }
            }

            return escaped;
        }

        std::string json_bool(bool value)
        {
            return value ? "true" : "false";
        }

        void append_json_number_array(std::ostringstream& output, const std::vector<double>& values)
        {
            output << '[';
            for (std::size_t index = 0; index < values.size(); ++index)
            {
                if (index > 0)
                {
                    output << ',';
                }
                output << values[index];
            }
            output << ']';
        }

        void append_json_int_array(std::ostringstream& output, const std::vector<int>& values)
        {
            output << '[';
            for (std::size_t index = 0; index < values.size(); ++index)
            {
                if (index > 0)
                {
                    output << ',';
                }
                output << values[index];
            }
            output << ']';
        }
    }

    drx85xx::drx85xx() = default;

    drx85xx::~drx85xx()
    {
        if (serialPort_ && measurementStarted_)
        {
            stop_measurement();
        }
    }

    void drx85xx::init(module::protocols::serial::serialPort& serialPort)
    {
        serialPort_ = &serialPort;
        initialized_ = false;
        nextCommandAttempt_ = {};
        nextInitAttempt_ = {};
        nextStartAttempt_ = {};
        nextStatusPoll_ = {};
        nextMeasurementPoll_ = {};
        nextFaultPoll_ = {};
        nextAlarmPoll_ = {};
        nextLogInfoPoll_ = {};
    }

    void drx85xx::configure(const driverConfig& config)
    {
        config_ = config;
    }

    const drx85xx::driverConfig& drx85xx::config() const
    {
        return config_;
    }

    const drx85xx::deviceState& drx85xx::state() const
    {
        return state_;
    }

    const drx85xx::communicationHealth& drx85xx::health() const
    {
        return health_;
    }

    const std::string& drx85xx::json_packet() const
    {
        return jsonPacket_;
    }

    std::int64_t drx85xx::last_measurement_timestamp_ms() const
    {
        return lastMeasurementTimestampMs_;
    }

    void drx85xx::loop(std::string& json_packet)
    {
        if (!ensure_port())
        {
            json_packet = jsonPacket_;
            return;
        }

        const auto now = clock::now();
        if (!poll_due(nextCommandAttempt_, now))
        {
            json_packet = jsonPacket_;
            return;
        }

        if (!initialized_)
        {
            if (!poll_due(nextInitAttempt_, now))
            {
                json_packet = jsonPacket_;
                return;
            }

            if (config_.polling.readIdentityOnInit)
            {
                if (!refresh_identity())
                {
                    schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                    schedule_poll(nextInitAttempt_, now, init_retry_interval);
                    jsonPacket_ = build_json_packet();
                    json_packet = jsonPacket_;
                    return;
                }
            }

            if (config_.polling.pollStatus)
            {
                if (!refresh_status())
                {
                    schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                    schedule_poll(nextInitAttempt_, now, init_retry_interval);
                    jsonPacket_ = build_json_packet();
                    json_packet = jsonPacket_;
                    return;
                }
            }

            initialized_ = true;

            // Keep initial identity/status reads separate from normal polling so
            // slower DustTrak units are not hit with duplicate back-to-back commands.
            jsonPacket_ = build_json_packet();
            json_packet = jsonPacket_;
            return;
        }

        if (config_.polling.readIdentityOnInit && !identity_complete())
        {
            if (!poll_due(nextInitAttempt_, now))
            {
                json_packet = jsonPacket_;
                return;
            }

            if (!refresh_identity())
            {
                schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                schedule_poll(nextInitAttempt_, now, init_retry_interval);
                jsonPacket_ = build_json_packet();
                json_packet = jsonPacket_;
                return;
            }

            // Do not stack status/measurement polling on top of identity recovery.
            jsonPacket_ = build_json_packet();
            json_packet = jsonPacket_;
            return;
        }

        if (config_.polling.autoStartMeasurement && !measurementStarted_)
        {
            if (!poll_due(nextStartAttempt_, now))
            {
                json_packet = jsonPacket_;
                return;
            }

            if (!start_measurement())
            {
                schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                schedule_poll(nextStartAttempt_, now, start_retry_interval);
                jsonPacket_ = build_json_packet();
                json_packet = jsonPacket_;
                return;
            }

            schedule_poll(nextStatusPoll_, now, status_poll_interval);
        }

        if (config_.polling.pollStatus && poll_due(nextStatusPoll_, now))
        {
            if (!refresh_status())
            {
                SPDLOG_WARN("MSTATUS failed during steady-state polling; pausing DustTrak command stream before next poll");
                schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                schedule_poll(nextStatusPoll_, now, timeout_cooldown);
                jsonPacket_ = build_json_packet();
                json_packet = jsonPacket_;
                return;
            }
            else
            {
                schedule_poll(nextStatusPoll_, now, status_poll_interval);
            }
        }

        if (config_.polling.pollMeasurementStats && poll_due(nextMeasurementPoll_, now))
        {
            if (!read_measurement_statistics())
            {
                schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                schedule_poll(nextMeasurementPoll_, now, timeout_cooldown);
                jsonPacket_ = build_json_packet();
                json_packet = jsonPacket_;
                return;
            }
            schedule_poll(nextMeasurementPoll_, now, measurement_poll_interval);
        }
        else if (config_.polling.pollMeasurements && poll_due(nextMeasurementPoll_, now))
        {
            if (!read_current_measurements())
            {
                schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                schedule_poll(nextMeasurementPoll_, now, timeout_cooldown);
                jsonPacket_ = build_json_packet();
                json_packet = jsonPacket_;
                return;
            }
            schedule_poll(nextMeasurementPoll_, now, measurement_poll_interval);
        }

        if (config_.polling.pollFaultMessages && poll_due(nextFaultPoll_, now))
        {
            if (!read_fault_messages())
            {
                schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                schedule_poll(nextFaultPoll_, now, timeout_cooldown);
                jsonPacket_ = build_json_packet();
                json_packet = jsonPacket_;
                return;
            }
            schedule_poll(nextFaultPoll_, now, optional_poll_interval);
        }

        if (config_.polling.pollAlarmMessages && poll_due(nextAlarmPoll_, now))
        {
            if (!read_alarm_messages())
            {
                schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                schedule_poll(nextAlarmPoll_, now, timeout_cooldown);
                jsonPacket_ = build_json_packet();
                json_packet = jsonPacket_;
                return;
            }
            schedule_poll(nextAlarmPoll_, now, optional_poll_interval);
        }

        if (config_.polling.pollLogInfo && poll_due(nextLogInfoPoll_, now))
        {
            if (!read_logging_info())
            {
                schedule_poll(nextCommandAttempt_, now, timeout_cooldown);
                schedule_poll(nextLogInfoPoll_, now, timeout_cooldown);
                jsonPacket_ = build_json_packet();
                json_packet = jsonPacket_;
                return;
            }
            schedule_poll(nextLogInfoPoll_, now, optional_poll_interval);
        }

        jsonPacket_ = build_json_packet();
        json_packet = jsonPacket_;
    }

    bool drx85xx::refresh_identity()
    {
        std::string response;

        if (!send_command("RDMN", response))
        {
            return false;
        }

        if (!validate_model_response(response))
        {
            resync_after_unexpected_response("RDMN", response);
            return false;
        }

        state_.identity.modelText = response;
        state_.identity.model = parse_model_kind(response);

        if (!send_command("RDSN", response))
        {
            return false;
        }

        if (!validate_serial_response(response))
        {
            resync_after_unexpected_response("RDSN", response);
            return false;
        }
        state_.identity.serialNumber = response;

        if (!send_command("RDBS", response))
        {
            return false;
        }

        if (!validate_firmware_response(response))
        {
            resync_after_unexpected_response("RDBS", response);
            return false;
        }
        state_.identity.firmwareVersion = response;

        SPDLOG_INFO(
            "DustTrak identity: model={} serial={} firmware={}",
            state_.identity.modelText,
            state_.identity.serialNumber,
            state_.identity.firmwareVersion);

        return true;
    }

    bool drx85xx::refresh_status()
    {
        std::string response;
        if (!send_command("MSTATUS", response))
        {
            return false;
        }

        if (!validate_status_response(response))
        {
            resync_after_unexpected_response("MSTATUS", response);
            return false;
        }

        state_.pollingStatus = response;
        measurementStarted_ = (response == "Running");
        return true;
    }

    bool drx85xx::start_measurement()
    {
        if (!send_ack_command("MSTART", false))
        {
            return false;
        }

        measurementStarted_ = true;
        return true;
    }

    bool drx85xx::stop_measurement()
    {
        if (!send_ack_command("MSTOP", false))
        {
            return false;
        }

        measurementStarted_ = false;
        return true;
    }

    bool drx85xx::update_ram()
    {
        return send_ack_command("MUPDATE", false);
    }

    bool drx85xx::shutdown()
    {
        return send_ack_command("MSHUTDOWN", false);
    }

    bool drx85xx::read_current_measurements()
    {
        std::string response;
        if (!send_command("RMMEAS", response))
        {
            return false;
        }

        const auto values = split_csv(response);
        if (!validate_measurement_response(response))
        {
            resync_after_unexpected_response("RMMEAS", response);
            return false;
        }

        state_.latestMeasurement.elapsedSeconds = parse_int(values.front());
        state_.latestMeasurement.channelValuesMgPerM3.clear();

        for (std::size_t index = 1; index < values.size(); ++index)
        {
            state_.latestMeasurement.channelValuesMgPerM3.push_back(parse_double(values[index]));
        }

        SPDLOG_INFO("RMMEAS: {} values at t={}s",
            state_.latestMeasurement.channelValuesMgPerM3.size(),
            state_.latestMeasurement.elapsedSeconds);

        record_measurement_success();
        return true;
    }

    bool drx85xx::read_measurement_statistics()
    {
        std::string response;
        if (!send_command("RMMEASSTATS", response))
        {
            return false;
        }

        const auto values = split_csv(response);
        const std::size_t expectedFieldCount = expected_measurement_stats_field_count(state_.identity.model);
        if ((expectedFieldCount != 0U && values.size() != expectedFieldCount)
            || (expectedFieldCount == 0U && (values.size() < 6 || ((values.size() - 1U) % 5U) != 0U)))
        {
            resync_after_unexpected_response("RMMEASSTATS", response);
            return false;
        }

        try
        {
            static_cast<void>(parse_int(values.front()));
            for (std::size_t index = 1; index < values.size(); ++index)
            {
                static_cast<void>(parse_double(values[index]));
            }
        }
        catch (const std::exception&)
        {
            resync_after_unexpected_response("RMMEASSTATS", response);
            return false;
        }

        measurementStats stats;
        stats.elapsedSeconds = parse_int(values.front());

        for (std::size_t index = 1; index + 4 < values.size(); index += 5)
        {
            channelStats channelStatsData;
            channelStatsData.currentMgPerM3 = parse_double(values[index]);
            channelStatsData.minimumMgPerM3 = parse_double(values[index + 1]);
            channelStatsData.maximumMgPerM3 = parse_double(values[index + 2]);
            channelStatsData.averageMgPerM3 = parse_double(values[index + 3]);
            channelStatsData.twaMgPerM3 = parse_double(values[index + 4]);
            stats.channels.push_back(channelStatsData);
        }

        state_.latestMeasurementStats = stats;
        SPDLOG_INFO("RMMEASSTATS: {} channels at t={}s", stats.channels.size(), stats.elapsedSeconds);
        record_measurement_success();
        return true;
    }

    bool drx85xx::read_logged_measurements()
    {
        std::string response;
        if (!send_command("RMLOGGEDMEAS", response))
        {
            return false;
        }

        if (!validate_measurement_response(response))
        {
            resync_after_unexpected_response("RMLOGGEDMEAS", response);
            return false;
        }

        SPDLOG_INFO("RMLOGGEDMEAS: {}", response);
        return true;
    }

    bool drx85xx::read_fault_messages()
    {
        std::string response;
        if (!send_command("RMMESSAGES", response))
        {
            return false;
        }

        const auto values = split_csv(response);
        const std::size_t expectedCount = expected_fault_flag_count(state_.identity.model);
        if (values.empty()
            || !all_parseable_ints(values)
            || (expectedCount != 0U && values.size() != expectedCount))
        {
            resync_after_unexpected_response("RMMESSAGES", response);
            return false;
        }
        state_.faultMessageFlags.clear();
        for (const auto& value : values)
        {
            state_.faultMessageFlags.push_back(parse_int(value));
        }

        SPDLOG_INFO("RMMESSAGES: {} fault/status values", state_.faultMessageFlags.size());
        return true;
    }

    bool drx85xx::read_alarm_messages()
    {
        std::string response;
        if (!send_command("RMALARM", response))
        {
            return false;
        }

        const auto values = split_csv(response);
        const std::size_t expectedCount = expected_alarm_flag_count(state_.identity.model);
        if (values.empty()
            || !all_parseable_ints(values)
            || (expectedCount != 0U && values.size() != expectedCount))
        {
            resync_after_unexpected_response("RMALARM", response);
            return false;
        }
        state_.alarmMessageFlags.clear();
        for (const auto& value : values)
        {
            state_.alarmMessageFlags.push_back(parse_int(value));
        }

        SPDLOG_INFO("RMALARM: {} alarm values", state_.alarmMessageFlags.size());
        return true;
    }

    bool drx85xx::read_logging_info()
    {
        std::string response;
        if (!send_command("RMLOGINFO", response))
        {
            return false;
        }

        const auto values = split_csv(response);
        if (values.size() != 7)
        {
            resync_after_unexpected_response("RMLOGINFO", response);
            return false;
        }

        if (!parseable_int(values[1])
            || !parseable_int(values[2])
            || !parseable_int(values[3])
            || !parseable_int(values[4])
            || !parseable_int(values[5])
            || !parseable_int(values[6]))
        {
            resync_after_unexpected_response("RMLOGINFO", response);
            return false;
        }

        loggingInfo info;
        info.logName = values[0];
        info.logError = parse_int(values[1]);
        info.totalTimeSeconds = parse_int(values[2]);
        info.timeElapsedSeconds = parse_int(values[3]);
        info.timeRemainingSeconds = parse_int(values[4]);
        info.currentTest = parse_int(values[5]);
        info.totalTests = parse_int(values[6]);

        state_.latestLogInfo = info;
        return true;
    }

    bool drx85xx::read_logging_mode(loggingProgram program, loggingModeConfig& outConfig)
    {
        std::string response;
        if (!send_command("RMODELOG" + to_string(program), response))
        {
            return false;
        }

        const auto values = split_csv(response);
        if (values.size() != 12)
        {
            resync_after_unexpected_response("RMODELOG", response);
            return false;
        }

        if (!looks_like_time(values[0])
            || !looks_like_date(values[1])
            || !parseable_int(values[4])
            || !parseable_int(values[6])
            || !parseable_int(values[7])
            || !parseable_int(values[8])
            || !parseable_int(values[10]))
        {
            resync_after_unexpected_response("RMODELOG", response);
            return false;
        }

        outConfig.startTime = values[0];
        outConfig.startDate = values[1];
        outConfig.interval = values[2];
        outConfig.testLength = values[3];
        outConfig.numberOfTests = parse_int(values[4]);
        outConfig.timeBetweenTests = values[5];
        outConfig.timeConstant = parse_int(values[6]);
        outConfig.useStartTime = parse_int(values[7]) != 0;
        outConfig.useStartDate = parse_int(values[8]) != 0;
        outConfig.autoZeroInterval = values[9];
        outConfig.autoZeroEnabled = parse_int(values[10]) != 0;
        outConfig.programName = values[11];

        return true;
    }

    bool drx85xx::write_logging_mode(loggingProgram program, const loggingModeConfig& config)
    {
        std::ostringstream command;
        command
            << "WMODELOG" << to_string(program) << ' '
            << config.startTime << ','
            << config.startDate << ','
            << config.interval << ','
            << config.testLength << ','
            << config.numberOfTests << ','
            << config.timeBetweenTests << ','
            << config.timeConstant << ','
            << (config.useStartTime ? 1 : 0) << ','
            << (config.useStartDate ? 1 : 0) << ','
            << config.autoZeroInterval << ','
            << (config.autoZeroEnabled ? 1 : 0) << ','
            << config.programName;

        return send_ack_command(command.str(), true);
    }

    bool drx85xx::read_current_logging_program(loggingProgram& program)
    {
        std::string response;
        if (!send_command("RMODECURLOG", response))
        {
            return false;
        }

        if (!parseable_int(response))
        {
            resync_after_unexpected_response("RMODECURLOG", response);
            return false;
        }

        program = static_cast<loggingProgram>(parse_int(response));
        return true;
    }

    bool drx85xx::set_current_logging_program(loggingProgram program)
    {
        return send_ack_command("WMODECURLOG" + to_string(program), true);
    }

    bool drx85xx::read_alarm_settings(channel measurementChannel, alarmConfig& outConfig)
    {
        std::string response;
        const std::string command = "RMODEALARMTWO" + (is_drx_model() ? to_string(measurementChannel) : std::string{});
        if (!send_command(command, response))
        {
            return false;
        }

        const auto values = split_csv(response);
        if (values.size() != 5)
        {
            resync_after_unexpected_response("RMODEALARMTWO", response);
            return false;
        }

        if (!all_parseable_ints({values[0], values[2], values[3]})
            || !parseable_double(values[1])
            || !parseable_double(values[4]))
        {
            resync_after_unexpected_response("RMODEALARMTWO", response);
            return false;
        }

        outConfig.alarm1State = static_cast<alarmState>(parse_int(values[0]));
        outConfig.alarm1ValueMgPerM3 = parse_double(values[1]);
        outConfig.stelAlarm1Enabled = parse_int(values[2]) != 0;
        outConfig.alarm2State = static_cast<alarmState>(parse_int(values[3]));
        outConfig.alarm2ValueMgPerM3 = parse_double(values[4]);

        return true;
    }

    bool drx85xx::write_alarm_settings(channel measurementChannel, const alarmConfig& config)
    {
        std::ostringstream command;
        command << "WMODEALARMTWO";
        if (is_drx_model())
        {
            command << to_string(measurementChannel);
        }
        command << ' '
                << to_string(config.alarm1State) << ','
                << config.alarm1ValueMgPerM3 << ','
                << (config.stelAlarm1Enabled ? 1 : 0) << ','
                << to_string(config.alarm2State) << ','
                << config.alarm2ValueMgPerM3;

        return send_ack_command(command.str(), true);
    }

    bool drx85xx::read_analog_output_settings(analogOutputConfig& outConfig)
    {
        std::string response;
        if (!send_command("RMODEANALOG", response))
        {
            return false;
        }

        const auto values = split_csv(response);
        const std::size_t expectedCount = expected_analog_field_count(state_.identity.model);
        if ((expectedCount != 0U && values.size() != expectedCount)
            || (expectedCount == 0U && (values.size() < 3 || values.size() > 4)))
        {
            resync_after_unexpected_response("RMODEANALOG", response);
            return false;
        }

        if (!parseable_int(values[0])
            || (values.size() == 4 && !parseable_int(values[1]))
            || !parseable_double(values[values.size() - 2])
            || !parseable_double(values[values.size() - 1]))
        {
            resync_after_unexpected_response("RMODEANALOG", response);
            return false;
        }

        outConfig.outputState = static_cast<analogOutputState>(parse_int(values[0]));
        if (values.size() == 4)
        {
            outConfig.outputChannel = static_cast<channel>(parse_int(values[1]));
            outConfig.minimumRangeMgPerM3 = parse_double(values[2]);
            outConfig.maximumRangeMgPerM3 = parse_double(values[3]);
        }
        else
        {
            outConfig.outputChannel.reset();
            outConfig.minimumRangeMgPerM3 = parse_double(values[1]);
            outConfig.maximumRangeMgPerM3 = parse_double(values[2]);
        }

        return true;
    }

    bool drx85xx::write_analog_output_settings(const analogOutputConfig& config)
    {
        std::ostringstream command;
        command << "WMODEANALOG " << to_string(config.outputState);
        if (is_drx_model())
        {
            command << ',' << to_string(config.outputChannel.value_or(channel::Total));
        }
        command << ','
                << config.minimumRangeMgPerM3 << ','
                << config.maximumRangeMgPerM3;

        return send_ack_command(command.str(), true);
    }

    bool drx85xx::read_current_user_calibration(int& calibrationIndex)
    {
        std::string response;
        if (!send_command("RMODECURUSERCAL", response))
        {
            return false;
        }

        if (!parseable_int(response))
        {
            resync_after_unexpected_response("RMODECURUSERCAL", response);
            return false;
        }

        calibrationIndex = parse_int(response);
        return true;
    }

    bool drx85xx::set_current_user_calibration(int calibrationIndex)
    {
        return send_ack_command("WMODECURUSERCAL" + std::to_string(calibrationIndex), true);
    }

    bool drx85xx::read_user_calibration(int calibrationIndex, userCalibrationConfig& outConfig)
    {
        std::string response;
        if (!send_command("RMODEUSERCAL" + std::to_string(calibrationIndex), response))
        {
            return false;
        }

        const auto values = split_csv(response);
        if (values.size() != 4)
        {
            resync_after_unexpected_response("RMODEUSERCAL", response);
            return false;
        }

        if (!parseable_int(values[0])
            || !parseable_double(values[1])
            || !parseable_double(values[2]))
        {
            resync_after_unexpected_response("RMODEUSERCAL", response);
            return false;
        }

        outConfig.index = parse_int(values[0]);
        outConfig.sizeCorrectionFactor = parse_double(values[1]);
        outConfig.photometricCalibrationFactor = parse_double(values[2]);
        outConfig.name = values[3];
        return true;
    }

    bool drx85xx::write_user_calibration(int calibrationIndex, const userCalibrationConfig& config)
    {
        std::ostringstream command;
        command
            << "WMODEUSERCAL" << calibrationIndex << ' '
            << config.sizeCorrectionFactor << ','
            << config.photometricCalibrationFactor << ','
            << config.name;

        return send_ack_command(command.str(), true);
    }

    bool drx85xx::read_date_time(std::string& outDateTime)
    {
        if (!send_command("RSDATETIME", outDateTime))
        {
            return false;
        }

        if (!looks_like_datetime(outDateTime))
        {
            resync_after_unexpected_response("RSDATETIME", outDateTime);
            return false;
        }

        state_.rawDateTime = outDateTime;
        return true;
    }

    bool drx85xx::write_date_time(const std::string& dateTime)
    {
        return send_ack_command("WSDATETIME " + dateTime, true);
    }

    bool drx85xx::read_filter_change_date(std::string& outFilterChangeDate)
    {
        if (!send_command("RSFILTERCHANGEDATE", outFilterChangeDate))
        {
            return false;
        }

        const auto values = split_csv(outFilterChangeDate);
        if (values.size() != 2 || !looks_like_date(values[0]) || !parseable_int(values[1]))
        {
            resync_after_unexpected_response("RSFILTERCHANGEDATE", outFilterChangeDate);
            return false;
        }

        state_.rawFilterChangeDate = outFilterChangeDate;
        return true;
    }

    bool drx85xx::read_calibration_date(std::string& outCalibrationDate)
    {
        if (!send_command("RSCALDATE", outCalibrationDate))
        {
            return false;
        }

        const auto values = split_csv(outCalibrationDate);
        if (values.size() != 4
            || !looks_like_date(values[0])
            || !parseable_int(values[1])
            || !looks_like_version(values[2])
            || !parseable_int(values[3]))
        {
            resync_after_unexpected_response("RSCALDATE", outCalibrationDate);
            return false;
        }

        state_.rawCalibrationDate = outCalibrationDate;
        return true;
    }

    bool drx85xx::zero_instrument()
    {
        return send_ack_command("MZERO", false);
    }

    bool drx85xx::read_zeroing_status(zeroingStatus& outStatus)
    {
        std::string response;
        if (!send_command("RMZEROING", response))
        {
            return false;
        }

        const auto values = split_csv(response);
        if (values.size() != 2)
        {
            resync_after_unexpected_response("RMZEROING", response);
            return false;
        }

        if (!all_parseable_ints(values))
        {
            resync_after_unexpected_response("RMZEROING", response);
            return false;
        }

        outStatus.currentSeconds = parse_int(values[0]);
        outStatus.totalSeconds = parse_int(values[1]);
        state_.latestZeroingStatus = outStatus;
        return true;
    }

    bool drx85xx::read_memory_state(std::string& outMemoryState)
    {
        if (!send_command("RMMEMORY", outMemoryState))
        {
            return false;
        }

        if (outMemoryState.empty()
            || outMemoryState.find(':') == std::string::npos
            || outMemoryState.find("Available") == std::string::npos)
        {
            resync_after_unexpected_response("RMMEMORY", outMemoryState);
            return false;
        }

        state_.rawMemoryState = outMemoryState;
        return true;
    }

    bool drx85xx::send_command(const std::string& command, std::string& response)
    {
        if (!ensure_port())
        {
            return false;
        }

        SPDLOG_INFO("DustTrak TX: {}", command);

        if (!serialPort_->write(command + '\r'))
        {
            SPDLOG_ERROR("{}: write failed", command);
            record_command_failure(command, "write_failed");
            return false;
        }

        if (!serialPort_->read_line(response))
        {
            SPDLOG_WARN("{}: timed out waiting for response", command);
            record_command_failure(command, "timeout");
            return false;
        }

        response = trim(response);
        SPDLOG_INFO("DustTrak RX: {} -> {}", command, response);
        record_command_success();
        return true;
    }

    bool drx85xx::send_ack_command(const std::string& command, bool applyUpdateRam)
    {
        std::string response;
        if (!send_command(command, response))
        {
            return false;
        }

        if (response != "OK")
        {
            SPDLOG_WARN("{}: expected OK, got '{}'", command, response);
            return false;
        }

        if (applyUpdateRam && config_.updateRamAfterWrite)
        {
            return update_ram();
        }

        return true;
    }

    bool drx85xx::ensure_port() const
    {
        if (!serialPort_)
        {
            SPDLOG_ERROR("No serial port assigned");
            return false;
        }

        return true;
    }

    bool drx85xx::is_drx_model() const
    {
        return state_.identity.model == modelKind::DustTrakDRX_8533
            || state_.identity.model == modelKind::DustTrakDRX_8534;
    }

    bool drx85xx::identity_complete() const
    {
        return state_.identity.model != modelKind::Unknown
            && !state_.identity.modelText.empty()
            && !state_.identity.serialNumber.empty()
            && !state_.identity.firmwareVersion.empty();
    }

    bool drx85xx::validate_model_response(const std::string& response) const
    {
        return parse_model_kind(response) != modelKind::Unknown;
    }

    bool drx85xx::validate_serial_response(const std::string& response) const
    {
        if (response.find(',') != std::string::npos)
        {
            return false;
        }

        return response.size() >= 4 && is_digits_only(response);
    }

    bool drx85xx::validate_firmware_response(const std::string& response) const
    {
        return looks_like_version(response);
    }

    bool drx85xx::validate_status_response(const std::string& response) const
    {
        static const std::set<std::string> validStates{
            "Idle",
            "Running",
            "Zeroing",
            "FAIL",
        };

        return validStates.contains(response);
    }

    bool drx85xx::validate_measurement_response(const std::string& response) const
    {
        const auto values = split_csv(response);
        const std::size_t expectedFieldCount = expected_measurement_field_count(state_.identity.model);
        if ((expectedFieldCount != 0U && values.size() != expectedFieldCount)
            || (expectedFieldCount == 0U && values.size() < 2))
        {
            SPDLOG_WARN("RMMEAS: unexpected response '{}'", response);
            return false;
        }

        try
        {
            static_cast<void>(parse_int(values.front()));
            for (std::size_t index = 1; index < values.size(); ++index)
            {
                static_cast<void>(parse_double(values[index]));
            }
        }
        catch (const std::exception&)
        {
            SPDLOG_WARN("RMMEAS: malformed response '{}'", response);
            return false;
        }

        return true;
    }

    void drx85xx::record_command_success()
    {
        health_.consecutiveFailures = 0;
    }

    void drx85xx::record_command_failure(const std::string& command, const std::string& error)
    {
        ++health_.consecutiveFailures;
        health_.lastFailedCommand = command;
        health_.lastError = error;
        health_.lastFailureTimestampMs = unix_timestamp_ms();
    }

    void drx85xx::record_measurement_success()
    {
        lastMeasurementTimestampMs_ = unix_timestamp_ms();
    }

    void drx85xx::resync_after_unexpected_response(const std::string& command, const std::string& response)
    {
        SPDLOG_WARN("{}: unexpected response '{}', flushing RX to resync", command, response);
        if (serialPort_)
        {
            serialPort_->flush_rx();
        }
    }

    bool drx85xx::poll_due(const clock::time_point nextPoll, const clock::time_point now)
    {
        return nextPoll.time_since_epoch().count() == 0 || now >= nextPoll;
    }

    void drx85xx::schedule_poll(clock::time_point& nextPoll, const clock::time_point now, const std::chrono::milliseconds interval)
    {
        nextPoll = now + interval;
    }

    std::string drx85xx::build_json_packet() const
    {
        std::ostringstream output;
        output << '{';

        output << "\"sensor\":\"DustTrak\",";
        output << "\"model\":\"" << escape_json(state_.identity.modelText) << "\",";
        output << "\"serial_number\":\"" << escape_json(state_.identity.serialNumber) << "\",";
        output << "\"firmware_version\":\"" << escape_json(state_.identity.firmwareVersion) << "\",";
        output << "\"status\":\"" << escape_json(state_.pollingStatus) << "\",";
        output << "\"measurement_started\":" << json_bool(measurementStarted_) << ',';

        output << "\"measurement\":{";
        output << "\"elapsed_seconds\":" << state_.latestMeasurement.elapsedSeconds << ',';
        output << "\"values_mg_m3\":";
        append_json_number_array(output, state_.latestMeasurement.channelValuesMgPerM3);
        output << "},";

        output << "\"fault_flags\":";
        append_json_int_array(output, state_.faultMessageFlags);
        output << ',';

        output << "\"alarm_flags\":";
        append_json_int_array(output, state_.alarmMessageFlags);
        output << ',';

        output << "\"measurement_stats\":";
        if (state_.latestMeasurementStats.has_value())
        {
            output << '{';
            output << "\"elapsed_seconds\":" << state_.latestMeasurementStats->elapsedSeconds << ',';
            output << "\"channels\":[";
            for (std::size_t index = 0; index < state_.latestMeasurementStats->channels.size(); ++index)
            {
                if (index > 0)
                {
                    output << ',';
                }

                const auto& channelStats = state_.latestMeasurementStats->channels[index];
                output << '{'
                       << "\"current_mg_m3\":" << channelStats.currentMgPerM3 << ','
                       << "\"min_mg_m3\":" << channelStats.minimumMgPerM3 << ','
                       << "\"max_mg_m3\":" << channelStats.maximumMgPerM3 << ','
                       << "\"avg_mg_m3\":" << channelStats.averageMgPerM3 << ','
                       << "\"twa_mg_m3\":" << channelStats.twaMgPerM3
                       << '}';
            }
            output << "]}";
        }
        else
        {
            output << "null";
        }
        output << ',';

        output << "\"log_info\":";
        if (state_.latestLogInfo.has_value())
        {
            output << '{'
                   << "\"name\":\"" << escape_json(state_.latestLogInfo->logName) << "\","
                   << "\"error\":" << state_.latestLogInfo->logError << ','
                   << "\"total_time_seconds\":" << state_.latestLogInfo->totalTimeSeconds << ','
                   << "\"time_elapsed_seconds\":" << state_.latestLogInfo->timeElapsedSeconds << ','
                   << "\"time_remaining_seconds\":" << state_.latestLogInfo->timeRemainingSeconds << ','
                   << "\"current_test\":" << state_.latestLogInfo->currentTest << ','
                   << "\"total_tests\":" << state_.latestLogInfo->totalTests
                   << '}';
        }
        else
        {
            output << "null";
        }
        output << ',';

        output << "\"date_time\":\"" << escape_json(state_.rawDateTime) << "\",";
        output << "\"filter_change\":\"" << escape_json(state_.rawFilterChangeDate) << "\",";
        output << "\"calibration_date\":\"" << escape_json(state_.rawCalibrationDate) << "\",";
        output << "\"memory_state\":\"" << escape_json(state_.rawMemoryState) << "\",";

        output << "\"zeroing\":";
        if (state_.latestZeroingStatus.has_value())
        {
            output << '{'
                   << "\"current_seconds\":" << state_.latestZeroingStatus->currentSeconds << ','
                   << "\"total_seconds\":" << state_.latestZeroingStatus->totalSeconds
                   << '}';
        }
        else
        {
            output << "null";
        }

        output << '}';
        return output.str();
    }

}
