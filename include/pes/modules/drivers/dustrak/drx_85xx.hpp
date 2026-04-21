#pragma once

#include "pes/modules/protocols/serial/serial.hpp"
#include <optional>
#include <string>
#include <vector>

namespace module::drivers::dustrak
{

    class drx85xx
    {
        public:
        enum class modelKind
        {
            Unknown,
            DustTrakII_8530,
            DustTrakII_8532,
            DustTrakDRX_8533,
            DustTrakDRX_8534,
        };

        enum class channel
        {
            PM1 = 0,
            PM25 = 1,
            RespirablePM4 = 2,
            PM10 = 3,
            Total = 4,
        };

        enum class loggingProgram
        {
            Survey = 0,
            Manual = 1,
            Program1 = 2,
            Program2 = 3,
            Program3 = 4,
            Program4 = 5,
            Program5 = 6,
        };

        enum class alarmState
        {
            Off = 0,
            Audible = 1,
            Visible = 2,
            AudibleVisible = 3,
            Relay = 4,
            AudibleRelay = 5,
            VisibleRelay = 6,
            AudibleVisibleRelay = 7,
        };

        enum class analogOutputState
        {
            Off = 0,
            Voltage = 1,
            Current = 2,
        };

        struct pollingConfig
        {
            bool readIdentityOnInit {true};
            bool pollStatus {true};
            bool autoStartMeasurement {false};
            bool pollMeasurements {true};
            bool pollMeasurementStats {false};
            bool pollFaultMessages {false};
            bool pollAlarmMessages {false};
            bool pollLogInfo {false};
        };

        struct driverConfig
        {
            pollingConfig polling {};
            bool updateRamAfterWrite {true};
        };

        struct deviceIdentity
        {
            modelKind model {modelKind::Unknown};
            std::string modelText {};
            std::string serialNumber {};
            std::string firmwareVersion {};
        };

        struct measurementSnapshot
        {
            int elapsedSeconds {0};
            std::vector<double> channelValuesMgPerM3 {};
        };

        struct channelStats
        {
            double currentMgPerM3 {0.0};
            double minimumMgPerM3 {0.0};
            double maximumMgPerM3 {0.0};
            double averageMgPerM3 {0.0};
            double twaMgPerM3 {0.0};
        };

        struct measurementStats
        {
            int elapsedSeconds {0};
            std::vector<channelStats> channels {};
        };

        struct loggingInfo
        {
            std::string logName {};
            int logError {0};
            int totalTimeSeconds {0};
            int timeElapsedSeconds {0};
            int timeRemainingSeconds {0};
            int currentTest {0};
            int totalTests {0};
        };

        struct loggingModeConfig
        {
            std::string startTime {"00:00:00"};
            std::string startDate {"01/01/2000"};
            std::string interval {"0:1"};
            std::string testLength {"0:0:0"};
            int numberOfTests {1};
            std::string timeBetweenTests {"0:0:0"};
            int timeConstant {5};
            bool useStartTime {false};
            bool useStartDate {false};
            std::string autoZeroInterval {"0:15"};
            bool autoZeroEnabled {false};
            std::string programName {"PROGRAM"};
        };

        struct alarmConfig
        {
            alarmState alarm1State {alarmState::Off};
            double alarm1ValueMgPerM3 {0.0};
            bool stelAlarm1Enabled {false};
            alarmState alarm2State {alarmState::Off};
            double alarm2ValueMgPerM3 {0.0};
        };

        struct analogOutputConfig
        {
            analogOutputState outputState {analogOutputState::Off};
            std::optional<channel> outputChannel {};
            double minimumRangeMgPerM3 {0.0};
            double maximumRangeMgPerM3 {0.0};
        };

        struct userCalibrationConfig
        {
            int index {0};
            double sizeCorrectionFactor {1.0};
            double photometricCalibrationFactor {1.0};
            std::string name {"USER CAL"};
        };

        struct zeroingStatus
        {
            int currentSeconds {0};
            int totalSeconds {0};
        };

        struct deviceState
        {
            deviceIdentity identity {};
            std::string pollingStatus {};
            measurementSnapshot latestMeasurement {};
            std::optional<measurementStats> latestMeasurementStats {};
            std::vector<int> faultMessageFlags {};
            std::vector<int> alarmMessageFlags {};
            std::optional<loggingInfo> latestLogInfo {};
            std::string rawMemoryState {};
            std::string rawDateTime {};
            std::string rawFilterChangeDate {};
            std::string rawCalibrationDate {};
            std::optional<zeroingStatus> latestZeroingStatus {};
        };

        drx85xx();
        ~drx85xx();
        drx85xx(const drx85xx&) = delete;
        drx85xx& operator=(const drx85xx&) = delete;

        void init(module::protocols::serial::serialPort& serialPort);
        void configure(const driverConfig& config);
        const driverConfig& config() const;
        const deviceState& state() const;
        const std::string& json_packet() const;

        void loop(std::string& json_packet);

        bool refresh_identity();
        bool refresh_status();
        bool start_measurement();
        bool stop_measurement();
        bool update_ram();
        bool shutdown();
        bool read_current_measurements();
        bool read_measurement_statistics();
        bool read_logged_measurements();
        bool read_fault_messages();
        bool read_alarm_messages();
        bool read_logging_info();
        bool read_logging_mode(loggingProgram program, loggingModeConfig& outConfig);
        bool write_logging_mode(loggingProgram program, const loggingModeConfig& config);
        bool read_current_logging_program(loggingProgram& program);
        bool set_current_logging_program(loggingProgram program);
        bool read_alarm_settings(channel measurementChannel, alarmConfig& outConfig);
        bool write_alarm_settings(channel measurementChannel, const alarmConfig& config);
        bool read_analog_output_settings(analogOutputConfig& outConfig);
        bool write_analog_output_settings(const analogOutputConfig& config);
        bool read_current_user_calibration(int& calibrationIndex);
        bool set_current_user_calibration(int calibrationIndex);
        bool read_user_calibration(int calibrationIndex, userCalibrationConfig& outConfig);
        bool write_user_calibration(int calibrationIndex, const userCalibrationConfig& config);
        bool read_date_time(std::string& outDateTime);
        bool write_date_time(const std::string& dateTime);
        bool read_filter_change_date(std::string& outFilterChangeDate);
        bool read_calibration_date(std::string& outCalibrationDate);
        bool zero_instrument();
        bool read_zeroing_status(zeroingStatus& outStatus);
        bool read_memory_state(std::string& outMemoryState);

        private:
        bool send_command(const std::string& command, std::string& response);
        bool send_ack_command(const std::string& command, bool applyUpdateRam);
        bool ensure_port() const;
        bool is_drx_model() const;
        bool identity_complete() const;
        bool validate_model_response(const std::string& response) const;
        bool validate_serial_response(const std::string& response) const;
        bool validate_firmware_response(const std::string& response) const;
        bool validate_status_response(const std::string& response) const;
        bool validate_measurement_response(const std::string& response) const;
        void resync_after_unexpected_response(const std::string& command, const std::string& response);
        std::string build_json_packet() const;

        module::protocols::serial::serialPort* serialPort_ {nullptr};
        driverConfig config_ {};
        deviceState state_ {};
        std::string jsonPacket_ {};
        bool initialized_ {false};
        bool measurementStarted_ {false};
    };

}
