#pragma once

#include "pes/modules/protocols/serial/serial.hpp"
#include "pes/modules/protocols/modbus/modbus_client.hpp"
#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "chrono"
#include "string"
#include "string_view"
#include "array"
#include "vector"

namespace core
{

    constexpr std::string_view rs232Ch0Path = "/dev/ttyAMA2";
    constexpr std::string_view rs232Ch1Path = "/dev/ttyAMA3";
    constexpr std::string_view rs485Ch2Path = "/dev/ttyAMA4";
    constexpr std::string_view rs485Ch3Path = "/dev/ttyAMA0";

    constexpr std::string_view rs232ConfigRedisKey = "rs232_config";
    constexpr std::string_view rs232ConfigFile = "/opt/gateway/software_storage/AES/rs232_config.json";

    constexpr std::string_view rs485ConfigRedisKey = "rs485_config";
    constexpr std::string_view rs485ConfigFile = "/opt/gateway/software_storage/AES/rs485_config.json";

    constexpr std::string_view modbusTcpConfigRedisKey = "modbus_tcp_config";
    constexpr std::string_view modbusTcpConfigFile = "/opt/gateway/software_storage/AES/modbus_tcp_config.json";

    constexpr std::size_t modbusTcpInterfaces = 2;
    constexpr std::size_t modbusTcpConnectionsPerInterface = 10;
    constexpr std::size_t modbusTcpMaxConnections = modbusTcpInterfaces * modbusTcpConnectionsPerInterface;

    enum class sensorKind
    {
        None,
        dustrak,
        TOTAL,
    };

    enum class transportKind
    {
        None,
        RS232,
        RS485,
        MOD_TCP,
        TOTAL,
    };

    struct sensorDustrakRuntimeConfigs
    {
        enum class runTimeConfig : std::uint8_t
        {
            alarmconfig = 0,
            analogoutputconfig,
            loggingmodeconfig,
            usercalibrationconfig,
            TOTAL,
        };
        std::array<bool, 4> enabled;
        std::array<module::drivers::dustrak::drx85xx::alarmConfig, 5> alarmConfigs;
        module::drivers::dustrak::drx85xx::analogOutputConfig analogOutputConfig;
        module::drivers::dustrak::drx85xx::loggingModeConfig loggingModeConfig;
        module::drivers::dustrak::drx85xx::userCalibrationConfig userCalibrationConfig;
    };

    struct sensorDustrakConfigs
    {
        module::drivers::dustrak::drx85xx::driverConfig driverConfig;
        sensorDustrakRuntimeConfigs runTimeConfigs;
    };

    struct sensorConfig
    {
        bool enabled;
        std::string name;
        sensorKind sensorKind_;
        transportKind transportKind_;
        module::protocols::serial::serialConfig serial;
        sensorDustrakConfigs sensorDustrakConfig;
    };

    struct sensorRuntime
    {
        sensorConfig config;
        module::protocols::serial::serialPort serialPort_;
        module::drivers::dustrak::drx85xx dustTrak;
    };

    struct modbusRtuRuntime
    {
        bool enabled {false};
        bool initialized {false};
        module::protocols::modbus::rtuConfig config;
        module::protocols::modbus::client client_;
        std::chrono::steady_clock::time_point lastPoll {};
        std::vector<module::protocols::modbus::sample> samples {};
    };

    struct modbusTcpRuntime
    {
        bool enabled {false};
        bool initialized {false};
        module::protocols::modbus::tcpConfig config;
        module::protocols::modbus::client client_;
        std::chrono::steady_clock::time_point lastPoll {};
        std::vector<module::protocols::modbus::sample> samples {};
    };

    extern std::array<sensorRuntime, 4> sensors;
    extern std::array<modbusRtuRuntime, 2> modbusRtuRuntimes;
    extern std::array<modbusTcpRuntime, modbusTcpMaxConnections> modbusTcpRuntimes;



    void interfaces_startup();
    void interfaces_loop();

}
