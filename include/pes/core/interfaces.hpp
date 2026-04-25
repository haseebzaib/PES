#pragma once

#include "pes/modules/protocols/serial/serial.hpp"
#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "string"
#include "string_view"
#include "array"

namespace core
{

    constexpr std::string_view rs232Ch0Path = "/dev/ttyAMA2";
    constexpr std::string_view rs232Ch1Path = "/dev/ttyAMA3";
    constexpr std::string_view rs485Ch2Path = "/dev/ttyAMA4";
    constexpr std::string_view rs485Ch3Path = "/dev/ttyAMA0";

    constexpr std::string_view rs232ConfigRedisKey = "rs232_config";
    constexpr std::string_view rs232ConfigFile = "/opt/gateway/software_storage/AES/rs232_config.json";

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

    extern std::array<sensorRuntime, 4> sensors;



    void interfaces_startup();
    void interfaces_loop();

}
