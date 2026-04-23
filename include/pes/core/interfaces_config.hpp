#pragma once

#include "pes/modules/protocols/serial/serial.hpp"
#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "string"
#include "string_view"

namespace core 
{

    constexpr std::string_view rs232Ch0Path = "/dev/ttyAMA2";
    constexpr std::string_view rs232Ch1Path = "/dev/ttyAMA3";
    constexpr std::string_view rs485Ch2Path = "/dev/ttyAMA4";
    constexpr std::string_view rs485Ch3Path = "/dev/ttyAMA0";


    extern module::protocols::serial::serialConfig rs232CfgCh0;
    extern module::protocols::serial::serialPort portRs232Ch0;
    extern module::protocols::serial::serialConfig rs232CfgCh1;
    extern module::protocols::serial::serialPort portRs232Ch1;
    extern module::protocols::serial::serialConfig rs485CfgCh2;
    extern module::protocols::serial::serialPort portRs485Ch2;
    extern module::protocols::serial::serialConfig rs485CfgCh3;
    extern module::protocols::serial::serialPort portRs485Ch3;


    extern module::drivers::dustrak::drx85xx drx85xx[2];

    void interface_configs_init();

}