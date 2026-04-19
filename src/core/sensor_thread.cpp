#include "pes/core/application.hpp"
#include "pes/core/sensor_thread.hpp"
#include "pes/modules/protocols/serial/serial.hpp"
#include "spdlog/spdlog.h"
#include "vector"
#include <thread>
#include "cstdlib"

namespace core
{

    module::protocols::serial::serialConfig rs232CfgCh0{
        .devicePath_ = "/dev/ttyAMA2"};
    module::protocols::serial::serialPort portRs232Ch0;

    module::protocols::serial::serialConfig rs232CfgCh1{
        .devicePath_ = "/dev/ttyAMA3"};
    module::protocols::serial::serialPort portRs232Ch1;

    module::protocols::serial::serialConfig rs485CfgCh2{
        .devicePath_ = "/dev/ttyAMA4"};
    module::protocols::serial::serialPort portRs485Ch2;

    module::protocols::serial::serialConfig rs485CfgCh3{
        .devicePath_ = "/dev/ttyAMA0"};
    module::protocols::serial::serialPort portRs485Ch3;

    void toggle_led()
    {
        static std::string ledState = "off";
        ledState = (ledState == "off" ? "on" : "off");
        std::string led_cmd = "/opt/gateway/scripts/gateway-cm5-ioctl user1 " + ledState;
        std::system(led_cmd.c_str());
    }

    void sensor_thread()
    {

        SPDLOG_INFO("STARTED");

        portRs232Ch0.open(rs232CfgCh0);

        while (1)
        {

            toggle_led();

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));

            std::string line;
            if (portRs232Ch0.read_line(line))
            {
                SPDLOG_INFO("Read line: {}", line);
            }
        }
    }

}