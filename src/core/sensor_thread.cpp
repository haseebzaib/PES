#include "pes/core/application.hpp"
#include "pes/core/sensor_thread.hpp"
#include "spdlog/spdlog.h"
#include "vector"
#include <thread>
#include "cstdlib"

namespace core
{

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

        while (1)
        {

            toggle_led();
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

}