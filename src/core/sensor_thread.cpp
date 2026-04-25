#include "pes/core/application.hpp"
#include "pes/core/sensor_thread.hpp"
#include "pes/modules/protocols/serial/serial.hpp"
#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "pes/utils/file/file_reader.hpp"
#include "pes/utils/file/json_file.hpp"
#include "pes/core/interfaces.hpp"
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

        interfaces_startup();



        while (1)
        {
            toggle_led();
            interfaces_loop();
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

}
