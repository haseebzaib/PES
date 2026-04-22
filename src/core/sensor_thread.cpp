#include "pes/core/application.hpp"
#include "pes/core/sensor_thread.hpp"
#include "pes/modules/protocols/serial/serial.hpp"
#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "spdlog/spdlog.h"
#include "vector"
#include <thread>
#include "cstdlib"

namespace core
{

    module::protocols::serial::serialConfig rs232CfgCh0{
        .devicePath_ = "/dev/ttyAMA2",
        .baudRate_ = 9600,
        .startupSettleDelayMs_ = 250,
        .txPostWriteDelayMs_ = 250,
        .rxTimeoutMs_ = 1000};
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


    module::drivers::dustrak::drx85xx drx85xx[2];

    std::string json_packet;

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
        portRs232Ch1.open(rs232CfgCh1);
        portRs485Ch2.open(rs485CfgCh2);
        portRs485Ch3.open(rs485CfgCh3);

        drx85xx[0].init(portRs232Ch0);
        module::drivers::dustrak::drx85xx::driverConfig dustTrakCfg;
        dustTrakCfg.polling.readIdentityOnInit = true;
        dustTrakCfg.polling.pollStatus = true;
        dustTrakCfg.polling.autoStartMeasurement = false;
        dustTrakCfg.polling.pollMeasurements = true;
        dustTrakCfg.polling.pollMeasurementStats = false;
        dustTrakCfg.polling.pollFaultMessages = false;
        dustTrakCfg.polling.pollAlarmMessages = false;
        dustTrakCfg.polling.pollLogInfo = false;
        dustTrakCfg.updateRamAfterWrite = true;
        drx85xx[0].configure(dustTrakCfg);

        while (1)
        {

            toggle_led();

            drx85xx[0].loop(json_packet);

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

}
