#include "pes/core/application.hpp"
#include "pes/core/sensor_thread.hpp"
#include "pes/core/interfaces_config.hpp"
#include "pes/modules/protocols/serial/serial.hpp"
#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "pes/utils/file/file_reader.hpp"
#include "pes/utils/file/json_file.hpp"
#include "spdlog/spdlog.h"
#include "vector"
#include <thread>
#include "cstdlib"

namespace core
{

    module::protocols::serial::serialConfig rs232CfgCh0{};
    module::protocols::serial::serialPort portRs232Ch0;
    module::protocols::serial::serialConfig rs232CfgCh1{};
    module::protocols::serial::serialPort portRs232Ch1;
    module::protocols::serial::serialConfig rs485CfgCh2{};
    module::protocols::serial::serialPort portRs485Ch2;
    module::protocols::serial::serialConfig rs485CfgCh3{};
    module::protocols::serial::serialPort portRs485Ch3;
    module::drivers::dustrak::drx85xx drx85xx[2];


    typedef void (*apply_configs)(void);


    static apply_configs const applyConfigs[]





     void interface_configs_init()
     {

     }


     void interface_configs_apply()
     {

     }

}
