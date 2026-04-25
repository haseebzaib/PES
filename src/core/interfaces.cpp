#include "pes/core/application.hpp"
#include "pes/core/sensor_thread.hpp"
#include "pes/core/interfaces.hpp"
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

    std::string json_packet;

    static void read_rs232_config(bool startup)
    {
        if(startup != true)
        {
           if(!redis_storage.Get(rs232ConfigRedisKey))
           {
             return;
           }

           redis_storage.Set<std::uint8_t>(rs232ConfigRedisKey,0);
        }
        


    }



    void interfaces_startup()
    {


    }



    void interfaces_loop()
    {

    }



}
