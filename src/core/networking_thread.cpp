#include "pes/core/application.hpp"
#include "pes/modules/ping/ping.hpp"
#include "pes/core/networking_thread.hpp"
#include "spdlog/spdlog.h"
#include "vector"
#include <thread>

namespace core
{



      std::vector<std::string> ping_hosts{}; //from database python webpage we get ping hosts 

    void network_thread()
    {

        ping_module.SetPingHosts(ping_hosts);

               SPDLOG_INFO("STARTED");

        while (1)
        {
            auto records = ping_module.ConsumeCompletedRecords();

            for (const auto &record : records)
            {
                // save record to sqlite
            }

             std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

}